# GeoSupplyExposure data pipeline
# Research design: geopolitical distance × intermediate-input dependence
# Strict provenance rule: no invented / interpolated / silently spliced data.
# Missing source data remain missing and are logged.
#
# Main raw sources
# 1) CEPII BACI HS92 V202601, 1995–2024
#    https://www.cepii.fr/DATA_DOWNLOAD/baci/data/BACI_HS92_V202601.zip
# 2) UN Statistics Division HS–SITC–BEC correspondence workbook (2022)
#    https://unstats.un.org/unsd/classifications/Econ/tables/HS-SITC-BEC%20Correlations_2022.xlsx
# 3) Erik Voeten UNGA voting / ideal-point data (Harvard Dataverse)
#    https://dataverse.harvard.edu/dataset.xhtml?persistentId=hdl:1902.1/12379
#    Target file: IdealPointsJuly2025.tab
#
# Methodological citation for ideal points:
# Bailey, Strezhnev & Voeten (2017), Journal of Conflict Resolution 61(2): 430–456.
#
# IMPORTANT:
# - BACI uses reconciled bilateral trade flows. Do not mix BACI versions.
# - Product codes are read as character, preserving leading zeros.
# - "Intermediate goods" use the UN Comtrade BEC Rev.4 SNA grouping:
#   111, 121, 21, 22, 31, 322, 42, 53.
# - The UNSD workbook is a CORRESPONDENCE table and may contain one-to-many mappings.
#   This script does NOT pick one arbitrarily. HS92 codes with conflicting
#   intermediate/non-intermediate mappings are flagged as ambiguous and excluded.
#   If ambiguous trade exceeds MAX_AMBIGUOUS_TRADE_SHARE, the script stops.
# - UNGA missing ideal points are NEVER filled. Coverage is measured by trade value;
#   GeoSupplyExposure is set to NA if coverage < MIN_IP_COVERAGE.
# - EA is not treated as a fake single country. EA10 is fixed as:
#   AUT, BEL, DEU, ESP, FIN, FRA, GRC, ITA, NLD, PRT.
#   EA exposure is computed from member-level extra-EA10 imports and member-specific
#   geopolitical distances. Intra-EA10 flows are excluded.
#
# Output period: 2000–2024 annual.

options(stringsAsFactors = FALSE)
options(timeout = max(3600, getOption("timeout")))

required <- c("data.table", "readxl", "jsonlite", "countrycode", "digest")
missing_pkgs <- required[!vapply(required, requireNamespace, logical(1), quietly = TRUE)]
if (length(missing_pkgs)) {
  stop(
    "Missing R packages: ", paste(missing_pkgs, collapse = ", "),
    "\nInstall with: install.packages(c(",
    paste(sprintf('"%s"', missing_pkgs), collapse = ", "), "))"
  )
}

library(data.table)

# ----------------------------
# 0. Configuration
# ----------------------------
START_YEAR <- 2000L
END_YEAR   <- 2024L
BACI_VERSION <- "202601"
MIN_IP_COVERAGE <- 0.95
MAX_AMBIGUOUS_TRADE_SHARE <- 0.01

ROOT <- normalizePath(".", winslash = "/", mustWork = TRUE)
RAW  <- file.path(ROOT, "data", "raw")
INT  <- file.path(ROOT, "data", "interim")
OUT  <- file.path(ROOT, "data", "processed")
DIRS <- c(RAW, INT, OUT)
invisible(lapply(DIRS, dir.create, recursive = TRUE, showWarnings = FALSE))

BACI_URL <- sprintf(
  "https://www.cepii.fr/DATA_DOWNLOAD/baci/data/BACI_HS92_V%s.zip",
  BACI_VERSION
)
BACI_ZIP <- file.path(RAW, sprintf("BACI_HS92_V%s.zip", BACI_VERSION))

UNSD_BEC_URL <- paste0(
  "https://unstats.un.org/unsd/classifications/Econ/tables/",
  "HS-SITC-BEC%20Correlations_2022.xlsx"
)
UNSD_BEC_XLSX <- file.path(RAW, "HS-SITC-BEC_Correlations_2022.xlsx")

UNGA_DATASET_URL <- paste0(
  "https://dataverse.harvard.edu/dataset.xhtml?",
  "persistentId=hdl:1902.1/12379"
)
UNGA_META_URL <- paste0(
  "https://dataverse.harvard.edu/api/datasets/:persistentId/?persistentId=",
  utils::URLencode("hdl:1902.1/12379", reserved = TRUE)
)
UNGA_TAB <- file.path(RAW, "IdealPointsJuly2025.tab")

# Project macro nodes (14)
nodes <- data.table(
  node = c("AU","BR","CA","CH","CN","EA","UK","JP","KR","NO","SG","TR","US","ZA"),
  economy = c(
    "Australia","Brazil","Canada","Switzerland","China","Euro Area",
    "United Kingdom","Japan","Korea","Norway","Singapore","Türkiye",
    "United States","South Africa"
  ),
  iso3c = c(
    "AUS","BRA","CAN","CHE","CHN",NA,"GBR","JPN","KOR","NOR",
    "SGP","TUR","USA","ZAF"
  )
)

# Fixed EA10 definition inherited from the user's 14-economy project convention.
ea10 <- data.table(
  node = "EA",
  iso3c = c("AUT","BEL","DEU","ESP","FIN","FRA","GRC","ITA","NLD","PRT"),
  economy = c(
    "Austria","Belgium","Germany","Spain","Finland",
    "France","Greece","Italy","Netherlands","Portugal"
  )
)

# Convert ISO alpha-3 -> UN M49 numeric, which matches BACI's inherited Comtrade codes.
all_importer_iso3 <- c(nodes[!is.na(iso3c), iso3c], ea10$iso3c)
iso_to_m49 <- data.table(
  iso3c = all_importer_iso3,
  m49 = as.integer(countrycode::countrycode(
    all_importer_iso3, origin = "iso3c", destination = "un"
  ))
)
if (anyNA(iso_to_m49$m49)) {
  stop("Failed to map one or more target ISO3 codes to UN M49: ",
       paste(iso_to_m49[is.na(m49), iso3c], collapse = ", "))
}

# ----------------------------
# Helpers
# ----------------------------
`%||%` <- function(x, y) if (is.null(x)) y else x

download_verified <- function(url, dest) {
  if (!file.exists(dest)) {
    message("Downloading: ", url)
    status <- try(
      utils::download.file(url, destfile = dest, mode = "wb", method = "libcurl",
                           quiet = FALSE),
      silent = TRUE
    )
    if (inherits(status, "try-error") || !file.exists(dest) || file.info(dest)$size <= 0) {
      if (file.exists(dest)) unlink(dest)
      stop("Download failed: ", url)
    }
  } else {
    message("Using existing raw file: ", dest)
  }
  invisible(dest)
}

sha256 <- function(path) {
  if (!file.exists(path)) return(NA_character_)
  digest::digest(file = path, algo = "sha256", serialize = FALSE)
}

clean_code <- function(x, width = NULL) {
  x <- trimws(as.character(x))
  x[x %in% c("", "NA", "NULL", "null")] <- NA_character_
  x <- sub("\\.0+$", "", x)
  if (!is.null(width)) {
    ok <- !is.na(x) & grepl("^[0-9]+$", x)
    x[ok] <- sprintf(paste0("%0", width, "d"), as.integer(x[ok]))
  }
  x
}

# ----------------------------
# 1. Download immutable raw sources
# ----------------------------
download_verified(BACI_URL, BACI_ZIP)
download_verified(UNSD_BEC_URL, UNSD_BEC_XLSX)

# Harvard Dataverse metadata -> locate exact latest IdealPointsJuly2025.tab by label.
if (!file.exists(UNGA_TAB)) {
  message("Reading Harvard Dataverse metadata...")
  meta <- jsonlite::fromJSON(UNGA_META_URL, simplifyVector = FALSE)
  if (!identical(meta$status, "OK")) stop("Harvard Dataverse metadata request failed.")
  files <- meta$data$latestVersion$files
  labels <- vapply(files, function(z) z$label %||% "", character(1))
  hit <- which(tolower(labels) == tolower("IdealPointsJuly2025.tab"))
  if (length(hit) != 1L) {
    # conservative fallback: identify by name pattern, but never silently choose >1
    hit <- grep("^IdealPoints.*2025\\.tab$", labels, ignore.case = TRUE)
  }
  if (length(hit) != 1L) {
    stop(
      "Could not uniquely locate IdealPointsJuly2025.tab in latest Harvard Dataverse version.\n",
      "Found labels: ", paste(labels, collapse = " | ")
    )
  }
  file_id <- files[[hit]]$dataFile$id
  unga_download_url <- paste0(
    "https://dataverse.harvard.edu/api/access/datafile/", file_id
  )
  download_verified(unga_download_url, UNGA_TAB)
} else {
  message("Using existing raw file: ", UNGA_TAB)
}

# ----------------------------
# 2. Provenance manifest
# ----------------------------
provenance <- data.table(
  source_id = c("BACI", "UNSD_BEC", "UNGA_IDEALPOINTS"),
  institution = c("CEPII", "United Nations Statistics Division", "Harvard Dataverse / Erik Voeten"),
  dataset = c(
    sprintf("BACI HS92 V%s", BACI_VERSION),
    "HS-SITC-BEC Correlations 2022",
    "United Nations General Assembly Voting Data / IdealPointsJuly2025.tab"
  ),
  source_url = c(BACI_URL, UNSD_BEC_URL, UNGA_DATASET_URL),
  raw_file = c(BACI_ZIP, UNSD_BEC_XLSX, UNGA_TAB),
  sha256 = c(sha256(BACI_ZIP), sha256(UNSD_BEC_XLSX), sha256(UNGA_TAB)),
  coverage = c("1995-2024; use 2000-2024", "HS/BEC correspondence", "idealpointfp 1946-2024"),
  transformation = c(
    "Filter 2000-2024; target importers; keep HS92; aggregate BEC4 intermediate goods",
    "Strict HS92-to-BEC4 intermediate classification; conflicting mappings flagged",
    "Use idealpointfp; annual bilateral distance = absolute difference; no fill"
  )
)
fwrite(provenance, file.path(OUT, "source_manifest.csv"))

# ----------------------------
# 3. Build strict HS92 -> intermediate-goods flag
# ----------------------------
sheets <- readxl::excel_sheets(UNSD_BEC_XLSX)
target_sheet <- sheets[tolower(sheets) == tolower("HS SITC BEC")]
if (!length(target_sheet)) {
  target_sheet <- sheets[grepl("HS.*SITC.*BEC", sheets, ignore.case = TRUE)]
}
if (length(target_sheet) != 1L) {
  stop("Could not uniquely identify UNSD HS/SITC/BEC sheet. Sheets: ",
       paste(sheets, collapse = ", "))
}

bec_raw <- as.data.table(readxl::read_excel(
  UNSD_BEC_XLSX, sheet = target_sheet, col_types = "text"
))
setnames(bec_raw, trimws(names(bec_raw)))

need_cols <- c("HS92", "BEC4")
if (!all(need_cols %in% names(bec_raw))) {
  stop("Expected columns HS92 and BEC4 not found. Columns are: ",
       paste(names(bec_raw), collapse = ", "))
}

# UN Comtrade BEC Rev.4 -> SNA basic class: intermediate goods.
INTERMEDIATE_BEC4 <- c("111","121","21","22","31","322","42","53")

bec_map <- unique(bec_raw[, .(
  hs6 = clean_code(HS92, 6),
  bec4 = clean_code(BEC4)
)])
bec_map <- bec_map[!is.na(hs6) & !is.na(bec4)]
bec_map[, class_intermediate := bec4 %in% INTERMEDIATE_BEC4]

# Do not choose arbitrarily when correspondence is one-to-many.
hs_class <- bec_map[, .(
  n_bec4 = uniqueN(bec4),
  has_intermediate = any(class_intermediate),
  has_non_intermediate = any(!class_intermediate),
  bec4_codes = paste(sort(unique(bec4)), collapse = "|")
), by = hs6]
hs_class[, ambiguous := has_intermediate & has_non_intermediate]
hs_class[, is_intermediate := fifelse(
  ambiguous, NA,
  has_intermediate
)]

fwrite(hs_class, file.path(INT, "HS92_BEC4_strict_classification.csv"))
fwrite(hs_class[ambiguous == TRUE],
       file.path(INT, "HS92_BEC4_ambiguous_codes.csv"))

intermediate_hs6 <- hs_class[is_intermediate == TRUE, hs6]
message("Strict intermediate HS6 codes: ", length(intermediate_hs6))
message("Ambiguous HS6 codes flagged: ", hs_class[ambiguous == TRUE, .N])

# ----------------------------
# 4. Load UNGA ideal points (no interpolation / no filling)
# ----------------------------
ip <- fread(UNGA_TAB, sep = "\t", na.strings = c("", "NA", "NaN", "."))
required_ip <- c("iso3c", "year", "idealpointfp")
if (!all(required_ip %in% names(ip))) {
  stop("UNGA file missing required columns: ",
       paste(setdiff(required_ip, names(ip)), collapse = ", "))
}
ip <- ip[, .(
  iso3c = trimws(as.character(iso3c)),
  year = as.integer(year),
  idealpointfp = as.numeric(idealpointfp)
)]
ip <- ip[year >= START_YEAR & year <= END_YEAR]
ip <- unique(ip[!is.na(iso3c) & !is.na(idealpointfp)],
             by = c("iso3c", "year"))
fwrite(ip, file.path(INT, "UNGA_IdealPoints_2000_2024.csv"))

# ----------------------------
# 5. Process BACI year by year
# ----------------------------
zip_list <- utils::unzip(BACI_ZIP, list = TRUE)
annual_re <- sprintf(
  "BACI_HS92_Y(20[0-2][0-9])_V%s\\.csv$",
  BACI_VERSION
)
annual_names <- zip_list$Name[grepl(annual_re, zip_list$Name)]
years_found <- as.integer(sub(annual_re, "\\1", basename(annual_names)))
keep_years <- START_YEAR:END_YEAR

missing_years <- setdiff(keep_years, years_found)
if (length(missing_years)) {
  stop("BACI ZIP is missing requested annual files: ",
       paste(missing_years, collapse = ", "))
}

# Metadata country file if present, retained for audit.
country_meta_name <- zip_list$Name[
  grepl("country.*codes.*\\.csv$", zip_list$Name, ignore.case = TRUE)
][1]
if (!is.na(country_meta_name)) {
  utils::unzip(BACI_ZIP, files = country_meta_name, exdir = INT, overwrite = TRUE)
}

target_m49 <- iso_to_m49$m49
ea_m49 <- iso_to_m49[iso3c %in% ea10$iso3c, m49]

all_years <- vector("list", length(keep_years))
qc_years  <- vector("list", length(keep_years))

for (idx in seq_along(keep_years)) {
  yr <- keep_years[idx]
  nm <- annual_names[years_found == yr]
  if (length(nm) != 1L) stop("Annual BACI file not unique for year ", yr)

  message("Processing BACI ", yr, "...")
  utils::unzip(BACI_ZIP, files = nm, exdir = INT, overwrite = TRUE)
  csv_path <- file.path(INT, nm)
  if (!file.exists(csv_path)) {
    # ZIP may contain nested paths; normalize expected extracted path.
    csv_path <- file.path(INT, basename(nm))
  }

  # k must stay character to preserve leading zeroes.
  d <- fread(
    csv_path,
    colClasses = list(character = "k"),
    select = c("t","k","i","j","v"),
    showProgress = TRUE
  )
  d[, `:=`(
    t = as.integer(t),
    i = as.integer(i),
    j = as.integer(j),
    v = as.numeric(v),
    k = sprintf("%06d", as.integer(k))
  )]

  # Only the 13 country reporters + 10 EA10 members are needed as importers.
  d <- d[j %in% target_m49]

  # Attach strict classification. Ambiguous codes remain NA.
  d <- merge(
    d,
    hs_class[, .(hs6, is_intermediate, ambiguous)],
    by.x = "k", by.y = "hs6", all.x = TRUE
  )

  total_target_trade <- d[, sum(v, na.rm = TRUE)]
  ambiguous_trade <- d[ambiguous == TRUE, sum(v, na.rm = TRUE)]
  unmapped_trade <- d[is.na(is_intermediate) & is.na(ambiguous), sum(v, na.rm = TRUE)]
  amb_share <- if (total_target_trade > 0) ambiguous_trade / total_target_trade else NA_real_
  unmap_share <- if (total_target_trade > 0) unmapped_trade / total_target_trade else NA_real_

  qc_years[[idx]] <- data.table(
    year = yr,
    total_target_import_value_kusd = total_target_trade,
    ambiguous_trade_value_kusd = ambiguous_trade,
    ambiguous_trade_share = amb_share,
    unmapped_trade_value_kusd = unmapped_trade,
    unmapped_trade_share = unmap_share
  )

  # Strict rule: no arbitrary BEC assignment.
  if (!is.na(amb_share) && amb_share > MAX_AMBIGUOUS_TRADE_SHARE) {
    stop(
      sprintf(
        "Year %d: ambiguous HS92→BEC4 trade share %.3f exceeds threshold %.3f. ",
        yr, amb_share, MAX_AMBIGUOUS_TRADE_SHARE
      ),
      "Do not proceed until a unique official conversion table is supplied."
    )
  }

  # Keep only unambiguously classified intermediate imports.
  x <- d[is_intermediate == TRUE, .(
    intermediate_import_kusd = sum(v, na.rm = TRUE)
  ), by = .(year = t, importer_m49 = j, supplier_m49 = i)]

  all_years[[idx]] <- x

  rm(d, x)
  gc()
  unlink(csv_path)
}

trade <- rbindlist(all_years, use.names = TRUE)
qc_trade <- rbindlist(qc_years, use.names = TRUE)
fwrite(qc_trade, file.path(OUT, "quality_BEC_mapping_by_year.csv"))

# M49 -> ISO3; unmappable suppliers stay NA and are not invented.
trade[, importer_iso3 := countrycode::countrycode(
  importer_m49, origin = "un", destination = "iso3c"
)]
trade[, supplier_iso3 := countrycode::countrycode(
  supplier_m49, origin = "un", destination = "iso3c"
)]

# Map importer to project node.
node_map <- rbind(
  nodes[!is.na(iso3c), .(node, importer_iso3 = iso3c)],
  ea10[, .(node = "EA", importer_iso3 = iso3c)]
)
trade <- merge(trade, node_map, by = "importer_iso3", all.x = TRUE)
if (anyNA(trade$node)) stop("Unexpected target importer failed node mapping.")

# For EA, exclude intra-EA10 trade because it is internal to the aggregate macro node.
trade[, is_intra_ea10 := node == "EA" & supplier_iso3 %in% ea10$iso3c]
trade_external <- trade[is_intra_ea10 == FALSE]

fwrite(trade_external, file.path(
  INT, "BACI_14nodes_intermediate_imports_global_suppliers_2000_2024.csv"
))

# ----------------------------
# 6. Compute geopolitical distance components
# ----------------------------
# Supplier ideal point
trade_external <- merge(
  trade_external,
  ip[, .(supplier_iso3 = iso3c, year, supplier_ip = idealpointfp)],
  by = c("supplier_iso3", "year"), all.x = TRUE
)

# Importer/member ideal point.
# For EA each EA10 member keeps its own ideal point; no artificial "EA ideal point".
trade_external <- merge(
  trade_external,
  ip[, .(importer_iso3 = iso3c, year, importer_ip = idealpointfp)],
  by = c("importer_iso3", "year"), all.x = TRUE
)

trade_external[, geo_distance := abs(importer_ip - supplier_ip)]
trade_external[, distance_available := !is.na(geo_distance)]
trade_external[, weighted_distance_value :=
                 intermediate_import_kusd * geo_distance]

# ----------------------------
# 7. Aggregate to node × year exposure with strict coverage QC
# ----------------------------
exposure <- trade_external[, {
  total_trade <- sum(intermediate_import_kusd, na.rm = TRUE)
  covered_trade <- sum(
    intermediate_import_kusd[distance_available],
    na.rm = TRUE
  )
  cov <- if (total_trade > 0) covered_trade / total_trade else NA_real_
  exp_cov <- if (covered_trade > 0) {
    sum(weighted_distance_value[distance_available], na.rm = TRUE) / covered_trade
  } else {
    NA_real_
  }

  list(
    intermediate_import_total_kusd = total_trade,
    intermediate_import_with_ip_kusd = covered_trade,
    ip_trade_coverage = cov,
    exposure_on_covered_trade = exp_cov,
    GeoSupplyExposure = if (!is.na(cov) && cov >= MIN_IP_COVERAGE) exp_cov else NA_real_,
    n_suppliers_total = uniqueN(supplier_m49),
    n_suppliers_with_distance = uniqueN(supplier_m49[distance_available])
  )
}, by = .(node, year)]

setorder(exposure, node, year)
fwrite(exposure, file.path(OUT, "GeoSupplyExposure_14nodes_2000_2024.csv"))

# Keep auditable component table.
fwrite(
  trade_external[, .(
    node, year, importer_iso3, supplier_iso3,
    intermediate_import_kusd,
    importer_ip, supplier_ip, geo_distance,
    distance_available, weighted_distance_value
  )],
  file.path(OUT, "GeoSupplyExposure_components.csv")
)

# ----------------------------
# 8. Coverage and missingness reports
# ----------------------------
missing_ip <- trade_external[distance_available == FALSE, .(
  missing_intermediate_import_kusd = sum(intermediate_import_kusd, na.rm = TRUE)
), by = .(node, year, importer_iso3, supplier_iso3)]
setorder(missing_ip, node, year, -missing_intermediate_import_kusd)
fwrite(missing_ip, file.path(OUT, "missing_idealpoint_trade_components.csv"))

coverage_summary <- exposure[, .(
  min_ip_trade_coverage = min(ip_trade_coverage, na.rm = TRUE),
  mean_ip_trade_coverage = mean(ip_trade_coverage, na.rm = TRUE),
  n_years_below_threshold = sum(ip_trade_coverage < MIN_IP_COVERAGE, na.rm = TRUE),
  n_missing_exposure = sum(is.na(GeoSupplyExposure))
), by = node]
fwrite(coverage_summary, file.path(OUT, "quality_exposure_coverage_by_node.csv"))

# ----------------------------
# 9. Method / source notes
# ----------------------------
method_notes <- c(
  sprintf("Period: %d-%d annual.", START_YEAR, END_YEAR),
  sprintf("BACI: HS92 V%s only; versions are never mixed.", BACI_VERSION),
  paste0(
    "Intermediate goods: UN Comtrade BEC Rev.4 SNA grouping = ",
    paste(INTERMEDIATE_BEC4, collapse = ", "), "."
  ),
  "UNSD correspondence ambiguity: no arbitrary choice; conflicting HS92 mappings are flagged/excluded.",
  sprintf(
    "Hard stop if ambiguous trade share exceeds %.1f%% in any year.",
    100 * MAX_AMBIGUOUS_TRADE_SHARE
  ),
  "Geopolitical distance: abs(idealpointfp_importer - idealpointfp_supplier).",
  "UNGA ideal points: no interpolation, no forward/backward fill.",
  sprintf(
    "GeoSupplyExposure reported only if trade-weighted ideal-point coverage >= %.1f%%.",
    100 * MIN_IP_COVERAGE
  ),
  paste0(
    "EA10 fixed members: ", paste(ea10$iso3c, collapse = ", "),
    ". Intra-EA10 imports excluded."
  ),
  paste0(
    "EA exposure uses member-level imports and member-specific ideal points; ",
    "no synthetic EA ideal point is constructed."
  )
)
writeLines(method_notes, file.path(OUT, "METHOD_NOTES.txt"))

message("\nDONE.")
message("Core output: ", file.path(OUT, "GeoSupplyExposure_14nodes_2000_2024.csv"))
message("Source manifest: ", file.path(OUT, "source_manifest.csv"))
message("QC reports are in: ", OUT)
