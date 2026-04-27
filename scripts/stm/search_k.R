#!/usr/bin/env Rscript
# scripts/stm/search_k.R
# ──────────────────────────────────────────────────────────────────────────────
# Runs STM searchK() over a range of candidate K values to guide model selection.
# Uses a subsample of the lemmatised corpus for speed
#
# Outputs data/stm/search_k.parquet with columns:
#   K, semcoh, exclus, heldout, residual, bound
#
# Usage (from /workspace):
#   Rscript scripts/stm/search_k.R
#
# Key env vars:
#   STM_K_RANGE   comma-separated K values   [default: 20,30,40,50,60]
#   STM_SAMPLE    subsample size             [default: 20000]
#   STM_SEED      RNG seed                   [default: 42]
#   STM_CORES     parallel workers           [default: 1]
#   DATA_PATH     root for data/stm/         [default: <repo_root>/data]
# ──────────────────────────────────────────────────────────────────────────────

suppressPackageStartupMessages({
  library(arrow)
  library(quanteda)
  library(stm)
  library(dplyr)
})

# ── Config ────────────────────────────────────────────────────────────────────

parse_int_env <- function(name, default, min_value = NULL) {
  raw <- Sys.getenv(name, default)
  value <- suppressWarnings(as.integer(raw))
  if (is.na(value)) {
    stop(sprintf("Invalid integer for %s: '%s'", name, raw))
  }
  if (!is.null(min_value) && value < min_value) {
    stop(sprintf("%s must be >= %d (got %d)", name, min_value, value))
  }
  value
}

K_RANGE_RAW <- gsub("\\s+", "", Sys.getenv("STM_K_RANGE", "20,30,40,50,60,70,73,80"))
K_RANGE <- suppressWarnings(as.integer(strsplit(K_RANGE_RAW, ",")[[1]]))
if (length(K_RANGE) == 0 || any(is.na(K_RANGE)) || any(K_RANGE < 2)) {
  stop(sprintf("Invalid STM_K_RANGE: '%s'", K_RANGE_RAW))
}
K_RANGE <- unique(K_RANGE)

SAMPLE <- parse_int_env("STM_SAMPLE", "20000", min_value = 1)
SEED <- parse_int_env("STM_SEED", "42")
CORES <- if (nzchar(Sys.getenv("STM_CORES"))) {
  parse_int_env("STM_CORES", "", min_value = 1)
} else {
  max(1L, parallel::detectCores())
}

SCRIPT_ARG <- grep("^--file=", commandArgs(trailingOnly = FALSE), value = TRUE)
SCRIPT_PATH <- if (length(SCRIPT_ARG) > 0) {
  normalizePath(sub("^--file=", "", SCRIPT_ARG[[1]]), mustWork = FALSE)
} else {
  ""
}

GIT_ROOT <- tryCatch(
  {
    out <- system2("git", c("rev-parse", "--show-toplevel"), stdout = TRUE, stderr = FALSE)
    if (length(out) > 0) normalizePath(out[[1]], mustWork = FALSE) else ""
  },
  error = function(e) ""
)

CANDIDATE_ROOTS <- unique(Filter(nzchar, c(
  if (nzchar(SCRIPT_PATH)) normalizePath(file.path(dirname(SCRIPT_PATH), "..", ".."), mustWork = FALSE) else "",
  GIT_ROOT,
  normalizePath(getwd(), mustWork = FALSE)
)))

is_repo_root <- function(path) {
  file.exists(file.path(path, "scripts", "stm", "search_k.R"))
}

MATCHED_ROOTS <- Filter(is_repo_root, CANDIDATE_ROOTS)
REPO_ROOT <- if (length(MATCHED_ROOTS) > 0) MATCHED_ROOTS[[1]] else CANDIDATE_ROOTS[[1]]

DATA_PATH_ENV <- Sys.getenv("DATA_PATH", "")
DATA_PATH <- if (nzchar(DATA_PATH_ENV)) {
  normalizePath(DATA_PATH_ENV, mustWork = FALSE)
} else {
  file.path(REPO_ROOT, "data")
}

OUT_DIR <- file.path(DATA_PATH, "stm")
CORPUS_PATH <- file.path(OUT_DIR, "lemmatized.parquet")

if (!file.exists(CORPUS_PATH)) {
  stop(sprintf("Lemmatised corpus not found at %s\nRun: make stm-lemmatize", CORPUS_PATH))
}

BUNDESTAG_NOISE <- c(
  "herr", "frau", "kollege", "kollegin", "kollegen", "kolleginnen",
  "bundesminister", "bundesministerin", "staatssekretär", "staatssekretärin",
  "abgeordnete", "abgeordneten", "bundesregierung", "bundestag", "bundesrat",
  "damen", "herren", "präsident", "vizepräsident", "sitzung",
  "drucksache", "tagesordnung", "antrag", "anfrage"
)

cat("══════════════════════════════════════════════════════════════\n")
cat(sprintf(
  " searchK  range=%s  sample=%d  seed=%d  cores=%d\n",
  paste(K_RANGE, collapse = ","), SAMPLE, SEED, CORES
))
cat("══════════════════════════════════════════════════════════════\n")

# ── Load and subsample ────────────────────────────────────────────────────────

set.seed(SEED)
df <- read_parquet(CORPUS_PATH)
cat(sprintf("[data] %d speeches total\n", nrow(df)))

if (nrow(df) > SAMPLE) {
  df <- df[sample(nrow(df), SAMPLE), ]
  cat(sprintf("[data] subsampled to %d\n", SAMPLE))
}

df$gender <- factor(df$gender, levels = c("male", "female"))
df$faction <- factor(df$faction)

# ── DFM (identical preprocessing to train.R) ──────────────────────────────────

cat("[quanteda] tokenising...\n")
corp <- corpus(df, text_field = "text", docid_field = "id")
toks <- tokens(corp,
  remove_punct   = TRUE,
  remove_numbers = TRUE,
  remove_symbols = TRUE
) |>
  tokens_remove(stopwords("de", source = "stopwords-iso")) |>
  tokens_remove(BUNDESTAG_NOISE)

dfmat <- dfm(toks) |>
  dfm_trim(min_termfreq = 10, min_docfreq = 5)

cat(sprintf("[quanteda] DFM: %d docs × %d features\n", ndoc(dfmat), nfeat(dfmat)))

stm_input <- convert(dfmat, to = "stm")
meta <- df[match(docnames(dfmat), df$id), c("gender", "faction", "year")]
meta$gender <- factor(meta$gender, levels = levels(df$gender))
meta$faction <- factor(meta$faction, levels = levels(df$faction))

# ── searchK ───────────────────────────────────────────────────────────────────

cat(sprintf("[searchK] testing K = %s ...\n", paste(K_RANGE, collapse = ", ")))
t0 <- proc.time()

search_result <- searchK(
  documents  = stm_input$documents,
  vocab      = stm_input$vocab,
  K          = K_RANGE,
  prevalence = ~ gender * s(year, df = 3) + faction,
  data       = meta,
  init.type  = "Spectral",
  seed       = SEED,
  cores      = CORES,
  max.em.its = 75,
  emtol      = 1e-4,
  verbose    = TRUE
)

elapsed <- proc.time() - t0
cat(sprintf("[searchK] done (%.1f min)\n", elapsed["elapsed"] / 60))

# ── Export ────────────────────────────────────────────────────────────────────

res <- as.data.frame(search_result$results)
res$K <- as.integer(res$K)

# unlist scalar columns (searchK wraps values in lists)
for (col in c("semcoh", "exclus", "heldout", "residual", "bound", "lbound")) {
  if (is.list(res[[col]])) res[[col]] <- unlist(res[[col]])
}

out_path <- file.path(OUT_DIR, "search_k.parquet")
write_parquet(res, out_path)
cat(sprintf("[save] %s\n", out_path))

cat("\n── Results ───────────────────────────────────────────\n")
print(res[, c("K", "semcoh", "exclus", "heldout")])
cat("──────────────────────────────────────────────────────\n")
