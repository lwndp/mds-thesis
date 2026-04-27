#!/usr/bin/env Rscript
# scripts/stm/train.R
# ──────────────────────────────────────────────────────────────────────────────
# STM training script for gendered discourse analysis.
#
# Reads pre-lemmatised speeches from data/stm/lemmatized.parquet (produced by
# scripts/stm/lemmatize.py).  Run lemmatize.py first.
#
# Prevalence formula: gender * s(year, df=5) + faction
#   - Models how topic proportions vary by speaker gender, time (spline),
#     and party faction, with a gender × time interaction to detect whether
#     the gender gap in topic choice has changed as women's share grew.
#
# K=0 triggers spectral automatic K selection (Arora et al. 2013).
# Override with STM_K env var after inspecting the spectral result.
#
# Usage:
#   Rscript scripts/stm/train.R
#
# Key env vars:
#   STM_K       integer, 0 = spectral auto   [default: 0]
#   STM_SEED    RNG seed                     [default: 42]
#   STM_CORES   CPU threads for BLAS/OMP     [default: 1]
#   DATA_PATH   root for data/stm/           [default: <repo_root>/data]
# ──────────────────────────────────────────────────────────────────────────────

suppressPackageStartupMessages({
  library(arrow)
  library(quanteda)
  library(quanteda.textstats)
  library(stm)
  library(dplyr)
})

# ── Config ────────────────────────────────────────────────────────────────────

K <- as.integer(Sys.getenv("STM_K", "0"))
SEED <- as.integer(Sys.getenv("STM_SEED", "42"))
CORES <- as.integer(Sys.getenv("STM_CORES", "1"))
SCRIPT_ARG <- grep("^--file=", commandArgs(trailingOnly = FALSE), value = TRUE)
SCRIPT_PATH <- if (length(SCRIPT_ARG) > 0) {
  sub("^--file=", "", SCRIPT_ARG[[1]])
} else {
  file.path(getwd(), "scripts/stm/train.R")
}
REPO_ROOT <- normalizePath(file.path(dirname(SCRIPT_PATH), "..", ".."), mustWork = FALSE)
DATA_PATH <- Sys.getenv("DATA_PATH", file.path(REPO_ROOT, "data"))

Sys.setenv(
  OMP_NUM_THREADS = CORES,
  OPENBLAS_NUM_THREADS = CORES,
  MKL_NUM_THREADS = CORES,
  VECLIB_MAXIMUM_THREADS = CORES,
  NUMEXPR_NUM_THREADS = CORES
)
quanteda_options(threads = CORES)

OUT_DIR <- file.path(DATA_PATH, "stm")
CORPUS_PATH <- file.path(OUT_DIR, "lemmatized.parquet")

dir.create(OUT_DIR, showWarnings = FALSE, recursive = TRUE)

if (!file.exists(CORPUS_PATH)) {
  stop(sprintf(
    "Lemmatised corpus not found at %s\nRun: python scripts/stm/lemmatize.py",
    CORPUS_PATH
  ))
}

# Parliament-specific noise tokens (titles, procedural words)
BUNDESTAG_NOISE <- c(
  "herr", "frau", "kollege", "kollegin", "kollegen", "kolleginnen",
  "bundesminister", "bundesministerin", "staatssekretär", "staatssekretärin",
  "abgeordnete", "abgeordneten", "damen", "herren", "präsident", "vizepräsident",
  "sitzung", "drucksache", "tagesordnung", "sehr", "geehrte",
  "liebe", "meine", "seine", "ihre"
)

cat("══════════════════════════════════════════════════════════════\n")
cat(sprintf(" STM training  K=%d  seed=%d  cores=%d\n", K, SEED, CORES))
cat("══════════════════════════════════════════════════════════════\n")

# ── Load pre-lemmatised corpus ────────────────────────────────────────────────

cat(sprintf("[data] reading %s...\n", CORPUS_PATH))
df <- read_parquet(CORPUS_PATH)
cat(sprintf("[data] %d speeches loaded\n", nrow(df)))

required_cols <- c("id", "text", "gender", "faction", "year", "electoral_term")
cat(sprintf("[data] columns: %s\n", paste(colnames(df), collapse = ", ")))
missing_cols <- setdiff(required_cols, colnames(df))
if (length(missing_cols) > 0) {
  stop(sprintf(
    "Missing required columns in %s: %s",
    CORPUS_PATH,
    paste(missing_cols, collapse = ", ")
  ))
}

df$gender <- factor(df$gender, levels = c("male", "female"))
df$faction <- factor(df$faction)

cat(sprintf("[data] gender:   %s\n", paste(table(df$gender), collapse = " / ")))
cat(sprintf("[data] factions: %s\n", paste(names(table(df$faction)), collapse = ", ")))

# ── quanteda DFM ──────────────────────────────────────────────────────────────

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
  dfm_trim(min_termfreq = 10, min_docfreq = 5) |>
  dfm_trim(max_docfreq = 0.99, docfreq_type = "prop")

cat(sprintf("[quanteda] DFM: %d docs × %d features\n", ndoc(dfmat), nfeat(dfmat)))

# ── Convert to STM format ─────────────────────────────────────────────────────

stm_input <- convert(dfmat, to = "stm")

meta <- df[match(docnames(dfmat), df$id), c("gender", "faction", "year", "electoral_term")]
meta$gender <- factor(meta$gender, levels = levels(df$gender))
meta$faction <- factor(meta$faction, levels = levels(df$faction))
stopifnot(nrow(meta) == ndoc(dfmat))

# ── STM ───────────────────────────────────────────────────────────────────────
#
# Prevalence formula: gender * s(year, df=5) + faction
#   Main effects  : gender + s(year, df=5) + faction
#   Interaction   : gender:s(year, df=5)  — does the gender gap change over time?
#
# K=0: spectral automatic selection.  K>0: fix number of topics.

cat(sprintf("[stm] fitting  K=%s  init=Spectral ...\n", ifelse(K == 0, "auto", K)))
t0 <- proc.time()

stm_model <- stm(
  documents  = stm_input$documents,
  vocab      = stm_input$vocab,
  K          = K,
  prevalence = ~ gender * s(year, df = 5) + faction,
  data       = meta,
  init.type  = "Spectral",
  seed       = SEED,
  verbose    = TRUE
)

elapsed <- proc.time() - t0
K_fit <- stm_model$settings$dim$K
cat(sprintf("[stm] done — K=%d  (%.1f min)\n", K_fit, elapsed["elapsed"] / 60))

# ── Save model ────────────────────────────────────────────────────────────────

model_file <- file.path(OUT_DIR, sprintf("stm_model_K%d.rds", K_fit))
data_file <- file.path(OUT_DIR, "stm_data.rds")

saveRDS(stm_model, model_file)
saveRDS(
  list(documents = stm_input$documents, vocab = stm_input$vocab, meta = meta),
  data_file
)
cat(sprintf("[save] model → %s\n", model_file))

# ── Export for Python visualisation ──────────────────────────────────────────

# 1. FREX + probability top words per topic
cat("[export] topic labels...\n")
lab <- labelTopics(stm_model, n = 15)
frex_df <- data.frame(
  topic = seq_len(K_fit),
  frex  = apply(lab$frex, 1, paste, collapse = ", "),
  prob  = apply(lab$prob, 1, paste, collapse = ", "),
  lift  = apply(lab$lift, 1, paste, collapse = ", ")
)
write_parquet(frex_df, file.path(OUT_DIR, "topic_labels.parquet"))

# 2. Per-document topic proportions (theta) with metadata
cat("[export] theta (topic proportions)...\n")
theta <- as.data.frame(stm_model$theta)
colnames(theta) <- sprintf("topic_%02d", seq_len(K_fit))
theta$doc_id <- docnames(dfmat)
theta$gender <- meta$gender
theta$faction <- meta$faction
theta$year <- meta$year
theta$electoral_term <- meta$electoral_term
write_parquet(theta, file.path(OUT_DIR, "theta.parquet"))

# 3. Effect estimates: gender × time interaction + faction
cat("[export] effect estimates...\n")
# Direct effect: gender controlling for faction (what identity adds beyond party)
effects <- estimateEffect(
  formula  = ~ gender * s(year, df = 5) + faction,
  stmobj   = stm_model,
  metadata = meta
)
saveRDS(effects, file.path(OUT_DIR, "stm_effects.rds"))

coef_rows <- lapply(seq_len(K_fit), function(k) {
  s <- summary(effects, topics = k)$tables[[1]]
  data.frame(
    topic = k,
    term = rownames(s),
    estimate = s[, "Estimate"],
    se = s[, "Std. Error"],
    tval = s[, "t value"],
    pval = s[, "Pr(>|t|)"],
    stringsAsFactors = FALSE
  )
})
write_parquet(do.call(rbind, coef_rows), file.path(OUT_DIR, "coef_table.parquet"))

# Total effect: gender without faction control (what descriptive representation delivers)
effects_total <- estimateEffect(
  formula  = ~ gender * s(year, df = 5),
  stmobj   = stm_model,
  metadata = meta
)
saveRDS(effects_total, file.path(OUT_DIR, "stm_effects_total.rds"))

coef_rows_total <- lapply(seq_len(K_fit), function(k) {
  s <- summary(effects_total, topics = k)$tables[[1]]
  data.frame(
    topic = k,
    term = rownames(s),
    estimate = s[, "Estimate"],
    se = s[, "Std. Error"],
    tval = s[, "t value"],
    pval = s[, "Pr(>|t|)"],
    stringsAsFactors = FALSE
  )
})
write_parquet(do.call(rbind, coef_rows_total), file.path(OUT_DIR, "coef_table_total.parquet"))

# 4. Mean topic proportion by gender
theta_long <- theta |>
  select(gender, starts_with("topic_")) |>
  group_by(gender) |>
  summarise(across(everything(), mean), .groups = "drop")
write_parquet(theta_long, file.path(OUT_DIR, "mean_theta_by_gender.parquet"))

# 5. Mean topic proportion by faction
theta_faction <- theta |>
  select(faction, starts_with("topic_")) |>
  group_by(faction) |>
  summarise(across(everything(), mean), .groups = "drop")
write_parquet(theta_faction, file.path(OUT_DIR, "mean_theta_by_faction.parquet"))

cat("══════════════════════════════════════════════════════════════\n")
cat(sprintf(" DONE  K=%d  outputs in %s/\n", K_fit, OUT_DIR))
cat("══════════════════════════════════════════════════════════════\n")
