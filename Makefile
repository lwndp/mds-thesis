PYTHON      ?= uv run python
JUPYTER     ?= uv run python -m jupyter
PYTHON_EXECUTABLE := .venv/bin/python
SERVER      ?= ds01
SERVER_DIR  ?= ~/workspace/thesis
UV_GROUPS   ?= --group training --group analysis --group dev

# STM options (override on command line: make stm-train STM_K=40)
STM_K      ?= 0
STM_SAMPLE ?= 0
STM_SEED   ?= 42
STM_CORES  ?= 32 # of CPU cores to use for STM training (set to 1 to disable parallelization)

.PHONY: all \
	thesis pdfs plots plots-eda plots-stm clean-plots \
	db-start db-dump load-mdbs \
	stm-lemmatize stm-lemmatize-status stm-lemmatize-stop \
	stm-train stm-train-status stm-train-stop stm-save stm-watch \
	hooks-install install requirements environment help

all: help

## ── Thesis build ─────────────────────────────────────────────────────────────

## clean-plots          : delete all SVGs in data/figures_out/
clean-plots:
	rm -f data/figures_out/*.svg

## plots-eda            : re-execute EDA notebook
plots-eda:
	QUARTO_PYTHON=$(PYTHON_EXECUTABLE) quarto render notebooks/eda.ipynb --execute --output-dir html --no-clean

## plots-stm            : clear STM cache and re-execute STM analysis notebook
plots-stm:
	rm -rf notebooks/stm_analysis_cache
	quarto render notebooks/stm_analysis.qmd --cache-refresh --output-dir html --no-clean
	rm -f notebooks/stm_analysis.knit.md

## plots                : clean figures, re-execute all notebooks and sync HTML exports
plots: clean-plots plots-eda plots-stm

## pdfs                 : compile thesis PDFs (online and print versions)
pdfs:
	cd writing && typst compile --root ../ --input print_version=false windpassinger_luis_master_thesis.typ windpassinger_luis_master_thesis.pdf && typst compile --root ../ --input print_version=true windpassinger_luis_master_thesis.typ windpassinger_luis_master_thesis_print.pdf

## poster               : compile A1 conference poster pdf
poster:
	cd poster && typst compile --root ../ poster.typ poster.pdf

## thesis               : regenerate all plots (deletes old svg files) and compile thesis PDFs (online and print versions)
thesis: plots pdfs poster

## ── Local setup ──────────────────────────────────────────────────────────────

## load-data		   : initialize local PostgreSQL DB with Open Discourse data and download MdB Stammdaten
load-data: db-start load-mdbs

## db-start         : pull and start the Open Discourse PostgreSQL container
db-start:
	bash scripts/db/start_opendiscourse_db.sh

## db-dump          : dump open_discourse schemas --> data/dump/
db-dump:
	bash scripts/db/db_dump.sh

## load-mdbs            : fetch MdB Stammdaten (XML) --> data/mds/mdb_stammdaten.parquet
load-mdbs:
	$(PYTHON) scripts/mdbs/fetch_mdbs.py

## ── STM pipeline (run from inside DS01 container at /workspace) ──────────────

## stm-lemmatize        : lemmatize speeches --> data/stm/lemmatized.parquet
stm-lemmatize:
	mkdir -p tmp data/stm
	STM_SAMPLE=$(STM_SAMPLE) STM_SEED=$(STM_SEED) \
	  nohup /opt/venv/bin/python scripts/stm/lemmatize.py > tmp/lemmatize.log 2>&1 &
	@echo "Lemmatization started (sample=$(STM_SAMPLE)). Monitor: tail -f tmp/lemmatize.log"
	
## stm-lemmatize-stop   : kill background lemmatization job
stm-lemmatize-stop:
	pkill -f "lemmatize.py" || true

## stm-train            : train STM on lemmatized.parquet (log: tmp/stm.log)
stm-train:
	mkdir -p tmp data/stm
	STM_K=$(STM_K) STM_SAMPLE=$(STM_SAMPLE) STM_SEED=$(STM_SEED) STM_CORES=$(STM_CORES) \
	  nohup Rscript scripts/stm/train.R > tmp/stm.log 2>&1 &
	@echo "STM training started (K=$(STM_K), cores=$(STM_CORES)). Monitor: tail -f tmp/stm.log"

## stm-train-stop       : kill background STM training job
stm-train-stop:
	pkill -f "train.R" || true

## ── Utilities ────────────────────────────────────────────────────────────────

## environment             : install all local dependencies (Python + R) — local only, not needed on docker image based training containers
environment:
	uv sync $(UV_GROUPS)
	Rscript -e "renv::restore(prompt=FALSE)"

## hooks-install       : enable repository hooks and make them executable
hooks-install:
	git config core.hooksPath .githooks
	chmod +x .githooks/pre-commit
	@echo "Git hooks enabled via core.hooksPath=.githooks"

## requirements        : export pinned Python requirements to requirements.txt
requirements:
	uv export --format requirements.txt --no-hashes --all-groups --output-file requirements.txt

## help                : show this help
help:
	@grep -E '^##' Makefile | sed 's/^## //'
