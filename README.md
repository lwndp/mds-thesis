# More Women, More Differences: Gender Composition and Speech Topics in Three Decades of Bundestag Debate

## Contents

This repository contains replication code and data for my thesis research project during my Masters of Data Science for Pubic Policy at Hertie School. That includes:

- Notebooks for analysis and data visualization
- Scripts for data processing and modeling
- Reproducibility dependencies (`pyproject.toml`, `uv.lock`, `renv.lock`)
- Environment template (`.env.example`)

## Data and attribution

### Sources

The underlying parliamentary speech corpus was compiled by the [Open Discourse](https://opendiscourse.de/) team:

> Richter, F.; Koch, P.; Franke, O.; Kraus, J.; Kuruc, F.; Thiem, A.; Högerl, J.; Heine, S.; Schöps, K., 2020, "Open Discourse", https://doi.org/10.7910/DVN/FIKIBO, Harvard Dataverse

The data on members of parliament is provided by:

> Deutscher Bundestag. 2025. ‘Deutscher Bundestag - Open Data’. https://www.bundestag.de/services/opendata.

### Download

All data used in the code in this repository is accessible for download on Zenodo via [![Zenodo](https://zenodo.org/badge/DOI/10.5281/zenodo.19681352.svg)](https://doi.org/10.5281/zenodo.19681352).

Most scripts and analysis notebooks reference environment variables set in `.env`. Therefore, it is crucial to set them correctly as illustrated in `.env.example`. For full replication, the downloaded data should be unzipped and placed in the repository's __root directory__ (i.e. on the same level as `scripts`, `notebooks` etc.).

## Setup

### Tools

In order to run scripts in this repository, the following tools need to be installed:

- Python 3.12 with [uv](https://docs.astral.sh/uv/)
- R 4.5+ from [CRAN](https://cran.r-project.org/)
- Docker (for the Open Discourse PostgreSQL database)
- Quarto (for notebook execution)
- [Make](https://cmake.org/)

The repository contains a `Makefile` which standardizes different operations for convenience. R package dependencies are managed via renv. The project's .Rprofile bootstraps renv automatically; no manual installation is required.

### Environment setup

**1. After cloning the repository, install dependencies**

```bash
make environment        # installs Python (uv) and R (renv) dependencies
```

**2. Configure environment variables**

```bash
cp .env.example .env
```

Edit `.env` and set `DB_CON_STRING` to your PostgreSQL connection URL. If the database runs locally via Docker (see below), the default value works without changes.

**3. Download data**

Download the replication data archive from [Zenodo](https://doi.org/10.5281/zenodo.19681353), unzip it, and place its contents in the repository root (alongside `scripts/`, `notebooks/`, etc.).

**4. Start the database and load auxiliary data**

```bash
make load-data          # starts the Open Discourse Docker container and loads MdB data
```

**5. Compile the thesis**

```bash
make plots             # regenerates all figures and variables (json)
```

## Contact me

In case of any questions, requests or feedback, please send me an [email](mailto:l.windpassinger@students.hertie-school.org).