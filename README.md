# HTML Cloning Pipeline for Transparency Reports

Automated pipeline written in **Python** to clone, restructure, and normalize quarterly HTML transparency report structures.

This tool was designed to automate repetitive tasks involved in generating new quarterly report structures from an existing template, ensuring consistent formatting, correct folder structures, and proper linking of downloadable resources.

---

# Overview

The pipeline performs several automated transformations over a directory containing HTML report pages and associated downloadable files.

It allows a developer or analyst to generate a **new quarterly publication structure** with minimal manual intervention.

Main goals of the project:

* Reduce manual editing of HTML files
* Standardize report structures across quarters
* Automatically fix titles and headings
* Normalize folder naming conventions
* Link downloadable resources automatically
* Detect missing files and produce reports

---

# Features

### HTML Structure Cloning

Copies an existing quarterly HTML structure and prepares it for a new reporting period.

### Folder Name Normalization

Automatically renames state folders and removes technical suffixes such as:

```
Estado_de_Mexico_2t_2026 → Estado de México
```

### HTML Metadata Correction

Updates:

* `<title>` tags
* `<h1>` page headers

Ensuring they reflect the correct reporting period.

### Breadcrumb Generation

Automatically regenerates navigation breadcrumbs across all pages.

### ZIP Download Linking

Links downloadable documents located inside each state's `Archivos` directory.

Example expected structure:

```
Estado/
 └── Archivos/
     ├── 1_Analitico_de_Plazas.zip
     ├── 2_Catalogo_de_Tabuladores.zip
     └── ...
```

### Missing File Detection

If required ZIP files are missing, the pipeline generates a CSV report indicating which pages lack downloadable resources.

---

# Project Structure

Example directory structure expected by the script:

```
source_html/
│
├── Aguascalientes/
│   └── Archivos/
│
├── Baja California/
│   └── Archivos/
│
└── ...
```

The pipeline generates a cloned structure in a destination directory:

```
dest_html/
│
├── Aguascalientes/
├── Baja California/
└── ...
```

---

# Configuration

At the top of the script there is a configuration section where paths and reporting periods must be defined.

Example:

```
SOURCE_ROOT = Path("./source_html")
DEST_ROOT   = Path("./dest_html")
PLANTILLA_TRIMESTRE = Path("./plantilla_trimestre.html")

FROM_QTAG = "1t"
FROM_YEAR = "2024"

TO_QTAG   = "4t"
TO_YEAR   = "2026"
```

These parameters determine which reporting period is cloned and how the new structure will be generated.

---

# Requirements

Python 3.9 or higher.

Standard library modules used:

* pathlib
* shutil
* re
* csv
* unicodedata
* typing

No external dependencies are required.

---

# Usage

Run the pipeline from the command line:

```
python HTML_CLONACION_PIPELINE.py
```

The script will execute all processing stages and output a summary report at the end.

Example output:

```
================== PIPELINE SUMMARY ==================
Pages cloned: 341
Folders renamed: 21
Titles updated: 452
Breadcrumbs generated: 121
Missing ZIP files: 410
======================================================
```

---

# Example Use Case

This pipeline is useful for:

* Government transparency report automation
* Static HTML report generation
* Batch HTML normalization
* Document publishing workflows

---

# Security Notice

This repository contains **only the automation script**.

No government data, documents, or HTML report files are included in this repository.

---

# Author

Jorge Ortiz
Software Development Student | Automation & Data Processing
