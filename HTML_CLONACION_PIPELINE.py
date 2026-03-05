#################################
###Autor: Jorge F. Ortiz Bravo
###Fecha de programación:03/03/2026
###ART73_CLONACION_PIPELINE.py V 1.0.0 03/03/2026 10:50 a.m. Se crea script a partir de ingenieria de prompt en base a scrips obsoletos desarrollados en la SEP por JFOB (SleekVoidAI)
###ART73_CLONACION_PIPELINE.py V 1.5.0 03/03/2026 11:22 a.m. Se modifica la funcion step_rename_state_folders a step_rename_state_folders_official para respetar acentos y espacios al nombrar folders de los estados, tambien se modifica la funcion step_breadcrumbs para poner el nombre con acento en el breadcrumb de las paginas
###ART73_CLONACION_PIPELINE.py V 1.5.1 05/03/2026 05/03/2026 Se comenta el modulo de configuracion del script para facilitarle la comprension de este modulo a el usuario
#################################


# -*- coding: utf-8 -*-
from __future__ import annotations

import re
import csv
import shutil
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Tuple

import time

def safe_rename_dir(src: Path, dst: Path, stats: Stats, label: str, retries: int = 5, wait_s: float = 0.4) -> bool:
    """
    Renombra carpeta con reintentos. Si falla, registra warning y NO revienta el pipeline.
    """
    if dst.exists():
        stats.warnings.append(f"[{label}] NO renombré (ya existe destino): {src.name} -> {dst.name}")
        return False

    last_err = None
    for _ in range(retries):
        try:
            src.rename(dst)
            return True
        except PermissionError as e:
            last_err = e
            time.sleep(wait_s)
        except OSError as e:
            last_err = e
            time.sleep(wait_s)

    stats.warnings.append(f"[{label}] ERROR renombrando {src.name} -> {dst.name}: {last_err}")
    return False

# ============================================================
# 0) CONFIGURACIÓN (AJUSTA ESTO)
# ============================================================
# Esta sección es la ÚNICA que normalmente debes cambiar.
# Aquí le dices al script:
# 1) De dónde va a tomar los archivos (carpeta fuente)
# 2) A dónde va a crear/copiar el nuevo trimestre (carpeta destino)
# 3) Qué plantilla usar para construir la página principal del trimestre (archivo HTML base)
# 4) Qué trimestre/año está clonando (origen) y a cuál lo quiere convertir (destino)
# 5) Dónde están los archivos ZIP (si vas a copiarlos)

from pathlib import Path
from typing import Optional


# ------------------------------------------------------------
# 1) CARPETA FUENTE (ORIGEN)
# ------------------------------------------------------------
# Aquí está tu trimestre "viejo" o "base" que ya está correcto.
# Dentro de esta carpeta deben existir:
# - Las carpetas de los estados (31 o 32)
# - Los HTML de cada estado y sus subpáginas
# - La carpeta "Archivos" (si aplica)
#
# Ejemplo típico:
# 05 HTML Primer_Trimestre_2024/
#   Aguascalientes_1t_2024/
#   Baja_California_1t_2024/
#   ...
SOURCE_ROOT = Path("./source_html")


# ------------------------------------------------------------
# 2) CARPETA DESTINO (A DÓNDE SE VA A CREAR EL CLON)
# ------------------------------------------------------------
# Aquí se creará el nuevo trimestre "clonado".
# Si esta carpeta ya existe, el script podría mezclar cosas viejas y nuevas.
# Lo ideal es que sea una carpeta nueva/vacía para evitar duplicados.
#
# Ejemplo típico:
# CLON Cuarto_Trimestre_2026/
#   Aguascalientes_4t_2026/
#   Baja_California_4t_2026/
#   ...
DEST_ROOT = Path("./dest_html")


# ------------------------------------------------------------
# 3) PLANTILLA DEL TRIMESTRE (HTML BASE “CORRECTO”)
# ------------------------------------------------------------
# Este archivo es el HTML que sirve como “formato” para generar la página principal del trimestre.
# Por ejemplo: la página que lista TODOS los estados en una lista con enlaces.
#
# Normalmente usas una plantilla que ya se ve bien (por ejemplo Primer_Trimestre_2024.html)
# para que el script reemplace el trimestre/año y regenere los links.
PLANTILLA_TRIMESTRE = Path("./plantilla_trimestre.html")


# ------------------------------------------------------------
# 4) TRIMESTRE Y AÑO ORIGEN (LO QUE YA EXISTE)
# ------------------------------------------------------------
# Esto le dice al script cuál es el trimestre/año del que está copiando todo.
#
# "1t" = Primer Trimestre
# "2t" = Segundo Trimestre
# "3t" = Tercer Trimestre
# "4t" = Cuarto Trimestre
#
# Ejemplo: si tu carpeta fuente es 1er trimestre de 2024:
FROM_QTAG = "1t"
FROM_YEAR = "2024"


# ------------------------------------------------------------
# 5) TRIMESTRE Y AÑO DESTINO (A LO QUE LO QUIERES CONVERTIR)
# ------------------------------------------------------------
# Aquí defines el trimestre nuevo que vas a generar.
# Ejemplo: quieres crear Cuarto Trimestre 2026:
TO_QTAG   = "4t"
TO_YEAR   = "2026"


# ------------------------------------------------------------
# 6) NOMBRE DE LA CARPETA DONDE VAN LOS ZIPs
# ------------------------------------------------------------
# Dentro de cada carpeta de estado, normalmente existe una carpeta llamada "Archivos"
# donde se guardan los ZIPs que se descargan desde la subpágina.
#
# Esto se usa para:
# - Contar si hay ZIPs disponibles
# - Vincular el botón "Descargar archivo" a su ZIP correcto
ARCHIVOS_FOLDER = "Archivos"


# ------------------------------------------------------------
# 7) (OPCIONAL) COPIAR ZIPs DESDE OTRA CARPETA EXTERNA
# ------------------------------------------------------------
# Este script puede copiar automáticamente los archivos .zip de cada estado
# desde otra carpeta externa (por ejemplo una carpeta donde se guardan los
# documentos oficiales del trimestre).
#
# Si NO usas esta opción, deja ZIP_SOURCE_ROOT = None
# y el script solo revisará si ya existen ZIPs dentro de DEST_ROOT.
#
# ----------------------------------------------------------------
# ESTRUCTURA EXACTA QUE DEBE TENER LA CARPETA DE ZIPs
# ----------------------------------------------------------------
# La carpeta indicada en ZIP_SOURCE_ROOT debe tener esta estructura:
#
# DOCUMENTOS SEP TRIMESTRALES\
# └── Cuarto_Trimestre_2026\              <-- ZIP_SOURCE_ROOT
#     ├── Aguascalientes\
#     │   └── Archivos\
#     │       ├── AnaliticoPlazas_Aguascalientes.zip
#     │       ├── CatalogoTabuladores_Aguascalientes.zip
#     │       └── ...
#     │
#     ├── Baja_California\
#     │   └── Archivos\
#     │       ├── AnaliticoPlazas_Baja_California.zip
#     │       └── ...
#     │
#     ├── Estado_de_Mexico\
#     │   └── Archivos\
#     │       ├── PersonalLicencias_Estado_de_Mexico.zip
#     │       └── ...
#     │
#     └── ...
#
# IMPORTANTE:
# - Cada estado debe tener su propia carpeta.
# - Dentro de cada estado debe existir una carpeta llamada "Archivos".
# - Dentro de "Archivos" deben estar los archivos .zip.
#
#
# ----------------------------------------------------------------
# CÓMO DEBEN LLAMARSE LOS ARCHIVOS ZIP
# ----------------------------------------------------------------
# El script identifica qué ZIP corresponde a cada página usando un
# "prefijo" específico al inicio del nombre del archivo.
#
# Por lo tanto, los ZIP deben comenzar con alguno de estos prefijos:
#
# AnaliticoPlazas_
# CatalogoTabuladores_
# CatalogoPercepDeduc_
# MovimientosPlaza_
# PlazasDocAdmtvasDirec_
# PersonalPagosRetroactivos_
# PersonalLicencias_
# PersonalComisionado_
# PersonalPrejubilatoria_
# PersonalJubilado_
#
#
# EJEMPLOS CORRECTOS:
#
# AnaliticoPlazas_Estado_de_Mexico_4t_2026.zip
# CatalogoTabuladores_Baja_California.zip
# PersonalLicencias_Nuevo_Leon.zip
# MovimientosPlaza_Querétaro.zip
#
#
# EJEMPLOS INCORRECTOS (NO los detectará el script):
#
# Analitico_de_Plazas_Edomex.zip
# Tabuladores_Estado_de_Mexico.zip
# personal_licencias.zip
#
#
# ----------------------------------------------------------------
# CUÁNTOS ZIPs DEBERÍA HABER
# ----------------------------------------------------------------
# Idealmente cada estado debería tener hasta 10 ZIPs, uno por cada reporte:
#
# Analítico de Plazas
# Catálogo de Tabuladores
# Catálogo de Percepciones y Deducciones
# Movimientos de Plazas
# Plazas Docentes Administrativas y Directivas
# Personal con Pagos Retroactivos
# Personal con Licencia
# Personal Comisionado
# Personal con Licencia Prejubilatoria
# Personal Jubilado
#
# Si falta alguno, el script lo reportará automáticamente en el archivo:
#
# reporte_faltantes_zip.csv
#
#
# ----------------------------------------------------------------
# ACTIVAR COPIA AUTOMÁTICA DE ZIPs
# ----------------------------------------------------------------
# Si quieres que el script copie automáticamente los ZIPs desde la carpeta
# externa hacia cada estado dentro de DEST_ROOT, indica aquí la ruta:
#
# Ejemplo:
#
# ZIP_SOURCE_ROOT = Path(
#     r"C:\Users\...\DOCUMENTOS SEP TRIMESTRALES\Cuarto_Trimestre_2026"
# )
#
# Si no quieres copiar ZIPs automáticamente, deja:
ZIP_SOURCE_ROOT: Optional[Path] = None

# ============================================================
# 1) CONSTANTES Y MAPS
# ============================================================

QUARTER = {
    "1t": ("Primer",  "Primer_Trimestre",  "01"),
    "2t": ("Segundo", "Segundo_Trimestre", "02"),
    "3t": ("Tercer",  "Tercer_Trimestre",  "03"),
    "4t": ("Cuarto",  "Cuarto_Trimestre",  "04"),
}

REPORT_KEYS = [
    "analitico_de_plazas",
    "catalogo_de_tabuladores",
    "catalogo_de_percepciones_y_deducciones",
    "movimientos_de_plazas",
    "plazas_docentes_administrativas_y_directivas",
    "personal_con_pagos_retroactivos_hasta_por_45_dias_naturales",
    "personal_con_licencia",
    "personal_comisionado",
    "personal_con_licencia_prejubilatoria",
    "personal_jubilado",
]

REPORT_TITLE = {
    "analitico_de_plazas": "Analitico de Plazas",
    "catalogo_de_percepciones_y_deducciones": "Catalogo de Percepciones y Deducciones",
    "catalogo_de_tabuladores": "Catalogo de Tabuladores",
    "movimientos_de_plazas": "Movimientos de Plazas",
    "personal_comisionado": "Personal Comisionado",
    "personal_jubilado": "Personal Jubilado",
    "personal_con_licencia": "Personal con Licencia",
    "personal_con_pagos_retroactivos_hasta_por_45_dias_naturales": "Personal con Pagos Retroactivos",
    "personal_con_licencia_prejubilatoria": "Personal con Licencia Prejubilatoria",
    "plazas_docentes_administrativas_y_directivas": "Plazas Docentes Administrativas y Directivas",
}

# Para breadcrumb (con acentos oficiales)
STATE_DISPLAY = {
    "Aguascalientes": "Aguascalientes",
    "Baja_California": "Baja California",
    "Baja_California_Sur": "Baja California Sur",
    "Campeche": "Campeche",
    "Coahuila": "Coahuila",
    "Colima": "Colima",
    "Chiapas": "Chiapas",
    "Chihuahua": "Chihuahua",
    "Ciudad_de_Mexico": "Ciudad de México",
    "Durango": "Durango",
    "Guanajuato": "Guanajuato",
    "Guerrero": "Guerrero",
    "Hidalgo": "Hidalgo",
    "Jalisco": "Jalisco",
    "Estado_de_Mexico": "Estado de México",
    "Michoacan": "Michoacán",
    "Morelos": "Morelos",
    "Nayarit": "Nayarit",
    "Nuevo_Leon": "Nuevo León",
    "Oaxaca": "Oaxaca",
    "Puebla": "Puebla",
    "Queretaro": "Querétaro",
    "Quintana_Roo": "Quintana Roo",
    "San_Luis_Potosi": "San Luis Potosí",
    "Sinaloa": "Sinaloa",
    "Sonora": "Sonora",
    "Tabasco": "Tabasco",
    "Tamaulipas": "Tamaulipas",
    "Tlaxcala": "Tlaxcala",
    "Veracruz": "Veracruz",
    "Yucatan": "Yucatán",
    "Zacatecas": "Zacatecas",
}

# ZIP prefix por reporte
ZIP_PREFIX = {
    "analitico_de_plazas": "AnaliticoPlazas_",
    "catalogo_de_percepciones_y_deducciones": "CatalogoPercepDeduc_",
    "catalogo_de_tabuladores": "CatalogoTabuladores_",
    "movimientos_de_plazas": "MovimientosPlaza_",
    "personal_comisionado": "PersonalComisionado_",
    "personal_jubilado": "PersonalJubilado_",
    "personal_con_licencia": "PersonalLicencias_",
    "personal_con_pagos_retroactivos_hasta_por_45_dias_naturales": "PersonalPagosRetroactivos_",
    "personal_con_licencia_prejubilatoria": "PersonalPrejubilatoria_",
    "plazas_docentes_administrativas_y_directivas": "PlazasDocAdmtvasDirec_",
}

REPORT_LABELS = {
    "analitico_de_plazas": (1, "Analítico de Plazas"),
    "catalogo_de_tabuladores": (2, "Catálogo de Tabuladores"),
    "catalogo_de_percepciones_y_deducciones": (3, "Catálogo de Percepciones y Deducciones"),
    "movimientos_de_plazas": (4, "Movimientos de Plazas"),
    "plazas_docentes_administrativas_y_directivas": (5, "Plazas Docentes Administrativas y Directivas"),
    "personal_con_pagos_retroactivos_hasta_por_45_dias_naturales": (6, "Personal con Pagos Retroactivos hasta por 45 días naturales"),
    "personal_con_licencia": (7, "Personal con Licencia"),
    "personal_comisionado": (8, "Personal Comisionado"),
    "personal_con_licencia_prejubilatoria": (9, "Personal con Licencia Prejubilatoria"),
    "personal_jubilado": (10, "Personal Jubilado"),
}

# ============================================================
# 2) ESTRUCTURA DE REPORTE (contadores)
# ============================================================

@dataclass
class Stats:
    paginas_clonadas: int = 0
    entidades_clonadas: int = 0

    carpetas_estados_renombradas: int = 0

    relinks_principales: int = 0
    titles_modificados: int = 0
    h1_modificados: int = 0
    breadcrumbs_modificados: int = 0
    pagina_trimestral_generada: int = 0  # 0/1

    zips_encontrados_total: int = 0
    zips_copiados_entidades: int = 0  # entidades a las que se copió Archivos
    zips_no_encontrados_entidades: int = 0

    descargas_vinculadas: int = 0
    descargas_faltantes: int = 0

    warnings: list[str] = field(default_factory=list)


# ============================================================
# 3) HELPERS GENERALES
# ============================================================

def read_text_smart(p: Path) -> tuple[str, str]:
    encodings = ["utf-8", "utf-8-sig", "cp1252", "latin-1"]
    last_err = None
    for enc in encodings:
        try:
            return p.read_text(encoding=enc), enc
        except Exception as e:
            last_err = e
    return p.read_text(encoding="latin-1", errors="replace"), "latin-1"

def strip_accents(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))

def smart_title_case(words: str) -> str:
    lower_keep = {"de", "del", "y", "la", "el", "los", "las"}
    parts = words.split()
    out = []
    for i, w in enumerate(parts):
        wl = w.lower()
        if i != 0 and wl in lower_keep:
            out.append(wl)
        else:
            out.append(wl.capitalize())
    return " ".join(out)

def estado_title_from_slug(slug: str) -> str:
    txt = smart_title_case(slug.replace("_", " ").strip())
    return strip_accents(txt)

def reporte_title_from_key(key: str) -> str:
    return REPORT_TITLE.get(key, strip_accents(smart_title_case(key.replace("_", " "))))

def parse_entity_folder(name: str) -> Optional[Tuple[str, str, str]]:
    m = re.match(r"^(.+?)_(1t|2t|3t|4t)_(\d{4})$", name, flags=re.IGNORECASE)
    if not m:
        return None
    return m.group(1), m.group(2).lower(), m.group(3)

def find_main_html(entity_dir: Path, qtag: str, year: str) -> Optional[Path]:
    for p in entity_dir.glob("*.html"):
        if re.search(rf"_{qtag}_{year}\.html$", p.name, flags=re.IGNORECASE):
            return p
    return None

def infer_report_key_from_filename(fname: str) -> Optional[str]:
    base = fname.lower().removesuffix(".html")
    m = re.search(r"_(1t|2t|3t|4t)_(\d{4})_", base)
    if not m:
        return None
    report_key = base[:m.start()].strip("_")
    report_key = re.sub(r"_+", "_", report_key)
    return report_key

# ============================================================
# 4) MÓDULO A: CLONACIÓN
# ============================================================

def replace_all(text: str) -> str:
    """Actualiza contenido interno de HTML (rutas, trimestre, zip, etc.)."""
    from_word, from_slug, from_num = QUARTER[FROM_QTAG]
    to_word,   to_slug,   to_num   = QUARTER[TO_QTAG]

    text = text.replace(f"_{FROM_QTAG}_{FROM_YEAR}", f"_{TO_QTAG}_{TO_YEAR}")

    text = re.sub(
        rf"{from_word}(\s|&nbsp;)Trimestre\s+{FROM_YEAR}",
        rf"{to_word}\1Trimestre {TO_YEAR}",
        text,
        flags=re.IGNORECASE
    )

    text = re.sub(
        rf'(/es/sep1/){re.escape(from_slug)}_{FROM_YEAR}',
        rf'\1{to_slug}_{TO_YEAR}',
        text,
        flags=re.IGNORECASE
    )

    text = re.sub(
        rf'(\.\./){re.escape(from_slug)}_{FROM_YEAR}\.html',
        rf'\1{to_slug}_{TO_YEAR}.html',
        text,
        flags=re.IGNORECASE
    )

    text = re.sub(
        rf'(Trimestre_){from_num}(_){FROM_YEAR}(\.zip)',
        rf'\1{to_num}\2{TO_YEAR}\3',
        text,
        flags=re.IGNORECASE
    )

    return text

def clone_entity_folder(src_entity: Path, dst_entity: Path, stats: Stats):
    """Copia estructura y reescribe HTML al trimestre destino."""
    dst_entity.mkdir(parents=True, exist_ok=True)

    for item in src_entity.iterdir():
        if item.is_dir():
            shutil.copytree(item, dst_entity / item.name, dirs_exist_ok=True)
        else:
            if item.suffix.lower() != ".html":
                shutil.copy2(item, dst_entity / item.name)

    for src_html in src_entity.glob("*.html"):
        new_name = src_html.name.replace(f"_{FROM_QTAG}_{FROM_YEAR}", f"_{TO_QTAG}_{TO_YEAR}")
        dst_html = dst_entity / new_name

        txt, _enc = read_text_smart(src_html)
        text2 = replace_all(txt)
        dst_html.write_text(text2, encoding="utf-8")

        stats.paginas_clonadas += 1

def step_clone(stats: Stats):
    """(1) Clona entidades del trimestre fuente al destino."""
    if not SOURCE_ROOT.exists():
        stats.warnings.append(f"[CLONAR] No existe SOURCE_ROOT: {SOURCE_ROOT}")
        return
    DEST_ROOT.mkdir(parents=True, exist_ok=True)

    for src_entity in sorted([p for p in SOURCE_ROOT.iterdir() if p.is_dir()]):
        if not src_entity.name.endswith(f"_{FROM_QTAG}_{FROM_YEAR}"):
            continue

        dst_name = src_entity.name.replace(f"_{FROM_QTAG}_{FROM_YEAR}", f"_{TO_QTAG}_{TO_YEAR}")
        dst_entity = DEST_ROOT / dst_name

        clone_entity_folder(src_entity, dst_entity, stats)
        stats.entidades_clonadas += 1

# ============================================================
# 5) MÓDULO B: RENOMBRE DE CARPETAS DE ESTADOS (quitar _2t_2026)
# ============================================================

def step_rename_state_folders_official(stats: Stats):
    """
    (2) Renombra carpetas de estados:
      - Quita sufijo _nt_YYYY (ej. _2t_2026)
      - Convierte el nombre base (con _) a nombre oficial con acentos (STATE_DISPLAY)
      - Si no está en STATE_DISPLAY, hace fallback: '_' -> ' ' sin acentos.
    """
    if not DEST_ROOT.exists():
        stats.warnings.append(f"[RENOMBRAR ESTADOS] No existe DEST_ROOT: {DEST_ROOT}")
        return

    suffix_re = re.compile(r"^(?P<base>.+?)_(?P<t>[1-4]t)_(?P<y>\d{4})$", re.IGNORECASE)

    renamed_ok = 0
    failed = 0

    for p in sorted(DEST_ROOT.iterdir()):
        if not p.is_dir():
            continue

        name = p.name
        m = suffix_re.match(name)
        base = m.group("base") if m else name  # ej. Estado_de_Mexico

        # Nombre final oficial con acentos
        official = STATE_DISPLAY.get(base, base.replace("_", " ")).strip()

        if official == name:
            continue

        target = p.with_name(official)

        if target.exists():
            stats.warnings.append(f"[RENOMBRAR ESTADOS] NO renombré (ya existe destino): {p.name} -> {target.name}")
            failed += 1
            continue

        try:
            p.rename(target)
            renamed_ok += 1
        except PermissionError as e:
            stats.warnings.append(f"[RENOMBRAR ESTADOS] ERROR (bloqueo/permisos) {p.name} -> {target.name}: {e}")
            failed += 1
        except OSError as e:
            stats.warnings.append(f"[RENOMBRAR ESTADOS] ERROR {p.name} -> {target.name}: {e}")
            failed += 1

    stats.carpetas_estados_renombradas += renamed_ok
    if failed:
        stats.warnings.append(f"[RENOMBRAR ESTADOS] Carpetas NO renombradas: {failed}")

# ============================================================
# 6) MÓDULO C: RELINK de subpáginas en página principal de estado
# ============================================================

def find_existing_subpage(entity_dir: Path, report_key: str, qtag: str, year: str) -> Optional[str]:
    patt = re.compile(rf"^{re.escape(report_key)}_{qtag}_{year}_.+\.html$", re.IGNORECASE)
    for f in sorted(entity_dir.glob("*.html")):
        if patt.match(f.name):
            return f.name
    return None

def relink_main(main_text: str, entity_dir: Path, qtag: str, year: str) -> str:
    new_text = main_text
    for key in REPORT_KEYS:
        real_file = find_existing_subpage(entity_dir, key, qtag, year)
        if not real_file:
            continue

        new_text = re.sub(
            rf'href="[^"]*{re.escape(key)}_{qtag}_{year}_[^"]*\.html"',
            f'href="{real_file}"',
            new_text,
            flags=re.IGNORECASE
        )
        new_text = re.sub(
            rf'href="[^"]*/{re.escape(key)}_{qtag}_{year}_[^"]*"',
            f'href="{real_file}"',
            new_text,
            flags=re.IGNORECASE
        )
        new_text = re.sub(
            rf'href="[^"]*{re.escape(key)}_{qtag}_{year}_[^"]*"',
            f'href="{real_file}"',
            new_text,
            flags=re.IGNORECASE
        )
    return new_text

def step_relink_subpages(stats: Stats):
    """(3) Corrige href en la página principal de cada estado apuntando al archivo real."""
    if not DEST_ROOT.exists():
        stats.warnings.append(f"[RELINK] No existe DEST_ROOT: {DEST_ROOT}")
        return

    for entity_dir in sorted([p for p in DEST_ROOT.iterdir() if p.is_dir()]):
        parsed = parse_entity_folder(entity_dir.name)
        # ojo: si ya renombraste carpetas y quitaste _2t_2026, aquí ya no matchea
        # entonces usamos fallback: inferimos qtag/year desde archivos
        qtag, year = TO_QTAG, TO_YEAR

        main_html = find_main_html(entity_dir, qtag, year)
        if not main_html:
            # si no encontró principal, no podemos relink
            continue

        txt, enc = read_text_smart(main_html)
        new_txt = relink_main(txt, entity_dir, qtag, year)
        if new_txt != txt:
            main_html.write_text(new_txt, encoding=enc)
            stats.relinks_principales += 1

# ============================================================
# 7) MÓDULO D: COPIAR ZIPS (opcional) + CONTAR
# ============================================================

def step_copy_zips(stats: Stats):
    """
    (4) Copia carpeta Archivos desde ZIP_SOURCE_ROOT a cada entidad.
    Si ZIP_SOURCE_ROOT es None, solo cuenta si existen zips en DEST_ROOT.
    """
    if not DEST_ROOT.exists():
        stats.warnings.append(f"[ZIPS] No existe DEST_ROOT: {DEST_ROOT}")
        return

    # Contar zips actuales en destino
    for entity_dir in sorted([p for p in DEST_ROOT.iterdir() if p.is_dir()]):
        zdir = entity_dir / ARCHIVOS_FOLDER
        if zdir.exists():
            stats.zips_encontrados_total += len(list(zdir.glob("*.zip")))

    if ZIP_SOURCE_ROOT is None:
        return

    if not ZIP_SOURCE_ROOT.exists():
        stats.warnings.append(f"[ZIPS] No existe ZIP_SOURCE_ROOT: {ZIP_SOURCE_ROOT}")
        return

    # index por slug de nombre de carpeta
    def slugify(s: str) -> str:
        s = s.strip()
        s = unicodedata.normalize("NFKD", s)
        s = "".join(ch for ch in s if not unicodedata.combining(ch))
        s = re.sub(r"\s+", "_", s)
        s = re.sub(r"[^a-zA-Z0-9_]+", "", s)
        return s.lower()

    origen_idx = {}
    for p in ZIP_SOURCE_ROOT.iterdir():
        if p.is_dir():
            origen_idx[slugify(p.name)] = p

    for entity_dir in sorted([p for p in DEST_ROOT.iterdir() if p.is_dir()]):
        estado_slug = slugify(entity_dir.name.replace("_", " "))
        src_state = origen_idx.get(estado_slug)
        if not src_state:
            stats.zips_no_encontrados_entidades += 1
            continue

        source_archivos = src_state / ARCHIVOS_FOLDER
        target_archivos = entity_dir / ARCHIVOS_FOLDER
        if not source_archivos.exists():
            stats.zips_no_encontrados_entidades += 1
            continue

        if target_archivos.exists():
            shutil.rmtree(target_archivos)
        shutil.copytree(source_archivos, target_archivos)
        stats.zips_copiados_entidades += 1

# ============================================================
# 8) MÓDULO E: VINCULAR DESCARGAS (.zip) EN SUBPÁGINAS
# ============================================================

def find_zip_for_prefix(zips_dir: Path, prefix: str) -> Optional[str]:
    for z in sorted(zips_dir.glob("*.zip")):
        if z.name.startswith(prefix):
            return z.name
    return None

def replace_download_href(html: str, new_href: str) -> str:
    pattern = re.compile(
        r'(<a\s+href=")[^"]+(">\s*[\r\n\s]*<button[^>]*>\s*Descargar archivo\s*</button>\s*[\r\n\s]*</a>)',
        flags=re.IGNORECASE
    )
    return pattern.sub(rf'\1{new_href}\2', html, count=1)

def step_link_downloads(stats: Stats):
    """(5) Vincula botones 'Descargar archivo' al zip correcto. Guarda CSV de faltantes si aplica."""
    if not DEST_ROOT.exists():
        stats.warnings.append(f"[DESCARGAS] No existe DEST_ROOT: {DEST_ROOT}")
        return

    faltantes = []

    for entity_dir in sorted([p for p in DEST_ROOT.iterdir() if p.is_dir()]):
        zips_dir = entity_dir / ARCHIVOS_FOLDER
        if not zips_dir.exists():
            continue

        for html_file in sorted(entity_dir.glob("*.html")):
            # saltar principal del estado
            if re.search(rf"_{TO_QTAG}_{TO_YEAR}\.html$", html_file.name, flags=re.IGNORECASE):
                continue

            report_key = infer_report_key_from_filename(html_file.name)
            if not report_key or report_key not in ZIP_PREFIX:
                stats.descargas_faltantes += 1
                faltantes.append({
                    "entidad": entity_dir.name,
                    "html": html_file.name,
                    "report_key": report_key or "",
                    "prefijo_esperado": "",
                    "zip_encontrado": "",
                    "motivo": "report_key_no_inferida_o_no_en_diccionario"
                })
                continue

            prefix = ZIP_PREFIX[report_key]
            zip_name = find_zip_for_prefix(zips_dir, prefix)
            if not zip_name:
                stats.descargas_faltantes += 1
                faltantes.append({
                    "entidad": entity_dir.name,
                    "html": html_file.name,
                    "report_key": report_key,
                    "prefijo_esperado": prefix,
                    "zip_encontrado": "",
                    "motivo": "no_hay_zip_con_prefijo"
                })
                continue

            txt, enc = read_text_smart(html_file)
            new_txt = replace_download_href(txt, f"{ARCHIVOS_FOLDER}/{zip_name}")
            if new_txt != txt:
                html_file.write_text(new_txt, encoding=enc)
                stats.descargas_vinculadas += 1

    # Reporte faltantes (solo archivo, no spam)
    if faltantes:
        out_csv = DEST_ROOT / "reporte_faltantes_zip.csv"
        with out_csv.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["entidad","html","report_key","prefijo_esperado","zip_encontrado","motivo"])
            w.writeheader()
            w.writerows(faltantes)

# ============================================================
# 9) MÓDULO F: GENERAR/ACTUALIZAR PÁGINA TRIMESTRAL
# ============================================================

def replace_trimestre_text_everywhere(html: str, qtag: str, year: str) -> str:
    word, slug, _num = QUARTER[qtag]

    html = re.sub(
        r"<h2>\s*(Primer|Segundo|Tercer|Cuarto)(?:&nbsp;|\s)Trimestre\s+\d{4}\s*</h2>",
        f"<h2>{word}&nbsp;Trimestre {year}</h2>",
        html,
        flags=re.IGNORECASE
    )

    html = re.sub(
        r'<a href="[^"]+_(?:Trimestre)_\d{4}\.html">\s*(Primer|Segundo|Tercer|Cuarto)\s+Trimestre\s+\d{4}\s*</a>',
        f'<a href="{slug}_{year}.html">{word} Trimestre {year}</a>',
        html,
        flags=re.IGNORECASE
    )
    return html

def rebuild_trimestre_links(html: str, items: list[tuple[str,str]]) -> str:
    new_block = []
    for text, href in items:
        new_block.append(f'            <p><a href="{href}">{text}</a></p>')
        new_block.append("            <hr>")
    block_str = "\n".join(new_block) + "\n"

    pattern = re.compile(r"(?:\s*<p>\s*<a href=.*?</a>\s*</p>\s*<hr>\s*)+",
                         flags=re.IGNORECASE | re.DOTALL)
    return pattern.sub(block_str, html, count=1)

def step_generate_trimestre_page(stats: Stats):
    """(6) Genera la página principal del trimestre con lista de estados sin duplicados."""
    if not DEST_ROOT.exists():
        stats.warnings.append(f"[TRIMESTRE] No existe DEST_ROOT: {DEST_ROOT}")
        return
    if not PLANTILLA_TRIMESTRE.exists():
        stats.warnings.append(f"[TRIMESTRE] No existe PLANTILLA_TRIMESTRE: {PLANTILLA_TRIMESTRE}")
        return

    # Detecta carpetas tipo Estado o Estado_2t_2026 y elimina duplicados
    suffix_re = re.compile(r"^(?P<base>.+?)_(?P<t>[1-4]t)_(?P<y>\d{4})$", re.IGNORECASE)

    # base_estado -> (preferencia, folder_path)
    # preferencia 0 = sin sufijo (mejor), 1 = con sufijo (fallback)
    chosen: dict[str, tuple[int, Path]] = {}

    for d in sorted([p for p in DEST_ROOT.iterdir() if p.is_dir()]):
        m = suffix_re.match(d.name)
        base = m.group("base") if m else d.name
        pref = 1 if m else 0

        # Quédate con la mejor opción
        if base not in chosen or pref < chosen[base][0]:
            chosen[base] = (pref, d)

    entities = []
    for base, (_pref, folder) in sorted(chosen.items(), key=lambda x: x[0].lower()):
        main_html = find_main_html(folder, TO_QTAG, TO_YEAR)
        if not main_html:
            # fallback: busca cualquier principal si no coincide exacto
            for p in folder.glob("*.html"):
                if re.search(r"_(1t|2t|3t|4t)_(\d{4})\.html$", p.name, flags=re.IGNORECASE):
                    main_html = p
                    break
        if not main_html:
            continue

        display = base.replace("_", " ")
        href = f"{folder.name}/{main_html.name}"
        entities.append((display, href))

    if not entities:
        stats.warnings.append("[TRIMESTRE] No se detectaron entidades con HTML principal.")
        return

    word, slug, _num = QUARTER[TO_QTAG]
    trimestre_out = DEST_ROOT / f"{slug}_{TO_YEAR}.html"

    base_html, _enc = read_text_smart(PLANTILLA_TRIMESTRE)
    base_html = replace_trimestre_text_everywhere(base_html, TO_QTAG, TO_YEAR)
    base_html = rebuild_trimestre_links(base_html, entities)

    trimestre_out.write_text(base_html, encoding="utf-8")
    stats.pagina_trimestral_generada = 1

# ============================================================
# 10) MÓDULO G: BREADCRUMB (principal + subpáginas)
# ============================================================

def build_breadcrumb(entity_slug: str, entity_main_filename: str, qtag: str, year: str,
                     current_filename: str, report_item: Optional[Tuple[int, str]]) -> str:
    q_word, q_slug, _n = QUARTER[qtag]
    trimestre_href = f"../{q_slug}_{year}.html"
    trimestre_text = f"{q_word}&nbsp;Trimestre {year}"

    entity_name = entity_slug  # ya viene con nombre oficial (acentos)

    lines = [
        '<ol class="breadcrumb">',
        '    <li class="active"><a href="http://www.gob.mx/"><i class="icon icon-home"></i></a></li>',
        '    <li class="active"><a href="https://dgsanef.sep.gob.mx/">Inicio</a></li>',
        '    <li class="active"><a href="https://dgsanef.sep.gob.mx/Transparencia">Transparencia</a></li>',
        '    <li class="active"><a href="https://dgsanef.sep.gob.mx/art73lgcg">Artículo 73 de la Ley General de',
        '            Contabilidad Gubernamental</a></li>',
        f'    <li class="active"><a href="{trimestre_href}">{trimestre_text}</a></li>',
        f'    <li class="active"><a href="{entity_main_filename}">{entity_name}</a></li>',
    ]
    if report_item is not None:
        n, title = report_item
        lines.append(f'    <li class="active"><a href="{current_filename}">{n}. {title}</a></li>')
    lines.append("</ol>")
    return "\n".join(lines)

def replace_ol_breadcrumb(html: str, new_ol: str) -> str:
    pattern = re.compile(r'<ol class="breadcrumb">.*?</ol>', flags=re.DOTALL | re.IGNORECASE)
    return pattern.sub(new_ol, html, count=1)

def step_breadcrumbs(stats: Stats):
    """(7) Reescribe breadcrumb en principal y subpáginas (funciona con carpetas con espacios/acentos)."""
    if not DEST_ROOT.exists():
        stats.warnings.append(f"[BREADCRUMB] No existe DEST_ROOT: {DEST_ROOT}")
        return

    def find_any_main_html(entity_dir: Path) -> Optional[Path]:
        # 1) ideal: el que termina en _2t_2026.html
        for p in entity_dir.glob("*.html"):
            if re.search(rf"_{TO_QTAG}_{TO_YEAR}\.html$", p.name, flags=re.IGNORECASE):
                return p
        # 2) fallback: cualquiera que sea principal por patrón _nt_YYYY.html
        for p in entity_dir.glob("*.html"):
            if re.search(r"_(1t|2t|3t|4t)_(\d{4})\.html$", p.name, flags=re.IGNORECASE):
                return p
        return None

    for entity_dir in sorted([p for p in DEST_ROOT.iterdir() if p.is_dir()]):
        # ✅ carpeta ya es el nombre oficial (con acentos)
        entity_name_official = entity_dir.name

        main_html = find_any_main_html(entity_dir)
        if not main_html:
            continue

        # principal
        txt, enc = read_text_smart(main_html)
        new_ol = build_breadcrumb(
            entity_slug=entity_name_official,          # ahora pasa el nombre oficial directo
            entity_main_filename=main_html.name,
            qtag=TO_QTAG,
            year=TO_YEAR,
            current_filename=main_html.name,
            report_item=None
        )
        new_txt = replace_ol_breadcrumb(txt, new_ol)
        if new_txt != txt:
            main_html.write_text(new_txt, encoding=enc)
            stats.breadcrumbs_modificados += 1

        # subpáginas
        for sub in sorted(entity_dir.glob("*.html")):
            if sub.name.lower() == main_html.name.lower():
                continue

            rkey = infer_report_key_from_filename(sub.name)
            report_item = REPORT_LABELS.get(rkey) if rkey else None

            txt2, enc2 = read_text_smart(sub)
            new_ol2 = build_breadcrumb(
                entity_slug=entity_name_official,       # nombre oficial directo
                entity_main_filename=main_html.name,
                qtag=TO_QTAG,
                year=TO_YEAR,
                current_filename=sub.name,
                report_item=report_item
            )
            new_txt2 = replace_ol_breadcrumb(txt2, new_ol2)
            if new_txt2 != txt2:
                sub.write_text(new_txt2, encoding=enc2)
                stats.breadcrumbs_modificados += 1

# ============================================================
# 11) MÓDULO H: TITLES + H1 (para evitar “México”)
# ============================================================

TITLE_RE = re.compile(r"(<title\b[^>]*>)(.*?)(</title>)", flags=re.IGNORECASE | re.DOTALL)
H1_RE = re.compile(r"(<h1\b[^>]*>)(.*?)(</h1>)", flags=re.IGNORECASE | re.DOTALL)

RE_ESTADO_HTML = re.compile(r"^([a-z0-9_]+)_(1t|2t|3t|4t)_(\d{4})\.html$", re.IGNORECASE)
RE_REPORTE_HTML = re.compile(r"^([a-z0-9_]+)_(1t|2t|3t|4t)_(\d{4})_([a-z0-9_]{2,30})\.html$", re.IGNORECASE)
RE_TRIMESTRE_MAIN = re.compile(r"^(primer|segundo|tercer|cuarto)_trimestre_(\d{4})\.html$", re.IGNORECASE)

TRIMESTRE_MAP = {"primer": "Primer", "segundo": "Segundo", "tercer": "Tercer", "cuarto": "Cuarto"}

def replace_title(html: str, new_title: str) -> tuple[str, bool]:
    m = TITLE_RE.search(html)
    if not m:
        return html, False
    out = html[:m.start(2)] + new_title + html[m.end(2):]
    return out, (out != html)

def replace_first_h1(html: str, new_h1: str) -> tuple[str, bool]:
    m = H1_RE.search(html)
    if not m:
        return html, False
    out = html[:m.start(2)] + new_h1 + html[m.end(2):]
    return out, (out != html)

def step_titles_and_h1(stats: Stats):
    """(8) Normaliza <title> de páginas de estado y subpáginas; corrige H1 de subpáginas."""
    if not DEST_ROOT.exists():
        stats.warnings.append(f"[TITLES] No existe DEST_ROOT: {DEST_ROOT}")
        return

    for html_path in sorted(DEST_ROOT.rglob("*.html")):
        txt, enc = read_text_smart(html_path)
        new_txt = txt

        new_title: Optional[str] = None

        name = html_path.name

        mrep = RE_REPORTE_HTML.match(name)
        if mrep:
            key = mrep.group(1).lower()
            new_title = reporte_title_from_key(key)

            # H1 debe ser el estado (carpeta padre)
            parent_slug = html_path.parent.name
            h1_state = estado_title_from_slug(parent_slug.lower())
            new_txt, changed_h1 = replace_first_h1(new_txt, h1_state)
            if changed_h1:
                stats.h1_modificados += 1

        else:
            mest = RE_ESTADO_HTML.match(name)
            if mest:
                slug_estado = mest.group(1).lower()
                new_title = estado_title_from_slug(slug_estado)
            else:
                mtri = RE_TRIMESTRE_MAIN.match(name)
                if mtri:
                    tri = TRIMESTRE_MAP.get(mtri.group(1).lower(), mtri.group(1).capitalize())
                    year = mtri.group(2)
                    new_title = f"{tri} Trimestre {year}"

        if new_title:
            new_txt, changed_title = replace_title(new_txt, new_title)
            if changed_title:
                stats.titles_modificados += 1

        if new_txt != txt:
            html_path.write_text(new_txt, encoding=enc)

# ============================================================
# 12) MAIN: ORQUESTADOR + RESUMEN
# ============================================================

def print_summary(stats: Stats):
    print("\n================== RESUMEN PIPELINE ART 73 ==================")
    print(f"Se clonaron {stats.paginas_clonadas} páginas (en {stats.entidades_clonadas} entidades).")
    print(f"Se renombraron {stats.carpetas_estados_renombradas} carpetas de estados (quitando sufijo _nt_YYYY).")
    print(f"Se vincularon {stats.relinks_principales} páginas principales (relink subpáginas).")
    print(f"Se modificó el <title> de {stats.titles_modificados} páginas.")
    print(f"Se corrigió el <h1> de {stats.h1_modificados} subpáginas.")
    print(f"Se generó la página trimestral: {'SI' if stats.pagina_trimestral_generada else 'NO'}.")
    print(f"Se generó/actualizó breadcrumb en {stats.breadcrumbs_modificados} páginas.")

    if stats.zips_encontrados_total == 0:
        print("No se encontraron archivos .zip en carpetas Archivos (para descargas).")
    else:
        print(f"Se detectaron {stats.zips_encontrados_total} archivos .zip en total en Archivos/.")

    if ZIP_SOURCE_ROOT is not None:
        print(f"Se copiaron Archivos/ a {stats.zips_copiados_entidades} entidades (faltaron {stats.zips_no_encontrados_entidades}).")

    if stats.descargas_vinculadas == 0 and stats.descargas_faltantes == 0:
        print("No se procesaron botones de descarga (no había subpáginas o no había Archivos/).")
    else:
        print(f"Se vincularon {stats.descargas_vinculadas} botones de descarga.")
        if stats.descargas_faltantes:
            print(f"Faltaron ZIP para {stats.descargas_faltantes} páginas (ver reporte_faltantes_zip.csv).")

    if stats.warnings:
        print("\n--- WARNINGS (resumen) ---")
        print(f"Total warnings: {len(stats.warnings)}")
        for w in stats.warnings[:10]:
            print("•", w)
        if len(stats.warnings) > 10:
            print(f"... y {len(stats.warnings)-10} más.")
    print("============================================================\n")

def main():
    stats = Stats()

    # Orden sugerido (tu pipeline)
    step_clone(stats)                  # 1) clonación
    step_rename_state_folders_official(stats)   # 2) quitar _2t_2026 a carpetas
    step_relink_subpages(stats)        # 3) relink href en principales
    step_copy_zips(stats)              # 4) copiar/contar zips
    step_link_downloads(stats)         # 5) vincular descargas
    step_generate_trimestre_page(stats)# 6) generar página trimestral
    step_breadcrumbs(stats)            # 7) breadcrumb
    step_titles_and_h1(stats)          # 8) titles + h1

    print_summary(stats)

if __name__ == "__main__":
    main()