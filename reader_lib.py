import os
import re
import logging
from typing import List, Dict, Optional
import zipfile
import xml.etree.ElementTree as ET

try:
    from openpyxl import Workbook
except ImportError:
    Workbook = None

logger = logging.getLogger(__name__)

NS = {"ssml": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}


def parse_connection_string(conn_str: str) -> Dict[str, str]:
    """Parse a semi-colon separated connection string into a dict of lower-cased keys."""
    result: Dict[str, str] = {}
    if not conn_str:
        return result
    parts = [p for p in conn_str.split(";") if p.strip()]
    for p in parts:
        if "=" in p:
            k, v = p.split("=", 1)
            result[k.strip().lower()] = v.strip()
    return result


def extract_database_from_conn_dict(conn_dict: Dict[str, str]) -> Optional[str]:
    """Extract database name from common keys in a parsed connection string."""
    for key in ("initial catalog", "database"):
        if key in conn_dict and conn_dict[key]:
            return conn_dict[key]
    return None


def extract_table_from_sql(sql: str) -> Optional[str]:
    """
    Best-effort extraction of first table after FROM in a SQL query.
    Handles quoted identifiers [], "", `` and dotted names.
    """
    if not sql:
        return None
    # Quick check: if command is a plain table name (no SELECT), treat it as table
    if re.match(r"^[\[\]`\"a-zA-Z0-9_.]+$", sql.strip()) and not re.search(r"\bselect\b", sql, re.IGNORECASE):
        return sql.strip()

    # Try to capture table after FROM
    pattern = r"""(?isx)
        \bfrom\b
        \s+
        (
            (?:\[[^\]]+\])|        # [schema].[table] or [table]
            (?:"[^"]+")|           # "schema"."table" or "table"
            (?:`[^`]+`)|           # `schema`.`table` or `table`
            (?:[a-zA-Z0-9_$.]+)    # schema.table or table
        )
    """
    m = re.search(pattern, sql)
    if m:
        # Clean bracketed identifier like [dbo].[Table]
        val = m.group(1).strip()
        # If multiple parts follow (e.g., alias), stop at next whitespace or punctuation
        val = re.split(r"[\s;]", val)[0]
        return val
    return None


def parse_connections_from_xlsx(xlsx_path: str) -> List[Dict[str, Optional[str]]]:
    """
    Open an .xlsx file, read xl/connections.xml, and extract connection info.
    Returns a list of dicts with keys: connection, database, table_name, sql_query.
    """
    entries: List[Dict[str, Optional[str]]] = []
    try:
        with zipfile.ZipFile(xlsx_path, "r") as zf:
            if "xl/connections.xml" not in zf.namelist():
                logger.debug(f"No xl/connections.xml in {xlsx_path}")
                return entries
            xml_bytes = zf.read("xl/connections.xml")
    except zipfile.BadZipFile:
        logger.warning(f"BadZipFile: {xlsx_path}")
        return entries
    except Exception as e:
        logger.warning(f"Failed to open {xlsx_path}: {e}")
        return entries

    try:
        root = ET.fromstring(xml_bytes)
    except Exception as e:
        logger.warning(f"Failed to parse connections.xml in {xlsx_path}: {e}")
        return entries

    # Root is <connections>, children are <connection>
    for conn in root.findall("ssml:connection", NS):
        conn_name = conn.get("name") or conn.get("id") or ""
        dbpr = conn.find("ssml:dbPr", NS)

        sql_query = None
        database = None
        table_name = None

        if dbpr is not None:
            conn_str = dbpr.get("connection") or ""
            command = dbpr.get("command") or ""
            command_type = dbpr.get("commandType") or ""

            # Parse DB from connection string
            conn_dict = parse_connection_string(conn_str)
            database = extract_database_from_conn_dict(conn_dict)

            # SQL query
            sql_query = command if command else None

            # Table name
            table_name = extract_table_from_sql(command)

            # If commandType suggests table and command looks like table name
            # Excel commandType values vary; we use heuristic only
            if not table_name and command and command_type in {"1", "2", "Table"}:
                table_name = extract_table_from_sql(command)

        else:
            # Other connection types (olapPr, webPr, textPr) may exist; try to glean info
            # For OLAP, there might be an <olapPr> with db info; we skip complex parsing
            olap = conn.find("ssml:olapPr", NS)
            textpr = conn.find("ssml:textPr", NS)
            webpr = conn.find("ssml:webPr", NS)
            # Not SQL; leave fields as None
            if olap is not None or textpr is not None or webpr is not None:
                logger.debug(f"Non-DB connection type detected in {xlsx_path} for connection '{conn_name}'")

        entries.append({
            "connection": conn_name,
            "database": database,
            "table_name": table_name,
            "sql_query": sql_query
        })

    return entries


def walk_xlsx_files(root_dir: str) -> List[str]:
    """Return a list of .xlsx file paths found under root_dir, excluding temp files (~$)."""
    results: List[str] = []
    for dirpath, _, filenames in os.walk(root_dir):
        for fname in filenames:
            if fname.lower().endswith(".xlsx") and not fname.startswith("~$"):
                results.append(os.path.join(dirpath, fname))
    return results


def write_excel_report(rows: List[Dict[str, Optional[str]]], output_path: str) -> None:
    """Write the collected rows to a single Excel file using openpyxl."""
    if Workbook is None:
        raise RuntimeError("openpyxl is required. Install with: pip install openpyxl")

    wb = Workbook()
    ws = wb.active
    ws.title = "Connections"

    headers = ["folder_name", "file_name", "connection", "database", "table_name", "sql query"]
    ws.append(headers)

    for row in rows:
        ws.append([
            row.get("folder_name") or "",
            row.get("file_name") or "",
            row.get("connection") or "",
            row.get("database") or "",
            row.get("table_name") or "",
            row.get("sql_query") or ""
        ])

    try:
        wb.save(output_path)
    except Exception as e:
        logger.error(f"Failed to save Excel report to {output_path}: {e}")
        raise
