import os
import re
import logging
from typing import List, Dict, Optional, Tuple
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


def _clean_identifier(identifier: str) -> str:
    """Remove brackets or quotes from an identifier and collapse whitespace."""
    ident = identifier.strip()
    # Remove surrounding [] " `
    if ident.startswith("[") and ident.endswith("]"):
        ident = ident[1:-1]
    elif ident.startswith('"') and ident.endswith('"'):
        ident = ident[1:-1]
    elif ident.startswith('`') and ident.endswith('`'):
        ident = ident[1:-1]
    return ident.strip()


def _normalize_sql(sql: str) -> str:
    """Normalize SQL by replacing Excel XML placeholders and collapsing whitespace."""
    s = sql or ""
    # Replace Excel XML newline placeholders
    s = re.sub(r"_x000d__x000a_", " ", s)
    s = re.sub(r"_x000d_", " ", s)
    s = re.sub(r"_x000a_", " ", s)
    # Replace doubled quotes sometimes present in Excel XML
    s = s.replace('""', '"')
    # Collapse whitespace
    s = re.sub(r"\s+", " ", s)
    return s.strip()


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
    sql_norm = _normalize_sql(sql)
    pattern = r"""(?isx)
        \bfrom\b
        \s+
        (
            (?:\[[^\]]+\](?:\s*\.\s*\[[^\]]+\]){0,3})|   # [db].[schema].[table]
            (?:(?:"[^"]+")(?:\s*\.\s*"[^"]+"){0,3})|     # "db"."schema"."table"
            (?:(?:`[^`]+`)(?:\s*\.\s*`[^`]+`){0,3})|          # `db`.`schema`.`table`
            (?:[a-zA-Z0-9_$]+(?:\s*\.\s*[a-zA-Z0-9_$]+){0,3}) # db.schema.table
        )
    """
    m = re.search(pattern, sql_norm)
    if m:
        # Clean bracketed identifier like [dbo].[Table]
        val = m.group(1).strip()
        # If multiple parts follow (e.g., alias), stop at next whitespace or punctuation
        val = re.split(r"[\s;]", val)[0]
        # Normalize identifier parts
        parts = [
            _clean_identifier(p)
            for p in re.split(r"\s*\.\s*", val) if p.strip()
        ]
        # Return most specific name (schema.table if available, else table)
        if len(parts) >= 2:
            return f"{parts[-2]}.{parts[-1]}"
        return parts[-1] if parts else None
    return None


def analyze_sql(sql: Optional[str], conn_dict: Optional[Dict[str, str]] = None, command_type: Optional[str] = None) -> Tuple[Optional[str], Optional[str], str]:
    """
    Analyze a SQL string to extract table and database where possible and
    classify whether it's a real SQL Server query.

    Returns (table_name, database, sql_si_no) where sql_si_no is "si" or "no".
    """
    if not sql:
        return None, None, "no"

    sql_norm = _normalize_sql(sql or "")
    lower = sql_norm.lower()
    # Heuristic to consider as SQL Server query
    is_sql = bool(re.search(r"\b(select|insert|update|delete|with)\b", lower)) and (
        bool(re.search(r"\bfrom\b", lower)) or bool(re.search(r"\binto\b", lower))
    )
    # If commandType indicates table or command-only and identifier looks like db.schema.table, treat as SQL
    if not is_sql and (command_type in {"1", "2", "3", "Table"}):
        if re.search(r"(?is)\b[a-zA-Z0-9_$]+\s*\.\s*[a-zA-Z0-9_$]+\s*\.\s*[a-zA-Z0-9_$]+", lower) or \
           re.search(r"(?is)\"[^\"]+\"\s*\.\s*\"[^\"]+\"\s*\.\s*\"[^\"]+\"", sql_norm):
            is_sql = True
    # Provider hint
    if not is_sql and conn_dict:
        provider = (conn_dict.get("provider") or "").lower()
        if "sqloledb" in provider or "sqlncli" in provider:
            # If provider is SQL Server and we have a plausible identifier or any SELECT
            if re.search(r"\bselect\b", lower) or re.search(r"\bfrom\b", lower) or re.search(r"\.[a-zA-Z0-9_$]+\.[a-zA-Z0-9_$]+", lower):
                is_sql = True

    table = extract_table_from_sql(sql_norm)
    database: Optional[str] = None

    # Try to extract database from a three- or four-part identifier
    if table:
        parts = [_clean_identifier(p) for p in table.split(".")]
        # parts may be schema.table or db.schema.table
        if len(parts) >= 3:
            database = parts[0]

    # Also check for "use <db>" or "database.dbo.table" in the original SQL
    m_use = re.search(r"\buse\s+([\w$]+)\b", lower)
    if m_use and not database:
        database = m_use.group(1)

    return table, database, ("si" if is_sql else "no")


def parse_connections_from_xlsx(xlsx_path: str) -> Tuple[List[Dict[str, Optional[str]]], Optional[str]]:
    """
    Open an .xlsx file, read xl/connections.xml, and extract connection info.
    Returns a list of dicts with keys: connection, database, table_name, sql_query.
    """
    entries: List[Dict[str, Optional[str]]] = []
    try:
        with zipfile.ZipFile(xlsx_path, "r") as zf:
            if "xl/connections.xml" not in zf.namelist():
                logger.debug(f"No xl/connections.xml in {xlsx_path}")
                return entries, None
            xml_bytes = zf.read("xl/connections.xml")
    except zipfile.BadZipFile:
        logger.warning(f"BadZipFile: {xlsx_path}")
        return entries, "BadZipFile"
    except Exception as e:
        logger.warning(f"Failed to open {xlsx_path}: {e}")
        return entries, type(e).__name__

    try:
        root = ET.fromstring(xml_bytes)
    except Exception as e:
        logger.warning(f"Failed to parse connections.xml in {xlsx_path}: {e}")
        return entries, type(e).__name__

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
            "sql_query": sql_query,
            "command_type": command_type,
            "connection_string": conn_str,
            "provider": conn_dict.get("provider") if dbpr is not None else None,
        })

    return entries, None


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

    headers = [
        "folder_name",
        "file_name",
        "connection",
        "database",
        "table_name",
        "sql query",
        "SQL si/no",
    ]
    ws.append(headers)

    for row in rows:
        ws.append([
            row.get("folder_name") or "",
            row.get("file_name") or "",
            row.get("connection") or "",
            row.get("database") or "",
            row.get("table_name") or "",
            row.get("sql_query") or "",
            row.get("sql_si_no") or ""
        ])

    try:
        wb.save(output_path)
    except Exception as e:
        logger.error(f"Failed to save Excel report to {output_path}: {e}")
        raise


def write_summary_report(
    rows: List[Dict[str, Optional[str]]],
    error_entries: List[Dict[str, str]],
    output_summary_path: str,
) -> None:
    """Generate a post-processing Excel summary with key metrics and groupings."""
    if Workbook is None:
        raise RuntimeError("openpyxl is required. Install with: pip install openpyxl")

    wb = Workbook()
    ws = wb.active
    ws.title = "Summary"

    # Metrics
    total_xlsx_read = len({(r.get("folder_name"), r.get("file_name")) for r in rows})
    total_errors = len(error_entries)

    # Unique databases and tables
    dbs = { (r.get("database") or "").strip().lower() for r in rows if r.get("database") }
    tables = { (r.get("table_name") or "").strip().lower() for r in rows if r.get("table_name") }

    # Tables without database
    tables_no_db = { (r.get("table_name") or "").strip().lower()
                     for r in rows if r.get("table_name") and not r.get("database") }

    # SQL queries without database (unique by text)
    sql_no_db = { (r.get("sql_query") or "").strip()
                  for r in rows if r.get("sql_query") and not r.get("database") }

    # Grouping: database -> unique tables count
    db_to_tables: Dict[str, set] = {}
    for r in rows:
        db = (r.get("database") or "").strip()
        tbl = (r.get("table_name") or "").strip()
        if not db:
            continue
        db_key = db.lower()
        if db_key not in db_to_tables:
            db_to_tables[db_key] = set()
        if tbl:
            db_to_tables[db_key].add(tbl.lower())

    # Unique xlsx to pay attention: missing db or missing table
    attention_xlsx = { (r.get("folder_name"), r.get("file_name"))
                       for r in rows if (not r.get("database") or not r.get("table_name")) }

    # Per-xlsx issues: count those where any row lacks db or table or sql
    from collections import defaultdict
    xlsx_rows_map: Dict[tuple, List[Dict[str, Optional[str]]]] = defaultdict(list)
    for r in rows:
        xlsx_rows_map[(r.get("folder_name"), r.get("file_name"))].append(r)

    xlsx_no_db = 0
    xlsx_no_table = 0
    xlsx_no_sql = 0
    for key, rr in xlsx_rows_map.items():
        if any((not x.get("database") or (isinstance(x.get("database"), str) and x.get("database").lower() == "query")) for x in rr):
            xlsx_no_db += 1
        if any((not x.get("table_name") or (isinstance(x.get("table_name"), str) and x.get("table_name").lower() == "query")) for x in rr):
            xlsx_no_table += 1
        if any(not x.get("sql_query") for x in rr):
            xlsx_no_sql += 1

    # Write metrics
    ws.append(["Metric", "Value"])
    ws.append(["n. di file .xlsx letti", total_xlsx_read])
    ws.append(["n. di file che hanno generato errore", total_errors])
    ws.append(["n. di database univoci identificati", len(dbs)])
    ws.append(["n. di tabelle univoche identificate", len(tables)])
    ws.append(["n. di tabelle univoche senza database", len(tables_no_db)])
    ws.append(["n. di query sql univoche senza database", len(sql_no_db)])
    ws.append(["N. di xlsx univoci da attenzionare (senza db o tabella)", len(attention_xlsx)])
    ws.append(["N. di xlsx univoci (righe senza db)", xlsx_no_db])
    ws.append(["N. di xlsx univoci (righe senza tabella)", xlsx_no_table])
    ws.append(["N. di xlsx univoci (righe senza sql)", xlsx_no_sql])

    # Blank row then grouping
    ws.append(["", ""])
    ws.append(["Nome Database", "tabelle univoche identificate"])
    for db_key, tbls in sorted(db_to_tables.items()):
        ws.append([db_key, len(tbls)])

    wb.save(output_summary_path)
