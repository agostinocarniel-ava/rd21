import os
import logging
from typing import List, Dict, Optional, Tuple

logger = logging.getLogger(__name__)

def _safe_get(conn, attr: str):
    try:
        return getattr(conn, attr)
    except Exception:
        return None

def _extract_table_from_command(command: Optional[str]) -> Optional[str]:
    if not command:
        return None
    cmd = str(command).strip()
    # Basic heuristic: if command looks like a bare identifier, return it
    import re
    if re.match(r"^[\[\]`\"a-zA-Z0-9_.]+$", cmd) and not re.search(r"\bselect\b", cmd, re.IGNORECASE):
        return cmd
    # Fallback: try FROM capture like reader_lib.extract_table_from_sql
    try:
        from reader_lib import extract_table_from_sql
        return extract_table_from_sql(cmd)
    except Exception:
        return None

def parse_connections_via_com(xlsx_path: str) -> Tuple[List[Dict[str, Optional[str]]], Optional[str]]:
    """
    Use Excel COM (xlwings/pywin32) to inspect Workbook.Connections and extract
    connection name, database, table and SQL command.
    Returns list of entries and optional error type.
    """
    entries: List[Dict[str, Optional[str]]] = []
    try:
        import xlwings as xw
    except Exception as e:
        logger.error("xlwings is required for COM-based parsing. Install with: pip install xlwings")
        return entries, "MissingDependency"

    app = None
    wb = None
    try:
        app = xw.App(visible=False)
        wb = app.books.open(xlsx_path, read_only=True)

        # Workbook connections collection via COM
        conns = wb.api.Connections
        count = int(conns.Count)
        for i in range(1, count + 1):
            conn = conns.Item(i)
            name = _safe_get(conn, 'Name') or ''
            # Many providers expose OLEDBConnection/ODBCConnection with properties
            oledb = _safe_get(conn, 'OLEDBConnection')
            odbc = _safe_get(conn, 'ODBCConnection')

            connection_string = ''
            command_text = None
            command_type = None
            provider = None

            try:
                if oledb:
                    connection_string = str(_safe_get(oledb, 'Connection')) or ''
                    command_text = _safe_get(oledb, 'CommandText')
                    command_type = _safe_get(oledb, 'CommandType')
                    provider = str(_safe_get(oledb, 'OLEDBConnection')) or None
                elif odbc:
                    connection_string = str(_safe_get(odbc, 'Connection')) or ''
                    command_text = _safe_get(odbc, 'CommandText')
                    command_type = _safe_get(odbc, 'CommandType')
            except Exception:
                pass

            # Parse DB from connection string using existing helper
            database = None
            table_name = None
            sql_query = None
            try:
                from reader_lib import parse_connection_string, extract_database_from_conn_dict
                conn_dict = parse_connection_string(connection_string)
                database = extract_database_from_conn_dict(conn_dict)
            except Exception:
                conn_dict = {}

            # Command text may be list/tuple from COM; normalize to first element
            if isinstance(command_text, (list, tuple)) and command_text:
                sql_query = str(command_text[0])
            elif command_text is not None:
                sql_query = str(command_text)

            table_name = _extract_table_from_command(sql_query)

            entries.append({
                "connection": name,
                "database": database,
                "table_name": table_name,
                "sql_query": sql_query,
                "command_type": str(command_type) if command_type is not None else None,
                "connection_string": connection_string,
                "provider": provider,
            })

        return entries, None
    except Exception as e:
        logger.warning(f"COM parse failed for {xlsx_path}: {e}")
        return entries, type(e).__name__
    finally:
        try:
            if wb:
                wb.close()
        except Exception:
            pass
        try:
            if app:
                app.quit()
        except Exception:
            pass
