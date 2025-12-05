import os
import sys
import logging
import argparse
from typing import List, Dict, Optional

#!/usr/bin/env python3
"""
reader.py

CLI entry point: scans folders for .xlsx files, extracts SQL queries
from xl/connections.xml, and writes a single Excel report with:
folder_name, file_name, connection, database, table_name, sql query
"""

# Configure logging (applies to imported modules as well)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("xlsx-connection-reader")

from reader_lib import (
    walk_xlsx_files,
    parse_connections_from_xlsx,
    write_excel_report,
    analyze_sql,
    write_summary_report,
)
from config import EXCEL_ROOT_DIR, OUTPUT_REPORT_PATH


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Extract SQL queries from Excel xlsx connections and produce a single report."
    )
    parser.add_argument(
        "-i", "--input",
        required=False,
        help="Root folder to scan for .xlsx files. Defaults to config.EXCEL_ROOT_DIR"
    )
    parser.add_argument(
        "-o", "--output",
        default=OUTPUT_REPORT_PATH,
        help="Output Excel file path. Defaults to config.OUTPUT_REPORT_PATH"
    )
    args = parser.parse_args()

    root_dir = os.path.abspath(args.input or EXCEL_ROOT_DIR)
    output_path = os.path.abspath(args.output)

    if not os.path.isdir(root_dir):
        logger.error(f"Input path is not a directory: {root_dir}")
        return 2

    logger.info(f"Scanning for .xlsx files under: {root_dir}")
    files = walk_xlsx_files(root_dir)
    logger.info(f"Found {len(files)} .xlsx files")

    report_rows: List[Dict[str, Optional[str]]] = []
    error_entries: List[Dict[str, str]] = []
    for fpath in files:
        try:
            entries, err = parse_connections_from_xlsx(fpath)
            if err:
                error_entries.append({"file_path": fpath, "error_type": err})
            if not entries:
                logger.debug(f"No connections found in {fpath}")
                continue

            for e in entries:
                row = {
                    "folder_name": os.path.relpath(os.path.dirname(fpath), start=root_dir),
                    "file_name": os.path.basename(fpath),
                    "connection": e.get("connection"),
                    "database": e.get("database"),
                    "table_name": e.get("table_name"),
                    "sql_query": e.get("sql_query"),
                }
                # Post-process to refine table/database and mark SQL queries
                # Build conn_dict again from the stored connection_string for richer analysis
                conn_dict_local = None
                try:
                    from reader_lib import parse_connection_string  # reuse helper
                    conn_dict_local = parse_connection_string(e.get("connection_string") or "")
                except Exception:
                    conn_dict_local = None

                table_pp, db_pp, sql_flag = analyze_sql(
                    row.get("sql_query"),
                    conn_dict=conn_dict_local,
                    command_type=e.get("command_type")
                )
                row["sql_si_no"] = sql_flag
                if table_pp and not row.get("table_name"):
                    row["table_name"] = table_pp
                if db_pp and not row.get("database"):
                    row["database"] = db_pp

                report_rows.append(row)
        except Exception as ex:
            logger.warning(f"Failed to process {fpath}: {ex}")

    logger.info(f"Collected {len(report_rows)} connection entries")

    if not report_rows:
        logger.warning("No connection entries found. The output file will still be created with headers.")

    try:
        write_excel_report(report_rows, output_path)
        logger.info(f"Report written to: {output_path}")
    except Exception as e:
        logger.error(f"Could not write report: {e}")
        return 3

    # Write error report (all types) if any
    if error_entries:
        err_report_path = os.path.splitext(output_path)[0] + "_errors.csv"
        try:
            with open(err_report_path, "w", encoding="utf-8") as fh:
                fh.write("file_path,error_type\n")
                for item in error_entries:
                    # CSV-safe: wrap in quotes if needed
                    fp = item["file_path"].replace('"', '""')
                    et = item["error_type"].replace('"', '""')
                    fh.write(f'"{fp}","{et}"\n')
            logger.info(f"Error report written to: {err_report_path}")
        except Exception as e:
            logger.warning(f"Failed to write error report: {e}")

    # Post-processing summary Excel
    try:
        summary_path = os.path.splitext(output_path)[0] + "_summary.xlsx"
        write_summary_report(report_rows, error_entries, summary_path)
        logger.info(f"Summary report written to: {summary_path}")
    except Exception as e:
        logger.warning(f"Failed to write summary report: {e}")

    return 0


if __name__ == "__main__":
    sys.exit(main())