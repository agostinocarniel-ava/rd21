# Excel Connections Reader

This tool scans folders for `.xlsx` files and extracts connection information (Database, Table, SQL Query) into a single Excel report. It supports two business logics:

- `zipxml`: Reads `xl/connections.xml` directly from the `.xlsx` file (default)
- `com`: Uses Excel COM via `xlwings` to interrogate `Workbook.Connections`

## Prerequisites
- Python 3.x
- Install packages:
  - Core: `openpyxl`
  - COM mode: `xlwings` (requires Microsoft Excel installed)

```powershell
pip install -r requirements.txt
```

## Usage
Default ZIP/XML logic:
```powershell
python .\reader.py -i "<root_dir>" -o "<output.xlsx>"
```
COM logic (requires Excel + xlwings):
```powershell
python .\reader.py -i "<root_dir>" -o "<output.xlsx>" --logic com
```

## Configuration
Static defaults are in `config.py`:
- `EXCEL_ROOT_DIR`: default input folder
- `OUTPUT_REPORT_PATH`: default output Excel path

CLI args override these defaults.

## Outputs
- Main report: the Excel file with columns: `folder_name`, `file_name`, `connection`, `database`, `table_name`, `sql query`, `SQL si/no`.
- Error CSV: `<output>_errors.csv` with parse errors.
- Summary Excel: `<output>_summary.xlsx` with aggregated metrics.

## Notes
- COM parsing is useful when workbook connections are not fully represented in `xl/connections.xml` or when provider-specific properties are needed.
- For internal Power Query/Mashup sources (e.g., `Data Source=$Workbook$` or `Microsoft.Mashup.OleDb.1`), the `SQL si/no` flag is set to `no`.
