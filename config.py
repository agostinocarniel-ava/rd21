import os
from pathlib import Path

"""
Configurazione centralizzata per l'applicazione.

Le impostazioni precedentemente in config.json sono state spostate qui per
facilitare l'import e la validazione tipata. Modifica i valori in base alle
esigenze del tuo ambiente.
"""


CONFIG = {
	"root": "./data",
	"out": "./reports",
	"excel": False,
	"analysis_settings": {
		"include_hidden_sheets": True,
		"include_charts": True,
		"include_named_ranges": True,
		"max_connection_timeout": 30,
		"detailed_table_analysis": True,
		"export_query_formulas": True,
	},
	"output_settings": {
		"generate_json": True,
		"generate_excel": True,
		"include_timestamps": True,
		"compress_output": False,
	},
	"logging": {
		"level": "INFO",
		"log_to_file": False,
		"log_file_path": "excel_analyzer.log",
	},
}


def resolve_paths(cfg: dict) -> dict:
	"""Ritorna una copia della config con percorsi risolti assoluti."""
	root = Path(cfg.get("root", ".")).resolve()
	out = Path(cfg.get("out", "reports")).resolve()
	new_cfg = dict(cfg)
	new_cfg["root"] = str(root)
	new_cfg["out"] = str(out)
	return new_cfg


# Compat per vecchi consumer del modulo
EXCEL_ROOT_DIR = CONFIG["root"]
OUTPUT_REPORT_PATH = str(Path(CONFIG["out"]).resolve() / "connections_report.xlsx")
