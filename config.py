import os

# Imposta qui la cartella radice da cui leggere gli Excel (.xlsx)
# Esempio struttura: root/cartella/sottocartella/[N reports].xlsx
EXCEL_ROOT_DIR = r"C:\path\to\excel\root"  # MODIFICA questo percorso secondo le tue esigenze

# Imposta qui il percorso di output del report Excel generato
OUTPUT_REPORT_PATH = os.path.join(os.getcwd(), "connections_report.xlsx")
