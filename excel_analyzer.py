#!/usr/bin/env python3
"""
Excel Analyzer - Inventario completo di tabelle, connessioni e query
Utilizza xlwings per analizzare file Excel e estrarre informazioni dettagliate.
"""

import xlwings as xw
import pandas as pd
import json
import re
from datetime import datetime
from pathlib import Path
import win32com.client as win32
from typing import Dict, List, Any, Set, Tuple
import logging
from config import CONFIG, resolve_paths


def parse_database_info_from_formula(formula: str) -> Dict[str, Any]:
    """
    Estrae informazioni su database, schema e tabelle da una formula Power Query
    
    Args:
        formula: Formula M di Power Query
        
    Returns:
        Dictionary con informazioni estratte
    """
    db_info = {
        'databases': [],
        'servers': [],
        'schemas': [],
        'tables': [],
        'sources': []
    }
    
    if not formula:
        return db_info
    
    try:
        # Pattern per SQL Database
        sql_db_pattern = r'Sql\.Database\s*\(\s*"([^"]+)"\s*,\s*"([^"]+)"\s*\)'
        sql_matches = re.findall(sql_db_pattern, formula)
        for server, database in sql_matches:
            db_info['servers'].append(server)
            db_info['databases'].append(database)
            db_info['sources'].append(f"SQL Server: {server}/{database}")
        
        # Pattern per schema e tabelle
        schema_table_pattern = r'\[Schema="([^"]+)"\s*,\s*Item="([^"]+)"\]'
        st_matches = re.findall(schema_table_pattern, formula)
        for schema, table in st_matches:
            db_info['schemas'].append(schema)
            db_info['tables'].append(table)
        
        # Pattern per Oracle Database
        oracle_pattern = r'Oracle\.Database\s*\(\s*"([^"]+)"\s*,?\s*"?([^"]*)"?\s*\)'
        oracle_matches = re.findall(oracle_pattern, formula)
        for server, service in oracle_matches:
            db_info['servers'].append(server)
            if service:
                db_info['databases'].append(service)
            db_info['sources'].append(f"Oracle: {server}" + (f"/{service}" if service else ""))
        
        # Pattern per MySQL/PostgreSQL
        mysql_pattern = r'MySql\.Database\s*\(\s*"([^"]+)"\s*,\s*"([^"]+)"\s*\)'
        mysql_matches = re.findall(mysql_pattern, formula)
        for server, database in mysql_matches:
            db_info['servers'].append(server)
            db_info['databases'].append(database)
            db_info['sources'].append(f"MySQL: {server}/{database}")
        
        postgresql_pattern = r'PostgreSQL\.Database\s*\(\s*"([^"]+)"\s*,\s*"([^"]+)"\s*\)'
        pg_matches = re.findall(postgresql_pattern, formula)
        for server, database in pg_matches:
            db_info['servers'].append(server)
            db_info['databases'].append(database)
            db_info['sources'].append(f"PostgreSQL: {server}/{database}")
        
        # Pattern per Web/OData sources
        web_pattern = r'Web\.Contents\s*\(\s*"([^"]+)"\s*\)'
        web_matches = re.findall(web_pattern, formula)
        for url in web_matches:
            db_info['sources'].append(f"Web: {url}")
        
        odata_pattern = r'OData\.Feed\s*\(\s*"([^"]+)"\s*\)'
        odata_matches = re.findall(odata_pattern, formula)
        for url in odata_matches:
            db_info['sources'].append(f"OData: {url}")
        
        # Pattern per Excel/CSV files
        excel_pattern = r'Excel\.Workbook\s*\(\s*[^)]*"([^"]+\.xlsx?)"'
        excel_matches = re.findall(excel_pattern, formula)
        for file_path in excel_matches:
            db_info['sources'].append(f"Excel: {file_path}")
        
        csv_pattern = r'Csv\.Document\s*\(\s*[^)]*"([^"]+\.csv)"'
        csv_matches = re.findall(csv_pattern, formula)
        for file_path in csv_matches:
            db_info['sources'].append(f"CSV: {file_path}")
        
        # Rimuovi duplicati mantenendo l'ordine
        db_info['databases'] = list(dict.fromkeys(db_info['databases']))
        db_info['servers'] = list(dict.fromkeys(db_info['servers']))
        db_info['schemas'] = list(dict.fromkeys(db_info['schemas']))
        db_info['tables'] = list(dict.fromkeys(db_info['tables']))
        db_info['sources'] = list(dict.fromkeys(db_info['sources']))
        
    except Exception as e:
        # Log dell'errore ma continua l'esecuzione
        pass
    
    return db_info


def parse_database_info_from_connection_string(conn_string: str) -> Dict[str, Any]:
    """
    Estrae informazioni database da stringa di connessione
    
    Args:
        conn_string: Stringa di connessione
        
    Returns:
        Dictionary con informazioni estratte
    """
    db_info = {
        'server': None,
        'database': None,
        'provider': None,
        'connection_type': 'Unknown'
    }
    
    if not conn_string:
        return db_info
    
    try:
        # Provider
        provider_match = re.search(r'Provider=([^;]+)', conn_string, re.IGNORECASE)
        if provider_match:
            db_info['provider'] = provider_match.group(1)
        
        # Server/Data Source
        server_patterns = [
            r'Server=([^;]+)',
            r'Data Source=([^;]+)',
            r'HOST=([^;]+)'
        ]
        for pattern in server_patterns:
            match = re.search(pattern, conn_string, re.IGNORECASE)
            if match:
                db_info['server'] = match.group(1)
                break
        
        # Database/Initial Catalog
        db_patterns = [
            r'Database=([^;]+)',
            r'Initial Catalog=([^;]+)',
            r'DBQ=([^;]+)'
        ]
        for pattern in db_patterns:
            match = re.search(pattern, conn_string, re.IGNORECASE)
            if match:
                db_info['database'] = match.group(1)
                break
        
        # Determina il tipo di connessione
        if 'sqlserver' in conn_string.lower() or 'sql server' in conn_string.lower():
            db_info['connection_type'] = 'SQL Server'
        elif 'oracle' in conn_string.lower():
            db_info['connection_type'] = 'Oracle'
        elif 'mysql' in conn_string.lower():
            db_info['connection_type'] = 'MySQL'
        elif 'postgresql' in conn_string.lower() or 'postgres' in conn_string.lower():
            db_info['connection_type'] = 'PostgreSQL'
        elif 'oledb' in conn_string.lower():
            db_info['connection_type'] = 'OLE DB'
        elif 'odbc' in conn_string.lower():
            db_info['connection_type'] = 'ODBC'
        
    except Exception as e:
        pass
    
    return db_info


def clean_data_for_excel(data):
    """
    Pulisce i dati per renderli compatibili con Excel/pandas
    Converte None in stringhe vuote e gestisce tipi incompatibili
    """
    if isinstance(data, dict):
        return {key: clean_data_for_excel(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [clean_data_for_excel(item) for item in data]
    elif data is None:
        return ''
    elif hasattr(data, 'isoformat'):
        return data.isoformat()
    else:
        return data

# Configurazione logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ExcelAnalyzer:
    """Classe principale per l'analisi di file Excel"""
    
    def __init__(self, file_path: str):
        """
        Inizializza l'analizzatore Excel
        
        Args:
            file_path: Percorso del file Excel da analizzare
        """
        self.file_path = Path(file_path)
        self.workbook = None
        self.app = None
        self.inventory = {
            'file_info': {},
            'worksheets': {},
            'tables': [],
            'pivot_tables': [],
            'connections': [],
            'queries': [],
            'query_tables': [],
            'named_ranges': [],
            'charts': [],
            'external_data': []
        }
    
    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()
    
    def connect(self):
        """Connette all'applicazione Excel e apre il workbook"""
        try:
            logger.info(f"Connessione al file Excel: {self.file_path}")
            
            # Connessione a Excel tramite xlwings
            self.app = xw.App(visible=False, add_book=False)
            self.workbook = self.app.books.open(str(self.file_path))
            
            logger.info("Connessione stabilita con successo")
            
        except Exception as e:
            logger.error(f"Errore durante la connessione: {e}")
            raise
    
    def disconnect(self):
        """Chiude la connessione a Excel"""
        try:
            if self.workbook:
                self.workbook.close()
            if self.app:
                self.app.quit()
            logger.info("Connessione chiusa")
        except Exception as e:
            logger.warning(f"Errore durante la chiusura: {e}")
    
    def analyze_file_info(self):
        """Analizza le informazioni generali del file"""
        try:
            logger.info("Analisi informazioni file...")
            
            wb = self.workbook.api  # Accesso all'oggetto COM
            
            # Ottieni le proprietà del documento in modo sicuro
            creation_date = None
            last_modified = None
            author = None
            
            try:
                if hasattr(wb, 'BuiltinDocumentProperties'):
                    try:
                        creation_date = wb.BuiltinDocumentProperties("Creation Date").Value
                        if creation_date:
                            creation_date = creation_date.isoformat() if hasattr(creation_date, 'isoformat') else str(creation_date)
                    except:
                        pass
                    
                    try:
                        last_modified = wb.BuiltinDocumentProperties("Last Save Time").Value
                        if last_modified:
                            last_modified = last_modified.isoformat() if hasattr(last_modified, 'isoformat') else str(last_modified)
                    except:
                        pass
                    
                    try:
                        author = wb.BuiltinDocumentProperties("Author").Value
                    except:
                        pass
            except:
                pass
            
            self.inventory['file_info'] = {
                'file_name': self.file_path.name,
                'file_path': str(self.file_path),
                'file_size': self.file_path.stat().st_size if self.file_path.exists() else 0,
                'creation_date': creation_date,
                'last_modified': last_modified,
                'author': author,
                'worksheets_count': len(self.workbook.sheets),
                'analysis_date': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Errore nell'analisi delle informazioni del file: {e}")
            self.inventory['file_info'] = {
                'file_name': self.file_path.name,
                'file_path': str(self.file_path),
                'error': str(e)
            }
    
    def analyze_worksheets(self):
        """Analizza tutti i fogli di lavoro"""
        try:
            logger.info("Analisi fogli di lavoro...")
            
            for sheet in self.workbook.sheets:
                sheet_info = {
                    'name': sheet.name,
                    'visible': sheet.visible,
                    'used_range': None,
                    'row_count': 0,
                    'column_count': 0,
                    'has_data': False
                }
                
                try:
                    # Informazioni sulla range utilizzata
                    used_range = sheet.used_range
                    if used_range:
                        sheet_info['used_range'] = used_range.address
                        sheet_info['row_count'] = used_range.last_cell.row
                        sheet_info['column_count'] = used_range.last_cell.column
                        sheet_info['has_data'] = True
                except Exception as e:
                    logger.warning(f"Errore nell'analisi del foglio {sheet.name}: {e}")
                
                self.inventory['worksheets'][sheet.name] = sheet_info
                
        except Exception as e:
            logger.error(f"Errore nell'analisi dei fogli di lavoro: {e}")
    
    def analyze_tables(self):
        """Analizza tutte le tabelle Excel"""
        try:
            logger.info("Analisi tabelle Excel...")
            
            for sheet in self.workbook.sheets:
                try:
                    sheet_api = sheet.api
                    
                    # Analisi ListObjects (tabelle Excel)
                    for table in sheet_api.ListObjects:
                        table_info = {
                            'name': table.Name,
                            'worksheet': sheet.name,
                            'range': table.Range.Address,
                            'header_row': table.HeaderRowRange.Address if table.HeaderRowRange else None,
                            'data_body_range': table.DataBodyRange.Address if table.DataBodyRange else None,
                            'total_row': table.TotalsRowRange.Address if table.TotalsRowRange else None,
                            'columns': [],
                            'row_count': table.Range.Rows.Count,
                            'column_count': table.Range.Columns.Count
                        }
                        
                        # Informazioni sulle colonne
                        for col in table.ListColumns:
                            col_info = {
                                'name': col.Name,
                                'index': col.Index,
                                'data_type': 'Unknown'  # Excel non espone facilmente il tipo di dati
                            }
                            table_info['columns'].append(col_info)
                        
                        self.inventory['tables'].append(table_info)
                        
                except Exception as e:
                    logger.warning(f"Errore nell'analisi delle tabelle del foglio {sheet.name}: {e}")
                    
        except Exception as e:
            logger.error(f"Errore generale nell'analisi delle tabelle: {e}")
    
    def analyze_pivot_tables(self):
        """Analizza tutte le tabelle pivot"""
        try:
            logger.info("Analisi tabelle pivot...")
            
            for sheet in self.workbook.sheets:
                try:
                    sheet_api = sheet.api
                    
                    for pivot_table in sheet_api.PivotTables():
                        pivot_info = {
                            'name': pivot_table.Name,
                            'worksheet': sheet.name,
                            'source_data': pivot_table.SourceData,
                            'table_range': pivot_table.TableRange2.Address,
                            'page_fields': [],
                            'row_fields': [],
                            'column_fields': [],
                            'data_fields': []
                        }
                        
                        # Campi pagina
                        for field in pivot_table.PageFields():
                            pivot_info['page_fields'].append(field.Name)
                        
                        # Campi riga
                        for field in pivot_table.RowFields():
                            pivot_info['row_fields'].append(field.Name)
                        
                        # Campi colonna
                        for field in pivot_table.ColumnFields():
                            pivot_info['column_fields'].append(field.Name)
                        
                        # Campi dati
                        for field in pivot_table.DataFields():
                            pivot_info['data_fields'].append({
                                'name': field.Name,
                                'function': field.Function
                            })
                        
                        self.inventory['pivot_tables'].append(pivot_info)
                        
                except Exception as e:
                    logger.warning(f"Errore nell'analisi delle tabelle pivot del foglio {sheet.name}: {e}")
                    
        except Exception as e:
            logger.error(f"Errore generale nell'analisi delle tabelle pivot: {e}")
    
    def analyze_connections(self):
        """Analizza tutte le connessioni dati"""
        try:
            logger.info("Analisi connessioni dati...")
            
            wb_api = self.workbook.api
            
            # Verifica se ci sono connessioni
            try:
                connections_count = wb_api.Connections.Count
                logger.info(f"Trovate {connections_count} connessioni nel workbook")
                
                if connections_count == 0:
                    logger.info("Nessuna connessione dati trovata nel file")
                    return
                
            except Exception as e:
                logger.info(f"Nessuna connessione dati disponibile o errore nell'accesso: {e}")
                return
            
            # Analizza ogni connessione
            for i in range(1, connections_count + 1):
                try:
                    connection = wb_api.Connections(i)
                    
                    conn_info = {
                        'name': 'Unknown',
                        'description': '',
                        'type': 'Unknown',
                        'ole_db_connection': None,
                        'odbc_connection': None,
                        'web_tables': [],
                        'database_info': {}
                    }
                    
                    # Nome connessione (sicuro)
                    try:
                        conn_info['name'] = connection.Name
                    except:
                        conn_info['name'] = f"Connection_{i}"
                    
                    # Descrizione (sicuro)
                    try:
                        conn_info['description'] = connection.Description
                    except:
                        pass
                    
                    # Dettagli connessione OLE DB
                    try:
                        if hasattr(connection, 'OLEDBConnection') and connection.OLEDBConnection:
                            ole_conn = connection.OLEDBConnection
                            conn_info['type'] = 'OLE DB'
                            conn_string = getattr(ole_conn, 'Connection', '')
                            conn_info['ole_db_connection'] = {
                                'connection_string': conn_string,
                                'command_text': getattr(ole_conn, 'CommandText', ''),
                                'command_type': getattr(ole_conn, 'CommandType', 0),
                                'refresh_on_file_open': getattr(ole_conn, 'RefreshOnFileOpen', False),
                                'save_password': getattr(ole_conn, 'SavePassword', False)
                            }
                            
                            # Estrai informazioni database dalla stringa di connessione
                            if conn_string:
                                db_details = parse_database_info_from_connection_string(conn_string)
                                conn_info['database_info'] = db_details
                                
                    except Exception as ole_err:
                        logger.debug(f"Errore nell'analisi connessione OLE DB per {conn_info['name']}: {ole_err}")
                    
                    # Dettagli connessione ODBC
                    try:
                        if hasattr(connection, 'ODBCConnection') and connection.ODBCConnection:
                            odbc_conn = connection.ODBCConnection
                            conn_info['type'] = 'ODBC'
                            conn_string = getattr(odbc_conn, 'Connection', '')
                            conn_info['odbc_connection'] = {
                                'connection_string': conn_string,
                                'sql': getattr(odbc_conn, 'Sql', ''),
                                'refresh_on_file_open': getattr(odbc_conn, 'RefreshOnFileOpen', False),
                                'save_password': getattr(odbc_conn, 'SavePassword', False)
                            }
                            
                            # Estrai informazioni database dalla stringa di connessione
                            if conn_string:
                                db_details = parse_database_info_from_connection_string(conn_string)
                                conn_info['database_info'] = db_details
                                
                    except Exception as odbc_err:
                        logger.debug(f"Errore nell'analisi connessione ODBC per {conn_info['name']}: {odbc_err}")
                    
                    # Connessioni Web
                    try:
                        if hasattr(connection, 'WebTables'):
                            conn_info['type'] = 'Web'
                            # Gestione Web Tables se necessario
                    except Exception as web_err:
                        logger.debug(f"Errore nell'analisi connessione Web per {conn_info['name']}: {web_err}")
                    
                    self.inventory['connections'].append(conn_info)
                    logger.info(f"Connessione analizzata: {conn_info['name']} ({conn_info['type']})")
                    
                except Exception as conn_err:
                    logger.warning(f"Errore nell'analisi della connessione {i}: {conn_err}")
                    continue
                
        except Exception as e:
            logger.warning(f"Errore generale nell'analisi delle connessioni: {e}")
            logger.info("Il file potrebbe non contenere connessioni dati o potrebbero non essere accessibili")
    
    def analyze_queries(self):
        """Analizza Power Query e altre query"""
        try:
            logger.info("Analisi Power Query...")
            
            wb_api = self.workbook.api
            
            # Verifica disponibilità Power Query
            queries_found = False
            
            # Metodo 1: Accesso diretto a Queries
            try:
                if hasattr(wb_api, 'Queries'):
                    queries_count = wb_api.Queries.Count
                    logger.info(f"Trovate {queries_count} Power Query nel workbook")
                    
                    if queries_count > 0:
                        queries_found = True
                        for i in range(1, queries_count + 1):
                            try:
                                query = wb_api.Queries(i)
                                query_info = {
                                    'name': 'Unknown',
                                    'type': 'Power Query',
                                    'formula': '',
                                    'description': '',
                                    'refresh_on_file_open': False,
                                    'connection': None
                                }
                                
                                # Estrai informazioni in modo sicuro
                                try:
                                    query_info['name'] = query.Name
                                except:
                                    query_info['name'] = f"Query_{i}"
                                
                                try:
                                    query_info['formula'] = query.Formula
                                    
                                    # Analizza la formula per estrarre informazioni database
                                    if query_info['formula']:
                                        db_details = parse_database_info_from_formula(query_info['formula'])
                                        query_info['database_info'] = {
                                            'servers': db_details['servers'],
                                            'databases': db_details['databases'],
                                            'schemas': db_details['schemas'],
                                            'tables': db_details['tables'],
                                            'sources': db_details['sources']
                                        }
                                    else:
                                        query_info['database_info'] = {}
                                        
                                except:
                                    query_info['database_info'] = {}
                                
                                try:
                                    query_info['description'] = query.Description
                                except:
                                    pass
                                
                                try:
                                    query_info['refresh_on_file_open'] = query.RefreshOnFileOpen
                                except:
                                    pass
                                
                                try:
                                    if hasattr(query, 'Connection'):
                                        query_info['connection'] = query.Connection.Name if query.Connection else None
                                except:
                                    pass
                                
                                self.inventory['queries'].append(query_info)
                                logger.info(f"Power Query analizzata: {query_info['name']}")
                                
                            except Exception as query_err:
                                logger.warning(f"Errore nell'analisi della Power Query {i}: {query_err}")
                                continue
                        
            except Exception as e:
                logger.debug(f"Metodo 1 Power Query non disponibile: {e}")
            
            # Metodo 2: Verifica tramite il modello dati (se disponibile)
            if not queries_found:
                try:
                    # Verifica se esiste un modello dati con query
                    if hasattr(wb_api, 'Model') and wb_api.Model:
                        model = wb_api.Model
                        if hasattr(model, 'DataMashup'):
                            logger.info("Trovato modello dati, ma le query potrebbero non essere accessibili via COM")
                except Exception as model_err:
                    logger.debug(f"Modello dati non accessibile: {model_err}")
            
            # Metodo 3: Cerca nelle connessioni per Power Query
            if not queries_found and len(self.inventory['connections']) > 0:
                pq_connections = [conn for conn in self.inventory['connections'] 
                                if 'Power Query' in str(conn.get('name', '')).upper() or 
                                   'MASHUP' in str(conn.get('type', '')).upper()]
                if pq_connections:
                    logger.info(f"Trovate {len(pq_connections)} connessioni che potrebbero essere Power Query")
            
            if not queries_found and len(self.inventory['queries']) == 0:
                logger.info("Nessuna Power Query trovata nel file o non accessibili tramite COM")
            
        except Exception as e:
            logger.warning(f"Errore generale nell'analisi delle Power Query: {e}")
            logger.info("Le Power Query potrebbero esistere ma non essere accessibili via automazione Excel")
    
    def analyze_query_tables(self):
        """Analizza le Query Tables"""
        try:
            logger.info("Analisi Query Tables...")
            
            for sheet in self.workbook.sheets:
                try:
                    sheet_api = sheet.api
                    
                    for query_table in sheet_api.QueryTables:
                        qt_info = {
                            'name': getattr(query_table, 'Name', f'QueryTable_{len(self.inventory["query_tables"])}'),
                            'worksheet': sheet.name,
                            'destination_range': query_table.Destination.Address,
                            'connection_string': getattr(query_table, 'Connection', ''),
                            'sql': getattr(query_table, 'Sql', ''),
                            'web_tables': getattr(query_table, 'WebTables', ''),
                            'refresh_on_file_open': getattr(query_table, 'RefreshOnFileOpen', False),
                            'refresh_style': getattr(query_table, 'RefreshStyle', 0),
                            'preserve_formatting': getattr(query_table, 'PreserveFormatting', True)
                        }
                        
                        self.inventory['query_tables'].append(qt_info)
                        
                except Exception as e:
                    logger.warning(f"Errore nell'analisi delle Query Tables del foglio {sheet.name}: {e}")
                    
        except Exception as e:
            logger.error(f"Errore generale nell'analisi delle Query Tables: {e}")
    
    def analyze_named_ranges(self):
        """Analizza tutti i nomi definiti (named ranges)"""
        try:
            logger.info("Analisi nomi definiti...")
            
            wb_api = self.workbook.api
            
            for name in wb_api.Names:
                try:
                    name_info = {
                        'name': name.Name,
                        'refers_to': name.RefersTo,
                        'scope': 'Workbook',  # Per default, può essere specifico del foglio
                        'visible': name.Visible,
                        'comment': getattr(name, 'Comment', '')
                    }
                    
                    self.inventory['named_ranges'].append(name_info)
                    
                except Exception as e:
                    logger.warning(f"Errore nell'analisi del nome definito: {e}")
                    
        except Exception as e:
            logger.error(f"Errore nell'analisi dei nomi definiti: {e}")
    
    def analyze_charts(self):
        """Analizza tutti i grafici"""
        try:
            logger.info("Analisi grafici...")
            
            for sheet in self.workbook.sheets:
                try:
                    sheet_api = sheet.api
                    
                    # ChartObjects nel foglio
                    chart_objects_count = sheet_api.ChartObjects().Count
                    logger.debug(f"Trovati {chart_objects_count} grafici nel foglio {sheet.name}")
                    
                    for i in range(1, chart_objects_count + 1):
                        try:
                            chart_obj = sheet_api.ChartObjects(i)
                            chart = chart_obj.Chart
                            
                            chart_info = {
                                'name': chart_obj.Name,
                                'worksheet': sheet.name,
                                'chart_type': 'Unknown',
                                'has_title': False,
                                'title': '',
                                'series_count': 0,
                                'source_data': '',
                                'position': {
                                    'left': chart_obj.Left,
                                    'top': chart_obj.Top,
                                    'width': chart_obj.Width,
                                    'height': chart_obj.Height
                                }
                            }
                            
                            # Informazioni sicure del grafico
                            try:
                                chart_info['chart_type'] = chart.ChartType
                            except:
                                pass
                            
                            try:
                                chart_info['has_title'] = chart.HasTitle
                                if chart.HasTitle:
                                    chart_info['title'] = chart.ChartTitle.Text
                            except:
                                pass
                            
                            try:
                                chart_info['series_count'] = chart.SeriesCollection().Count
                                if chart_info['series_count'] > 0:
                                    chart_info['source_data'] = chart.SeriesCollection(1).Formula
                            except:
                                pass
                            
                            self.inventory['charts'].append(chart_info)
                            logger.info(f"Grafico analizzato: {chart_info['name']} nel foglio {sheet.name}")
                            
                        except Exception as chart_err:
                            logger.warning(f"Errore nell'analisi del grafico {i} nel foglio {sheet.name}: {chart_err}")
                            continue
                        
                except Exception as e:
                    logger.warning(f"Errore nell'analisi dei grafici del foglio {sheet.name}: {e}")
                    
        except Exception as e:
            logger.warning(f"Errore generale nell'analisi dei grafici: {e}")
    
    def analyze_external_data(self):
        """Analizza dati esterni e connessioni alternative"""
        try:
            logger.info("Analisi dati esterni...")
            
            for sheet in self.workbook.sheets:
                try:
                    sheet_api = sheet.api
                    
                    # Verifica presenza di dati esterni tramite altri metodi
                    external_data_info = {
                        'worksheet': sheet.name,
                        'has_external_data': False,
                        'refresh_areas': [],
                        'pivot_caches': []
                    }
                    
                    # Cerca aree di aggiornamento (refresh areas)
                    try:
                        if hasattr(sheet_api, 'Names'):
                            for name in sheet_api.Names:
                                if 'refresh' in name.Name.lower() or 'query' in name.Name.lower():
                                    external_data_info['refresh_areas'].append({
                                        'name': name.Name,
                                        'refers_to': name.RefersTo
                                    })
                                    external_data_info['has_external_data'] = True
                    except:
                        pass
                    
                    if external_data_info['has_external_data'] or external_data_info['refresh_areas']:
                        self.inventory['external_data'].append(external_data_info)
                        
                except Exception as e:
                    logger.debug(f"Errore nell'analisi dati esterni del foglio {sheet.name}: {e}")
            
        except Exception as e:
            logger.debug(f"Errore generale nell'analisi dati esterni: {e}")
    
    def consolidate_database_inventory(self):
        """Crea un inventario consolidato di database, schema e tabelle"""
        try:
            logger.info("Consolidamento inventario database...")
            
            consolidated = {
                'servers': set(),
                'databases': set(),
                'schemas': set(),
                'tables': set(),
                'sources': set(),
                'database_connections': [],
                'query_mappings': []
            }
            
            # Analizza le Power Query
            for query in self.inventory.get('queries', []):
                db_info = query.get('database_info', {})
                
                query_mapping = {
                    'query_name': query.get('name', 'Unknown'),
                    'servers': db_info.get('servers', []),
                    'databases': db_info.get('databases', []),
                    'schemas': db_info.get('schemas', []),
                    'tables': db_info.get('tables', []),
                    'sources': db_info.get('sources', [])
                }
                
                # Aggiungi ai set consolidati
                consolidated['servers'].update(db_info.get('servers', []))
                consolidated['databases'].update(db_info.get('databases', []))
                consolidated['schemas'].update(db_info.get('schemas', []))
                consolidated['tables'].update(db_info.get('tables', []))
                consolidated['sources'].update(db_info.get('sources', []))
                
                if any([query_mapping['servers'], query_mapping['databases'], 
                       query_mapping['schemas'], query_mapping['tables']]):
                    consolidated['query_mappings'].append(query_mapping)
            
            # Analizza le connessioni
            for conn in self.inventory.get('connections', []):
                db_info = conn.get('database_info', {})
                
                if db_info:
                    conn_mapping = {
                        'connection_name': conn.get('name', 'Unknown'),
                        'connection_type': conn.get('type', 'Unknown'),
                        'server': db_info.get('server'),
                        'database': db_info.get('database'),
                        'provider': db_info.get('provider')
                    }
                    
                    if conn_mapping['server']:
                        consolidated['servers'].add(conn_mapping['server'])
                    if conn_mapping['database']:
                        consolidated['databases'].add(conn_mapping['database'])
                    
                    consolidated['database_connections'].append(conn_mapping)
            
            # Converti set in liste ordinate
            self.inventory['database_inventory'] = {
                'summary': {
                    'total_servers': len(consolidated['servers']),
                    'total_databases': len(consolidated['databases']),
                    'total_schemas': len(consolidated['schemas']),
                    'total_tables': len(consolidated['tables']),
                    'total_sources': len(consolidated['sources'])
                },
                'servers': sorted(list(consolidated['servers'])),
                'databases': sorted(list(consolidated['databases'])),
                'schemas': sorted(list(consolidated['schemas'])),
                'tables': sorted(list(consolidated['tables'])),
                'sources': sorted(list(consolidated['sources'])),
                'database_connections': consolidated['database_connections'],
                'query_mappings': consolidated['query_mappings']
            }
            
            logger.info(f"Inventario database consolidato: {len(consolidated['servers'])} server, "
                       f"{len(consolidated['databases'])} database, {len(consolidated['schemas'])} schema, "
                       f"{len(consolidated['tables'])} tabelle")
            
        except Exception as e:
            logger.error(f"Errore nel consolidamento inventario database: {e}")
            self.inventory['database_inventory'] = {}
    
    def run_full_analysis(self) -> Dict[str, Any]:
        """Esegue l'analisi completa del file Excel"""
        logger.info("Inizio analisi completa del file Excel")
        
        try:
            # Analisi delle varie componenti
            self.analyze_file_info()
            self.analyze_worksheets()
            self.analyze_tables()
            self.analyze_pivot_tables()
            self.analyze_connections()
            self.analyze_queries()
            self.analyze_query_tables()
            self.analyze_named_ranges()
            self.analyze_charts()
            self.analyze_external_data()
            
            # Consolidamento inventario database
            self.consolidate_database_inventory()
            
            logger.info("Analisi completa terminata con successo")
            return self.inventory
            
        except Exception as e:
            logger.error(f"Errore durante l'analisi completa: {e}")
            raise
    
    def save_report(self, output_path: str = None, format_type: str = 'json'):
        """
        Salva il report dell'inventario
        
        Args:
            output_path: Percorso del file di output (opzionale)
            format_type: Formato del report ('json' o 'excel')
        """
        if not output_path:
            base_name = self.file_path.stem
            if format_type == 'json':
                output_path = f"{base_name}_inventory.json"
            else:
                output_path = f"{base_name}_inventory.xlsx"
        
        try:
            if format_type == 'json':
                with open(output_path, 'w', encoding='utf-8') as f:
                    json.dump(self.inventory, f, indent=2, ensure_ascii=False, default=str)
                logger.info(f"Report JSON salvato in: {output_path}")
                
            elif format_type == 'excel':
                # Crea un report Excel strutturato
                with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                    # Pulizia dei dati per compatibilità Excel
                    clean_inventory = clean_data_for_excel(self.inventory)
                    
                    # Informazioni generali
                    if clean_inventory['file_info']:
                        pd.DataFrame([clean_inventory['file_info']]).to_excel(
                            writer, sheet_name='File_Info', index=False
                        )
                    
                    # Fogli di lavoro
                    if clean_inventory['worksheets']:
                        worksheets_df = pd.DataFrame(list(clean_inventory['worksheets'].values()))
                        if not worksheets_df.empty:
                            worksheets_df.to_excel(writer, sheet_name='Worksheets', index=False)
                    
                    # Tabelle
                    if clean_inventory['tables']:
                        tables_df = pd.DataFrame(clean_inventory['tables'])
                        if not tables_df.empty:
                            tables_df.to_excel(writer, sheet_name='Tables', index=False)
                    
                    # Tabelle Pivot
                    if clean_inventory['pivot_tables']:
                        pivot_df = pd.DataFrame(clean_inventory['pivot_tables'])
                        if not pivot_df.empty:
                            pivot_df.to_excel(writer, sheet_name='Pivot_Tables', index=False)
                    
                    # Connessioni
                    if clean_inventory['connections']:
                        conn_df = pd.DataFrame(clean_inventory['connections'])
                        if not conn_df.empty:
                            conn_df.to_excel(writer, sheet_name='Connections', index=False)
                    
                    # Query
                    if clean_inventory['queries']:
                        query_df = pd.DataFrame(clean_inventory['queries'])
                        if not query_df.empty:
                            query_df.to_excel(writer, sheet_name='Queries', index=False)
                    
                    # Query Tables
                    if clean_inventory['query_tables']:
                        qt_df = pd.DataFrame(clean_inventory['query_tables'])
                        if not qt_df.empty:
                            qt_df.to_excel(writer, sheet_name='Query_Tables', index=False)
                    
                    # Nomi definiti
                    if clean_inventory['named_ranges']:
                        nr_df = pd.DataFrame(clean_inventory['named_ranges'])
                        if not nr_df.empty:
                            nr_df.to_excel(writer, sheet_name='Named_Ranges', index=False)
                    
                    # Grafici
                    if clean_inventory['charts']:
                        charts_df = pd.DataFrame(clean_inventory['charts'])
                        if not charts_df.empty:
                            charts_df.to_excel(writer, sheet_name='Charts', index=False)
                    
                    # Dati esterni
                    if clean_inventory['external_data']:
                        ext_df = pd.DataFrame(clean_inventory['external_data'])
                        if not ext_df.empty:
                            ext_df.to_excel(writer, sheet_name='External_Data', index=False)
                    
                    # Inventario Database
                    if clean_inventory.get('database_inventory'):
                        db_inv = clean_inventory['database_inventory']
                        
                        # Riepilogo database
                        if db_inv.get('summary'):
                            summary_df = pd.DataFrame([db_inv['summary']])
                            summary_df.to_excel(writer, sheet_name='DB_Summary', index=False)
                        
                        # Lista completa elementi database
                        db_elements = []
                        for server in db_inv.get('servers', []):
                            db_elements.append({'Type': 'Server', 'Name': server})
                        for database in db_inv.get('databases', []):
                            db_elements.append({'Type': 'Database', 'Name': database})
                        for schema in db_inv.get('schemas', []):
                            db_elements.append({'Type': 'Schema', 'Name': schema})
                        for table in db_inv.get('tables', []):
                            db_elements.append({'Type': 'Table', 'Name': table})
                        
                        if db_elements:
                            elements_df = pd.DataFrame(db_elements)
                            elements_df.to_excel(writer, sheet_name='DB_Elements', index=False)
                        
                        # Mappature query-database
                        if db_inv.get('query_mappings'):
                            query_maps = []
                            for qm in db_inv['query_mappings']:
                                for i, server in enumerate(qm.get('servers', [])):
                                    database = qm.get('databases', [])[i] if i < len(qm.get('databases', [])) else ''
                                    schema = qm.get('schemas', [])[i] if i < len(qm.get('schemas', [])) else ''
                                    table = qm.get('tables', [])[i] if i < len(qm.get('tables', [])) else ''
                                    
                                    query_maps.append({
                                        'Query': qm.get('query_name', ''),
                                        'Server': server,
                                        'Database': database,
                                        'Schema': schema,
                                        'Table': table
                                    })
                            
                            if query_maps:
                                qm_df = pd.DataFrame(query_maps)
                                qm_df.to_excel(writer, sheet_name='Query_DB_Mapping', index=False)
                
                logger.info(f"Report Excel salvato in: {output_path}")
                
        except Exception as e:
            logger.error(f"Errore nel salvataggio del report: {e}")
            raise


def main():
    """Funzione principale per eseguire l'analisi.
    Aggiornata per analizzare ricorsivamente una struttura di cartelle e processare tutti i file .xlsx.
    """
    import argparse
    from pathlib import Path

    parser = argparse.ArgumentParser(description="Analizza ricorsivamente file Excel (.xlsx) in una cartella")
    parser.add_argument("root", nargs="?", default=None, help="Cartella radice da analizzare")
    parser.add_argument("--out", dest="out", default=None, help="Cartella di output per i report")
    parser.add_argument("--excel", dest="excel", action="store_true", help="Genera anche report Excel oltre al JSON")
    args = parser.parse_args()

    # Carica configurazione centralizzata da config.py
    config = resolve_paths(CONFIG)

    # Applica priorità: CLI > config.json > default
    root_cfg = args.root if args.root else config.get('root', '.')
    out_cfg = args.out if args.out else config.get('out', 'reports')
    excel_cfg = args.excel or bool(config.get('excel', False))

    root_path = Path(root_cfg).resolve()
    out_dir = Path(out_cfg).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    if not root_path.exists() or not root_path.is_dir():
        logger.error(f"Cartella radice non valida: {root_path}")
        return

    logger.info(f"Ricerca dei file .xlsx in: {root_path}")

    excel_files = [p for p in root_path.rglob("*.xlsx") if p.is_file()]
    total_files = len(excel_files)
    logger.info(f"Trovati {total_files} file .xlsx da processare")

    summary = {
        'root': str(root_path),
        'processed_count': 0,
        'errors_count': 0,
        'files': [],
    }

    for idx, file_path in enumerate(excel_files, start=1):
        logger.info(f"[{idx}/{total_files}] Analisi del file: {file_path}")
        try:
            with ExcelAnalyzer(str(file_path)) as analyzer:
                inventory = analyzer.run_full_analysis()

                # Salva i report per-file nella cartella di output
                json_out = out_dir / f"{file_path.stem}_inventory.json"
                analyzer.save_report(output_path=str(json_out), format_type='json')
                if excel_cfg:
                    xlsx_out = out_dir / f"{file_path.stem}_inventory.xlsx"
                    analyzer.save_report(output_path=str(xlsx_out), format_type='excel')

                summary['processed_count'] += 1
                summary['files'].append({
                    'file': str(file_path),
                    'json_report': str(json_out),
                    'excel_report': str(xlsx_out) if excel_cfg else None,
                    'worksheets': len(inventory.get('worksheets', {})),
                    'tables': len(inventory.get('tables', [])),
                    'pivot_tables': len(inventory.get('pivot_tables', [])),
                    'connections': len(inventory.get('connections', [])),
                    'queries': len(inventory.get('queries', []))
                })

        except Exception as e:
            logger.error(f"Errore durante l'analisi di {file_path}: {e}")
            summary['errors_count'] += 1
            summary['files'].append({
                'file': str(file_path),
                'error': str(e)
            })

    # Salva un riepilogo complessivo
    summary_path = out_dir / "summary.json"
    try:
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        logger.info(f"Riepilogo complessivo salvato: {summary_path}")
    except Exception as e:
        logger.warning(f"Impossibile salvare il riepilogo complessivo: {e}")


if __name__ == "__main__":
    main()