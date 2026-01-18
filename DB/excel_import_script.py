import sqlite3
import pandas as pd
import os
import re
import sys
from datetime import datetime
from pathlib import Path

# Füge Root-Verzeichnis zum Python-Pfad hinzu
SCRIPT_DIR = Path(__file__).parent.resolve()
ROOT_DIR = SCRIPT_DIR.parent
sys.path.insert(0, str(ROOT_DIR))

# Importiere zentrale Konfiguration
from config import Config


class PublicationImporter:
    """Klasse für den Import von Publication Excel-Dateien"""
    
    def __init__(self, db_path=None):
        """
        Initialisiert den Importer.
        
        Args:
            db_path (str): Pfad zur SQLite Datenbank (optional, nutzt Config)
        """
        self.db_path = db_path or str(Config.DB_PATH)
        self.conn = None
        self.cursor = None
        self.stats = {
            'files_processed': 0,
            'products_added': 0,
            'products_updated': 0,
            'prices_added': 0,
            'errors': []
        }
    
    def connect(self):
        """Stellt Verbindung zur Datenbank her."""
        if not os.path.exists(self.db_path):
            raise FileNotFoundError(
                f"Datenbank '{self.db_path}' nicht gefunden!\\n"
                f"Bitte zuerst create_database.py ausführen.\\n"
                f"Konfigurierter Pfad: {Config.DB_PATH}"
            )
        
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        print(f"✅ Verbindung zu '{self.db_path}' hergestellt")
    
    def close(self):
        """Schließt die Datenbankverbindung."""
        if self.conn:
            self.conn.close()
            print("✅ Datenbankverbindung geschlossen")
    
    def extract_date_from_filename(self, filename):
        """Extrahiert das Datum aus dem Dateinamen."""
        match = re.search(r'Publications-(\d{8})', filename)
        if match:
            date_str = match.group(1)
            try:
                date_obj = datetime.strptime(date_str, '%Y%m%d')
                return date_obj.strftime('%Y-%m-%d')
            except ValueError:
                return None
        return None
    
    def detect_column_mapping(self, df):
        """Erkennt automatisch die Spalten-Zuordnung."""
        columns = df.columns.tolist()
        mapping = {
            'product_number': None,
            'description': None,
            'category': None,
            'unit': None,
            'price': None
        }
        
        for col in columns:
            col_lower = col.lower()
            
            if any(term in col_lower for term in ['nummer', 'number', 'nr', 'artikel', 'item', 'sku']):
                if not mapping['product_number']:
                    mapping['product_number'] = col
            
            elif any(term in col_lower for term in ['beschreibung', 'description', 'name', 'bezeichnung']):
                if not mapping['description']:
                    mapping['description'] = col
            
            elif any(term in col_lower for term in ['kategorie', 'category', 'gruppe', 'group']):
                if not mapping['category']:
                    mapping['category'] = col
            
            elif any(term in col_lower for term in ['einheit', 'unit', 'me', 'uom']):
                if not mapping['unit']:
                    mapping['unit'] = col
            
            elif any(term in col_lower for term in ['preis', 'price', 'betrag', 'amount']):
                if not mapping['price']:
                    mapping['price'] = col
        
        return mapping
    
    def get_or_create_product(self, product_data):
        """Holt vorhandenes Produkt oder erstellt ein neues."""
        product_number = product_data.get('product_number')
        
        if not product_number:
            raise ValueError("Produktnummer fehlt!")
        
        self.cursor.execute(
            "SELECT id FROM products WHERE product_number = ?",
            (product_number,)
        )
        result = self.cursor.fetchone()
        
        if result:
            product_id = result[0]
            
            self.cursor.execute("""
                UPDATE products 
                SET description = ?, category = ?, unit = ?
                WHERE id = ?
            """, (
                product_data.get('description'),
                product_data.get('category'),
                product_data.get('unit'),
                product_id
            ))
            
            self.stats['products_updated'] += 1
            return product_id
        else:
            self.cursor.execute("""
                INSERT INTO products (product_number, description, category, unit)
                VALUES (?, ?, ?, ?)
            """, (
                product_number,
                product_data.get('description'),
                product_data.get('category'),
                product_data.get('unit')
            ))
            
            self.stats['products_added'] += 1
            return self.cursor.lastrowid
    
    def add_price(self, product_id, price, valid_from, source_file):
        """Fügt einen Preis hinzu."""
        if price is None or pd.isna(price):
            return
        
        try:
            price_value = float(price)
        except (ValueError, TypeError):
            self.stats['errors'].append(
                f"Ungültiger Preis für Produkt {product_id}: {price}"
            )
            return
        
        self.cursor.execute("""
            UPDATE prices 
            SET is_current = 0, valid_until = ?
            WHERE product_id = ? AND is_current = 1
        """, (valid_from, product_id))
        
        self.cursor.execute("""
            INSERT INTO prices (product_id, price, valid_from, source_file, is_current)
            VALUES (?, ?, ?, ?, 1)
        """, (product_id, price_value, valid_from, source_file))
        
        self.stats['prices_added'] += 1
    
    def import_excel_file(self, filepath, sheet_name=None):
        """Importiert eine einzelne Excel-Datei."""
        if sheet_name is None:
            sheet_name = Config.EXCEL_SHEET_NAME
        
        filename = os.path.basename(filepath)
        print(f"\\n📄 Importiere: {filename}")
        
        try:
            valid_from = self.extract_date_from_filename(filename)
            if not valid_from:
                print(f"  ⚠️  Warnung: Konnte kein Datum aus '{filename}' extrahieren")
                valid_from = datetime.now().strftime('%Y-%m-%d')
            
            print(f"  📅 Gültigkeitsdatum: {valid_from}")
            
            try:
                df = pd.read_excel(filepath, sheet_name=sheet_name)
            except ValueError:
                df = pd.read_excel(filepath)
                print(f"  ℹ️  Sheet '{sheet_name}' nicht gefunden, verwende erstes Sheet")
            
            if df.empty:
                print(f"  ⚠️  Datei ist leer, überspringe...")
                return False
            
            print(f"  📊 Zeilen: {len(df)}")
            
            mapping = self.detect_column_mapping(df)
            print(f"  🔍 Erkannte Spalten:")
            for key, value in mapping.items():
                if value:
                    print(f"     • {key}: '{value}'")
            
            if not mapping['product_number']:
                raise ValueError("Produktnummer-Spalte nicht gefunden!")
            
            if not mapping['price']:
                raise ValueError("Preis-Spalte nicht gefunden!")
            
            imported_count = 0
            skipped_count = 0
            
            for idx, row in df.iterrows():
                try:
                    product_data = {
                        'product_number': str(row[mapping['product_number']]) if pd.notna(row[mapping['product_number']]) else None,
                        'description': str(row[mapping['description']]) if mapping['description'] and pd.notna(row[mapping['description']]) else None,
                        'category': str(row[mapping['category']]) if mapping['category'] and pd.notna(row[mapping['category']]) else None,
                        'unit': str(row[mapping['unit']]) if mapping['unit'] and pd.notna(row[mapping['unit']]) else None
                    }
                    
                    if not product_data['product_number'] or product_data['product_number'] == 'nan':
                        skipped_count += 1
                        continue
                    
                    product_id = self.get_or_create_product(product_data)
                    price = row[mapping['price']]
                    self.add_price(product_id, price, valid_from, filename)
                    
                    imported_count += 1
                    
                except Exception as e:
                    error_msg = f"Zeile {idx + 2}: {str(e)}"
                    self.stats['errors'].append(error_msg)
                    print(f"  ❌ Fehler in Zeile {idx + 2}: {e}")
            
            self.conn.commit()
            
            print(f"  ✅ Import abgeschlossen:")
            print(f"     • Erfolgreich: {imported_count} Zeilen")
            if skipped_count > 0:
                print(f"     • Übersprungen: {skipped_count} Zeilen")
            
            self.stats['files_processed'] += 1
            return True
            
        except Exception as e:
            self.conn.rollback()
            error_msg = f"Fehler bei {filename}: {str(e)}"
            self.stats['errors'].append(error_msg)
            print(f"  ❌ FEHLER: {e}")
            return False
    
    def import_directory(self, directory_path=None, pattern=None):
        """Importiert alle Excel-Dateien aus einem Verzeichnis."""
        if directory_path is None:
            directory_path = str(Config.TESTDATA_PATH)
        
        if pattern is None:
            pattern = Config.EXCEL_FILE_PATTERN
        
        print(f"\\n🔍 Suche nach Dateien in: {directory_path}")
        print(f"   Pattern: {pattern}")
        
        path = Path(directory_path)
        files = sorted(path.glob(pattern))
        
        if not files:
            print(f"⚠️  Keine passenden Dateien gefunden!")
            return
        
        print(f"\\n📚 Gefundene Dateien: {len(files)}")
        for f in files:
            print(f"   • {f.name}")
        
        print("\\n" + "="*70)
        print("🚀 STARTE IMPORT")
        print("="*70)
        
        for filepath in files:
            self.import_excel_file(str(filepath))
        
        self.print_summary()
    
    def print_summary(self):
        """Zeigt eine Zusammenfassung des Imports."""
        print("\\n" + "="*70)
        print("📊 IMPORT ZUSAMMENFASSUNG")
        print("="*70)
        print(f"Verarbeitete Dateien: {self.stats['files_processed']}")
        print(f"Neue Produkte: {self.stats['products_added']}")
        print(f"Aktualisierte Produkte: {self.stats['products_updated']}")
        print(f"Hinzugefügte Preise: {self.stats['prices_added']}")
        
        if self.stats['errors']:
            print(f"\\n⚠️  Fehler: {len(self.stats['errors'])}")
            for error in self.stats['errors'][:10]:
                print(f"   • {error}")
            if len(self.stats['errors']) > 10:
                print(f"   ... und {len(self.stats['errors']) - 10} weitere Fehler")
        else:
            print("\\n✅ Keine Fehler aufgetreten!")
        
        print("="*70 + "\\n")


def main():
    """Hauptfunktion für den Import."""
    
    print("\\n" + "="*70)
    print("📥 EXCEL IMPORT für Publications Datenbank")
    print("="*70 + "\\n")
    
    # Zeige Konfiguration
    Config.print_config()
    
    # Validiere Konfiguration
    if not Config.validate():
        print("\\n⚠️  Bitte Konfiguration prüfen (.env Datei)\\n")
    
    # Importer initialisieren
    importer = PublicationImporter()
    
    try:
        importer.connect()
        
        print("\\nWählen Sie eine Option:")
        print("  1. Einzelne Datei importieren")
        print("  2. Alle Dateien aus Verzeichnis importieren")
        print("  3. Standard Testdaten-Verzeichnis verwenden (aus .env)")
        print(f"     ({Config.TESTDATA_PATH})")
        
        choice = input("\\nIhre Wahl (1-3): ").strip()
        
        if choice == '1':
            filepath = input("Pfad zur Excel-Datei: ").strip()
            if os.path.exists(filepath):
                importer.import_excel_file(filepath)
                importer.print_summary()
            else:
                print(f"❌ Datei nicht gefunden: {filepath}")
        
        elif choice == '2':
            directory = input("Pfad zum Verzeichnis: ").strip()
            if os.path.isdir(directory):
                importer.import_directory(directory)
            else:
                print(f"❌ Verzeichnis nicht gefunden: {directory}")
        
        elif choice == '3':
            if Config.TESTDATA_PATH.exists():
                print(f"\\n✅ Verwende konfiguriertes Verzeichnis")
                importer.import_directory()
            else:
                print(f"❌ Verzeichnis nicht gefunden: {Config.TESTDATA_PATH}")
                print("Bitte .env Datei prüfen oder Option 2 wählen.")
        
        else:
            print("❌ Ungültige Wahl!")
    
    except Exception as e:
        print(f"\\n❌ KRITISCHER FEHLER: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        importer.close()


if __name__ == "__main__":
    main()
