#!/usr/bin/env python3
"""
Datenbank Diagnose & Fix Script
Prüft die Datenbank und erstellt sie bei Bedarf neu
"""

import sqlite3
import sys
import os
from pathlib import Path

# Importiere Config falls vorhanden
try:
    ROOT_DIR = Path(__file__).parent.resolve()
    sys.path.insert(0, str(ROOT_DIR))
    from config import Config
    USE_CONFIG = True
except ImportError:
    USE_CONFIG = False
    print("⚠️  config.py nicht gefunden, verwende manuelle Pfade")


def get_db_path():
    """Ermittelt den Datenbankpfad."""
    if USE_CONFIG:
        return str(Config.DB_PATH)
    
    # Manuelle Suche
    possible_paths = [
        'publications.db',
        'mediprice_app/publications.db',
        '../mediprice_app/publications.db',
        Path(__file__).parent / 'mediprice_app' / 'publications.db'
    ]
    
    for path in possible_paths:
        p = Path(path)
        if p.exists():
            return str(p.resolve())
    
    # Fallback
    return str(Path(__file__).parent / 'mediprice_app' / 'publications.db')


def check_database(db_path):
    """
    Prüft die Datenbank auf Vollständigkeit.
    
    Returns:
        dict: Diagnose-Ergebnisse
    """
    print(f"\n🔍 Prüfe Datenbank: {db_path}\n")
    
    result = {
        'exists': False,
        'has_tables': False,
        'tables': [],
        'product_count': 0,
        'price_count': 0,
        'errors': []
    }
    
    # Prüfe ob Datei existiert
    if not os.path.exists(db_path):
        result['errors'].append(f"Datei existiert nicht: {db_path}")
        return result
    
    result['exists'] = True
    file_size = os.path.getsize(db_path)
    print(f"✅ Datei existiert ({file_size} Bytes)")
    
    # Prüfe Datenbank-Inhalt
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Liste alle Tabellen
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' 
            ORDER BY name
        """)
        tables = [row[0] for row in cursor.fetchall()]
        result['tables'] = tables
        
        if tables:
            result['has_tables'] = True
            print(f"✅ Gefundene Tabellen: {', '.join(tables)}")
        else:
            print("❌ Keine Tabellen gefunden!")
            result['errors'].append("Datenbank enthält keine Tabellen")
        
        # Prüfe erwartete Tabellen
        expected_tables = ['products', 'prices']
        missing_tables = [t for t in expected_tables if t not in tables]
        
        if missing_tables:
            print(f"❌ Fehlende Tabellen: {', '.join(missing_tables)}")
            result['errors'].append(f"Fehlende Tabellen: {', '.join(missing_tables)}")
        
        # Zähle Einträge wenn Tabellen existieren
        if 'products' in tables:
            cursor.execute("SELECT COUNT(*) FROM products")
            result['product_count'] = cursor.fetchone()[0]
            print(f"📊 Produkte: {result['product_count']}")
        
        if 'prices' in tables:
            cursor.execute("SELECT COUNT(*) FROM prices")
            result['price_count'] = cursor.fetchone()[0]
            print(f"📊 Preise: {result['price_count']}")
        
        conn.close()
        
    except sqlite3.Error as e:
        result['errors'].append(f"Datenbankfehler: {str(e)}")
        print(f"❌ Datenbankfehler: {e}")
    
    return result


def create_tables(db_path):
    """
    Erstellt die notwendigen Tabellen.
    
    Args:
        db_path (str): Pfad zur Datenbank
    """
    print(f"\n🔧 Erstelle Tabellen in: {db_path}\n")
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Tabelle: products
        print("  📋 Erstelle Tabelle: products")
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_number TEXT UNIQUE NOT NULL,
            description TEXT,
            category TEXT,
            unit TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_products_number ON products(product_number)
        ''')
        
        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_products_category ON products(category)
        ''')
        
        # Tabelle: prices
        print("  📋 Erstelle Tabelle: prices")
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER NOT NULL,
            price REAL NOT NULL,
            valid_from DATE NOT NULL,
            valid_until DATE,
            source_file TEXT,
            is_current BOOLEAN DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE
        )
        ''')
        
        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_prices_product_id ON prices(product_id)
        ''')
        
        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_prices_current ON prices(is_current)
        ''')
        
        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_prices_valid_from ON prices(valid_from)
        ''')
        
        # Trigger
        print("  ⚙️  Erstelle Trigger")
        cursor.execute('''
        CREATE TRIGGER IF NOT EXISTS update_products_timestamp 
        AFTER UPDATE ON products
        BEGIN
            UPDATE products 
            SET updated_at = CURRENT_TIMESTAMP 
            WHERE id = NEW.id;
        END
        ''')
        
        conn.commit()
        conn.close()
        
        print("\n✅ Tabellen erfolgreich erstellt!")
        return True
        
    except sqlite3.Error as e:
        print(f"\n❌ Fehler beim Erstellen der Tabellen: {e}")
        return False


def backup_database(db_path):
    """
    Erstellt ein Backup der Datenbank.
    
    Args:
        db_path (str): Pfad zur Datenbank
    """
    if not os.path.exists(db_path):
        return None
    
    from datetime import datetime
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_path = f"{db_path}.backup_{timestamp}"
    
    try:
        import shutil
        shutil.copy2(db_path, backup_path)
        print(f"✅ Backup erstellt: {backup_path}")
        return backup_path
    except Exception as e:
        print(f"⚠️  Backup fehlgeschlagen: {e}")
        return None


def main():
    """Hauptfunktion."""
    
    print("\n" + "="*70)
    print("🔧 DATENBANK DIAGNOSE & FIX")
    print("="*70)
    
    # Datenbankpfad ermitteln
    db_path = get_db_path()
    
    if USE_CONFIG:
        print(f"\n📍 Verwende Config-Pfad")
        Config.print_config()
    else:
        print(f"\n📍 Datenbankpfad: {db_path}")
    
    # Diagnose durchführen
    result = check_database(db_path)
    
    # Zusammenfassung
    print("\n" + "="*70)
    print("📊 DIAGNOSE-ZUSAMMENFASSUNG")
    print("="*70)
    print(f"Datenbank existiert:  {'✅ Ja' if result['exists'] else '❌ Nein'}")
    print(f"Tabellen vorhanden:   {'✅ Ja' if result['has_tables'] else '❌ Nein'}")
    print(f"Anzahl Tabellen:      {len(result['tables'])}")
    print(f"Produkte:             {result['product_count']}")
    print(f"Preise:               {result['price_count']}")
    
    if result['errors']:
        print(f"\n⚠️  Probleme gefunden: {len(result['errors'])}")
        for error in result['errors']:
            print(f"   • {error}")
    else:
        print(f"\n✅ Keine Probleme gefunden!")
    
    print("="*70)
    
    # Reparatur-Optionen anbieten
    if result['errors']:
        print("\n🔧 REPARATUR-OPTIONEN:")
        print("  1. Tabellen erstellen (ohne Daten zu löschen)")
        print("  2. Datenbank komplett neu erstellen (löscht alle Daten!)")
        print("  3. Nichts tun (nur Diagnose)")
        
        choice = input("\nIhre Wahl (1-3): ").strip()
        
        if choice == '1':
            if result['exists']:
                backup_database(db_path)
            create_tables(db_path)
            print("\n💡 Nächster Schritt: Daten importieren mit excel_import_script.py")
        
        elif choice == '2':
            confirm = input("\n⚠️  WARNUNG: Alle Daten gehen verloren! Fortfahren? (ja/nein): ")
            if confirm.lower() in ['ja', 'j', 'yes', 'y']:
                if result['exists']:
                    backup_database(db_path)
                    os.remove(db_path)
                    print(f"🗑️  Alte Datenbank gelöscht")
                
                # Stelle sicher dass Verzeichnis existiert
                Path(db_path).parent.mkdir(parents=True, exist_ok=True)
                
                create_tables(db_path)
                print("\n💡 Nächster Schritt: Daten importieren mit excel_import_script.py")
            else:
                print("❌ Abgebrochen")
        
        elif choice == '3':
            print("\n👍 OK, keine Änderungen vorgenommen")
        
        else:
            print("\n❌ Ungültige Wahl")
    
    else:
        print("\n✅ Datenbank ist in Ordnung, keine Reparatur nötig!")
    
    print()


if __name__ == "__main__":
    main()