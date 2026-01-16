#!/usr/bin/env python3
"""
SQLite Datenbank Erstellungs-Script für Publications
Erstellt die Datenbank mit zwei Tabellen: products und prices
"""

import sqlite3
import os
from datetime import datetime


def create_database(db_path='publications.db'):
    """
    Erstellt die SQLite Datenbank mit den Tabellen products und prices.
    
    Args:
        db_path (str): Pfad zur Datenbankdatei
    
    Returns:
        bool: True wenn erfolgreich, False bei Fehler
    """
    
    # Prüfen ob Datenbank bereits existiert
    db_exists = os.path.exists(db_path)
    
    if db_exists:
        print(f"⚠️  WARNUNG: Datenbank '{db_path}' existiert bereits!")
        response = input("Möchten Sie sie überschreiben? (ja/nein): ")
        if response.lower() not in ['ja', 'j', 'yes', 'y']:
            print("❌ Abbruch: Datenbank wurde nicht überschrieben.")
            return False
        os.remove(db_path)
        print(f"🗑️  Alte Datenbank gelöscht.")
    
    try:
        # Verbindung zur Datenbank herstellen
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        print(f"\n📊 Erstelle Datenbank: {db_path}")
        
        # =====================================================================
        # Tabelle: products
        # =====================================================================
        print("  ├─ Erstelle Tabelle: products")
        
        cursor.execute('''
        CREATE TABLE products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_number TEXT UNIQUE NOT NULL,
            description TEXT,
            category TEXT,
            unit TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # Index für schnellere Suche nach Produktnummer
        cursor.execute('''
        CREATE INDEX idx_products_number ON products(product_number)
        ''')
        
        # Index für Kategorie-Suchen
        cursor.execute('''
        CREATE INDEX idx_products_category ON products(category)
        ''')
        
        print("  │  ✓ Tabelle 'products' erstellt")
        print("  │  ✓ Indizes erstellt")
        
        # =====================================================================
        # Tabelle: prices
        # =====================================================================
        print("  ├─ Erstelle Tabelle: prices")
        
        cursor.execute('''
        CREATE TABLE prices (
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
        
        # Indizes für Performance
        cursor.execute('''
        CREATE INDEX idx_prices_product_id ON prices(product_id)
        ''')
        
        cursor.execute('''
        CREATE INDEX idx_prices_current ON prices(is_current)
        ''')
        
        cursor.execute('''
        CREATE INDEX idx_prices_valid_from ON prices(valid_from)
        ''')
        
        print("  │  ✓ Tabelle 'prices' erstellt")
        print("  │  ✓ Indizes erstellt")
        
        # =====================================================================
        # Trigger für updated_at Timestamp
        # =====================================================================
        print("  ├─ Erstelle Trigger")
        
        cursor.execute('''
        CREATE TRIGGER update_products_timestamp 
        AFTER UPDATE ON products
        BEGIN
            UPDATE products 
            SET updated_at = CURRENT_TIMESTAMP 
            WHERE id = NEW.id;
        END
        ''')
        
        print("  │  ✓ Trigger 'update_products_timestamp' erstellt")
        
        # =====================================================================
        # Commit und Abschluss
        # =====================================================================
        conn.commit()
        
        # Statistik anzeigen
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='index'")
        indexes = cursor.fetchall()
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='trigger'")
        triggers = cursor.fetchall()
        
        print("  └─ Datenbank erfolgreich erstellt!\n")
        print("📈 Statistik:")
        print(f"  • Tabellen: {len(tables)} ({', '.join([t[0] for t in tables])})")
        print(f"  • Indizes: {len(indexes)}")
        print(f"  • Trigger: {len(triggers)}")
        
        # Datenbankgröße anzeigen
        db_size = os.path.getsize(db_path)
        print(f"  • Dateigröße: {db_size} Bytes")
        
        conn.close()
        print(f"\n✅ Datenbank '{db_path}' wurde erfolgreich erstellt!")
        return True
        
    except sqlite3.Error as e:
        print(f"\n❌ FEHLER beim Erstellen der Datenbank: {e}")
        return False
    
    except Exception as e:
        print(f"\n❌ UNERWARTETER FEHLER: {e}")
        return False


def verify_database(db_path='publications.db'):
    """
    Überprüft die erstellte Datenbank.
    
    Args:
        db_path (str): Pfad zur Datenbankdatei
    """
    
    if not os.path.exists(db_path):
        print(f"❌ Datenbank '{db_path}' existiert nicht!")
        return
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        print(f"\n🔍 Verifiziere Datenbank: {db_path}\n")
        
        # Tabellen-Struktur anzeigen
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        for table in tables:
            table_name = table[0]
            print(f"📋 Tabelle: {table_name}")
            
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            
            for col in columns:
                col_id, col_name, col_type, not_null, default, pk = col
                pk_marker = " [PRIMARY KEY]" if pk else ""
                not_null_marker = " NOT NULL" if not_null else ""
                default_marker = f" DEFAULT {default}" if default else ""
                print(f"  • {col_name}: {col_type}{pk_marker}{not_null_marker}{default_marker}")
            
            print()
        
        conn.close()
        print("✅ Verifizierung abgeschlossen!")
        
    except sqlite3.Error as e:
        print(f"❌ FEHLER bei der Verifizierung: {e}")


def show_usage_example():
    """
    Zeigt Beispiel-Code für die Verwendung der Datenbank.
    """
    
    print("\n" + "="*70)
    print("📝 VERWENDUNGSBEISPIEL")
    print("="*70)
    
    example_code = '''
import sqlite3

# Verbindung zur Datenbank
conn = sqlite3.connect('publications.db')
cursor = conn.cursor()

# Produkt hinzufügen
cursor.execute("""
    INSERT INTO products (product_number, description, category, unit)
    VALUES (?, ?, ?, ?)
""", ("ART-001", "Beispiel Produkt", "Kategorie A", "Stück"))

product_id = cursor.lastrowid

# Preis hinzufügen
cursor.execute("""
    INSERT INTO prices (product_id, price, valid_from, source_file)
    VALUES (?, ?, ?, ?)
""", (product_id, 19.99, "2025-01-01", "Publications20250101.xlsx"))

conn.commit()

# Aktuellen Preis abfragen
cursor.execute("""
    SELECT p.product_number, p.description, pr.price, pr.valid_from
    FROM products p
    JOIN prices pr ON p.id = pr.product_id
    WHERE pr.is_current = 1
    ORDER BY p.product_number
""")

for row in cursor.fetchall():
    print(f"Produkt: {row[0]}, Preis: {row[2]} CHF")

conn.close()
'''
    
    print(example_code)
    print("="*70 + "\n")


if __name__ == "__main__":
    print("\n" + "="*70)
    print("🗄️  SQLite Datenbank Erstellung für Publications")
    print("="*70 + "\n")
    
    # Datenbank erstellen
    success = create_database('publications.db')
    
    if success:
        # Datenbank verifizieren
        verify_database('publications.db')
        
        # Verwendungsbeispiel anzeigen
        show_usage_example()
        
        print("\n💡 Nächste Schritte:")
        print("  1. Importiere deine Excel-Dateien mit einem Import-Script")
        print("  2. Verwende die Datenbank in deiner Flask-Applikation")
        print("  3. Erstelle Abfragen für Reports und Analysen\n")
    else:
        print("\n⚠️  Datenbank konnte nicht erstellt werden.")
        print("Bitte überprüfe die Fehlermeldung und versuche es erneut.\n")