# =============================================================================
# app.py - Haupt-Applikation
# =============================================================================

from flask import Flask, render_template, request, jsonify, url_for
import sqlite3
import matplotlib
matplotlib.use('Agg')  # Für Server-Nutzung ohne Display
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
import os
import sys
from pathlib import Path

# Bestimme Root-Verzeichnis absolut
SCRIPT_DIR = Path(__file__).parent.resolve()
ROOT_DIR = SCRIPT_DIR.parent

# Füge Root zum Python-Pfad hinzu
sys.path.insert(0, str(ROOT_DIR))

# Versuche Config zu importieren
try:
    from config import Config
    USE_CONFIG = True
    print(f"✅ Config geladen aus: {ROOT_DIR / 'config.py'}")
except ImportError as e:
    USE_CONFIG = False
    print(f"⚠️  Config nicht gefunden: {e}")
    print(f"   Gesucht in: {ROOT_DIR}")
    print(f"   Verwende Fallback-Pfade")

app = Flask(__name__)

# Lade Konfiguration
if USE_CONFIG:
    app.config['DATABASE'] = str(Config.DB_PATH)
    app.config['PLOT_FOLDER'] = str(Config.FLASK_PLOT_FOLDER)
    app.config['SECRET_KEY'] = Config.FLASK_SECRET_KEY
    Config.validate()
else:
    # Fallback: Absolute Pfade
    app.config['DATABASE'] = str(SCRIPT_DIR / 'publications.db')
    app.config['PLOT_FOLDER'] = str(SCRIPT_DIR / 'static' / 'plots')
    app.config['SECRET_KEY'] = 'dev-secret-key-change-in-production'
    
    # Erstelle Plot-Ordner
    os.makedirs(app.config['PLOT_FOLDER'], exist_ok=True)

# Debug: Zeige welche DB verwendet wird
print(f"\n{'='*70}")
print("🗄️  DATENBANK-KONFIGURATION")
print(f"{'='*70}")
print(f"Datenbank-Pfad:  {app.config['DATABASE']}")
print(f"DB existiert:    {'✅ Ja' if os.path.exists(app.config['DATABASE']) else '❌ Nein'}")
if os.path.exists(app.config['DATABASE']):
    db_size = os.path.getsize(app.config['DATABASE'])
    print(f"DB Größe:        {db_size:,} Bytes")
print(f"Plot-Ordner:     {app.config['PLOT_FOLDER']}")
print(f"{'='*70}\n")


def get_db_connection():
    """Erstellt Datenbankverbindung."""
    conn = sqlite3.connect(app.config['DATABASE'])
    conn.row_factory = sqlite3.Row
    return conn


def search_products(query):
    """
    Sucht Produkte basierend auf Suchbegriff.
    
    Args:
        query (str): Suchbegriff
    
    Returns:
        list: Gefundene Produkte
    """
    conn = get_db_connection()
    
    # Suche in Produktnummer und Beschreibung
    products = conn.execute('''
        SELECT DISTINCT p.*, 
               pr.price as current_price,
               pr.valid_from as current_valid_from
        FROM products p
        LEFT JOIN prices pr ON p.id = pr.product_id AND pr.is_current = 1
        WHERE p.product_number LIKE ? 
           OR p.description LIKE ?
        ORDER BY p.product_number
        LIMIT 50
    ''', (f'%{query}%', f'%{query}%')).fetchall()
    
    conn.close()
    return products


def get_product_details(product_id):
    """
    Holt Details eines Produkts.
    
    Args:
        product_id (int): Produkt-ID
    
    Returns:
        dict: Produktdetails
    """
    conn = get_db_connection()
    
    product = conn.execute('''
        SELECT * FROM products WHERE id = ?
    ''', (product_id,)).fetchone()
    
    conn.close()
    return product


def get_price_history(product_id):
    """
    Holt Preisverlauf eines Produkts.
    
    Args:
        product_id (int): Produkt-ID
    
    Returns:
        list: Preisverlauf
    """
    conn = get_db_connection()
    
    prices = conn.execute('''
        SELECT price, valid_from, source_file
        FROM prices
        WHERE product_id = ?
        ORDER BY valid_from ASC
    ''', (product_id,)).fetchall()
    
    conn.close()
    return prices


def create_price_chart(product_id, product_name):
    """
    Erstellt Preisdiagramm für ein Produkt.
    
    Args:
        product_id (int): Produkt-ID
        product_name (str): Produktname für Titel
    
    Returns:
        str: Pfad zum generierten Diagramm
    """
    prices = get_price_history(product_id)
    
    if not prices:
        return None
    
    # Daten vorbereiten
    dates = []
    price_values = []
    
    for price in prices:
        date_obj = datetime.strptime(price['valid_from'], '%Y-%m-%d')
        dates.append(date_obj)
        price_values.append(float(price['price']))
    
    # Diagramm erstellen
    plt.figure(figsize=(12, 6))
    plt.plot(dates, price_values, marker='o', linewidth=2, markersize=8)
    
    # Styling
    plt.title(f'Preisverlauf: {product_name}', fontsize=16, fontweight='bold')
    plt.xlabel('Datum', fontsize=12)
    plt.ylabel('Preis (CHF)', fontsize=12)
    plt.grid(True, alpha=0.3)
    
    # Datumsformatierung
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
    plt.gca().xaxis.set_major_locator(mdates.MonthLocator(interval=1))
    plt.gcf().autofmt_xdate()
    
    # Preise an Datenpunkten anzeigen
    for date, price in zip(dates, price_values):
        plt.annotate(f'{price:.2f}', 
                    xy=(date, price), 
                    xytext=(0, 10),
                    textcoords='offset points',
                    ha='center',
                    fontsize=9,
                    bbox=dict(boxstyle='round,pad=0.3', facecolor='yellow', alpha=0.7))
    
    plt.tight_layout()
    
    # Speichern
    filename = f'price_history_{product_id}.png'
    filepath = os.path.join(app.config['PLOT_FOLDER'], filename)
    plt.savefig(filepath, dpi=100, bbox_inches='tight')
    plt.close()
    
    return filename


def calculate_price_statistics(prices):
    """
    Berechnet Statistiken zum Preisverlauf.
    
    Args:
        prices (list): Preisverlauf
    
    Returns:
        dict: Statistiken
    """
    if not prices:
        return None
    
    price_values = [float(p['price']) for p in prices]
    
    min_price = min(price_values)
    max_price = max(price_values)
    avg_price = sum(price_values) / len(price_values)
    current_price = price_values[-1] if price_values else 0
    
    # Preisänderung berechnen
    if len(price_values) > 1:
        first_price = price_values[0]
        price_change = current_price - first_price
        price_change_percent = (price_change / first_price * 100) if first_price > 0 else 0
    else:
        price_change = 0
        price_change_percent = 0
    
    return {
        'min': min_price,
        'max': max_price,
        'avg': avg_price,
        'current': current_price,
        'change': price_change,
        'change_percent': price_change_percent,
        'count': len(price_values)
    }


# =============================================================================
# Routes
# =============================================================================

@app.route('/')
def index():
    """Startseite mit Suchfeld."""
    # Statistiken für Dashboard
    conn = get_db_connection()
    
    product_count = conn.execute('SELECT COUNT(*) as count FROM products').fetchone()['count']
    price_count = conn.execute('SELECT COUNT(*) as count FROM prices').fetchone()['count']
    
    # Neueste Preise
    latest_date = conn.execute('''
        SELECT MAX(valid_from) as latest FROM prices
    ''').fetchone()['latest']
    
    conn.close()
    
    return render_template('index.html', 
                         product_count=product_count,
                         price_count=price_count,
                         latest_date=latest_date)


@app.route('/search')
def search():
    """Suchseite mit Ergebnissen."""
    query = request.args.get('q', '')
    
    if query:
        results = search_products(query)
    else:
        results = []
    
    return render_template('search.html', query=query, results=results)


@app.route('/product/<int:product_id>')
def product_detail(product_id):
    """Detailseite für ein Produkt mit Preisverlauf."""
    product = get_product_details(product_id)
    
    if not product:
        return "Produkt nicht gefunden", 404
    
    prices = get_price_history(product_id)
    stats = calculate_price_statistics(prices)
    
    # Diagramm erstellen
    chart_filename = create_price_chart(
        product_id, 
        f"{product['product_number']} - {product['description']}"
    )
    
    return render_template('product_detail.html',
                         product=product,
                         prices=prices,
                         stats=stats,
                         chart_filename=chart_filename)


@app.route('/api/search')
def api_search():
    """API-Endpoint für Suche (für AJAX)."""
    query = request.args.get('q', '')
    
    if len(query) < 2:
        return jsonify([])
    
    results = search_products(query)
    
    # Konvertiere zu JSON
    products_list = []
    for product in results:
        products_list.append({
            'id': product['id'],
            'product_number': product['product_number'],
            'description': product['description'],
            'category': product['category'],
            'current_price': product['current_price']
        })
    
    return jsonify(products_list)


if __name__ == '__main__':
    # Zeige Konfiguration beim Start
    print("\n🚀 Starte Flask-Applikation")
    Config.print_config()
    
    app.run(debug=True, port=Config.FLASK_PORT)
