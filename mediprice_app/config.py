from pathlib import Path
from dotenv import load_dotenv
import os

# Lade .env aus Root-Verzeichnis
ROOT_DIR = Path(__file__).parent.resolve()
ENV_FILE = ROOT_DIR / '.env'

# Lade Umgebungsvariablen
load_dotenv(ENV_FILE)


class Config:
    """Zentrale Konfiguration für das gesamte Projekt."""
    
    # Basis-Pfade
    ROOT_DIR = ROOT_DIR
    
    # Datenbank
    DB_PATH = ROOT_DIR / os.getenv('DB_PATH', 'publications.db')
    DB_BACKUP_PATH = ROOT_DIR / os.getenv('DB_BACKUP_PATH', 'DB/backups')
    
    # Testdaten
    TESTDATA_PATH = ROOT_DIR / os.getenv('TESTDATA_PATH', 'Testdaten/BAG_xls_yyyy')
    EXCEL_FILE_PATTERN = os.getenv('EXCEL_FILE_PATTERN', 'Publications-*.xlsx')
    
    # Excel Import
    EXCEL_SHEET_NAME = os.getenv('EXCEL_SHEET_NAME', 'Publication')
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', '1000'))
    
    # Flask
    FLASK_APP = ROOT_DIR / os.getenv('FLASK_APP', 'mediprice_app/app.py')
    FLASK_ENV = os.getenv('FLASK_ENV', 'development')
    FLASK_SECRET_KEY = os.getenv('FLASK_SECRET_KEY', 'dev-secret-key')
    FLASK_PORT = int(os.getenv('FLASK_PORT', '5000'))
    FLASK_PLOT_FOLDER = ROOT_DIR / os.getenv('FLASK_PLOT_FOLDER', 'static/plots')
    
    # Logging
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    LOG_FILE = ROOT_DIR / os.getenv('LOG_FILE', 'logs/application.log')
    
    @classmethod
    def validate(cls):
        """Validiert die Konfiguration."""
        errors = []
        
        if not cls.DB_PATH.parent.exists():
            errors.append(f"DB-Verzeichnis existiert nicht: {cls.DB_PATH.parent}")
        
        if not cls.TESTDATA_PATH.exists():
            errors.append(f"Testdaten-Verzeichnis existiert nicht: {cls.TESTDATA_PATH}")
        
        # Erstelle Plot-Ordner falls nicht vorhanden
        if not cls.FLASK_PLOT_FOLDER.exists():
            cls.FLASK_PLOT_FOLDER.mkdir(parents=True, exist_ok=True)
            print(f"✅ Plot-Ordner erstellt: {cls.FLASK_PLOT_FOLDER}")
        
        if errors:
            print("⚠️  Konfigurations-Warnungen:")
            for error in errors:
                print(f"   • {error}")
        
        return len(errors) == 0
    
    @classmethod
    def print_config(cls):
        """Gibt die aktuelle Konfiguration aus."""
        print("\n" + "="*70)
        print("⚙️  AKTUELLE KONFIGURATION")
        print("="*70)
        print(f"Root-Verzeichnis:      {cls.ROOT_DIR}")
        print(f"Datenbank:             {cls.DB_PATH}")
        print(f"Testdaten:             {cls.TESTDATA_PATH}")
        print(f"Excel Pattern:         {cls.EXCEL_FILE_PATTERN}")
        print(f"Flask Port:            {cls.FLASK_PORT}")
        print(f"Flask Plot Folder:     {cls.FLASK_PLOT_FOLDER}")
        print(f"Environment:           {cls.FLASK_ENV}")
        print("="*70 + "\n")