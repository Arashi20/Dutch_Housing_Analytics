"""
Configuration Module - Dutch Housing Crisis Analytics
=====================================================
Centralized configuration for ETL pipeline.

Similar to config patterns in:
- Stock_Portfolio_Dashboard_V2/config.py
- stock-research-MAS agent configuration

Author: Arashi20
Date: 2026-02-18
Project: Dutch Housing Crisis Dashboard for Rijksoverheid application
"""

import os
from pathlib import Path
from dotenv import load_dotenv
import logging

# Load environment variables from .env file
load_dotenv()

# ============================================================
# PROJECT PATHS
# ============================================================
# Similar structure to Workout_Data_Pipeline
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / 'data'
RAW_DATA_DIR = DATA_DIR / 'raw'
PROCESSED_DATA_DIR = DATA_DIR / 'processed'
SQL_EXPORTS_DIR = DATA_DIR / 'sql_exports'
LOGS_DIR = PROJECT_ROOT / 'logs'
SQL_DIR = PROJECT_ROOT / 'sql'
DOCS_DIR = PROJECT_ROOT / 'docs'
NOTEBOOKS_DIR = PROJECT_ROOT / 'notebooks'
POWERBI_DIR = PROJECT_ROOT / 'powerbi'

# Create directories if they don't exist
for directory in [RAW_DATA_DIR, PROCESSED_DATA_DIR, SQL_EXPORTS_DIR, 
                  LOGS_DIR, SQL_DIR, DOCS_DIR, NOTEBOOKS_DIR, POWERBI_DIR]:
    directory.mkdir(parents=True, exist_ok=True)


# ============================================================
# CBS API CONFIGURATION
# ============================================================
# ODataApi (JSON endpoint)
CBS_API_BASE_URL = os.getenv(
    'CBS_API_BASE_URL', 
    'https://opendata.cbs.nl/ODataApi/odata'
)

# Table IDs (CBS website)
CBS_TABLES = {
    'doorlooptijden': '86260NED',      # Doorlooptijden nieuwbouw
    'woningen_pijplijn': '82211NED',   # Woningen en niet-woningen in de pijplijn
}


# OData request configuration
ODATA_CONFIG = {
    'timeout': 60,                # Request timeout (seconds)
    'max_retries': 3,             # Number of retry attempts
    'batch_size': 10000,          # Records per request (pagination)
    'retry_delay_base': 2,        # Base delay for exponential backoff
    'rate_limit_delay': 10,       # Delay when rate limited (429 error)
    'backoff_factor': 2,          # Exponential backoff multiplier
}

# ============================================================
# DATA EXTRACTION CONFIGURATION
# ============================================================

# Dataset 1: Doorlooptijden (Lead Times)
# Based on your screenshot showing "Onderwerpen" dimension
DOORLOOPTIJD_CONFIG = {
    'table_id': '86260NED',
    
    # Measures to extract:
    # We select median + average + percentiles for distribution analysis
    'onderwerpen': [
        'Mediaan doorlooptijd (maanden)',        # 50th percentile - most representative
        'Gemiddelde doorlooptijd (maanden)',     # Mean - for comparison with median
        '10% Kwantiel doorlooptijd (maanden)',   # Fast projects (10th percentile)
        '90% Kwantiel doorlooptijd (maanden)',   # Slow projects (90th percentile)
    ],
    
    # Extract all regions (provinces + stedelijkheid categories)
    'extract_all_regions': True,
    
    # Time period
    'perioden': {
        'start_year': int(os.getenv('EXTRACTION_START_YEAR', 2015)),
        'end_year': int(os.getenv('EXTRACTION_END_YEAR', 2024)),
        'granularity': 'year',  # This table only has yearly data
    }
}

# Dataset 2: Woningen in Pijplijn (Pipeline)
# Based on your screenshot showing bouwstroom categories (Image 3)
PIJPLIJN_CONFIG = {
    'table_id': '82211NED',
    
    # From your screenshot - we want these pipeline stages
    # Note: These are COLUMN VALUES, not separate dimension tables!
    'bouwstroom_phases': [
        # Current pipeline state
        'Verblijfsobjecten in de pijplijn totaal',
        'Bouw gestart pijplijn',
        'Vergunningspijplijn',
        
        # CRITICAL: Bottleneck metrics (projects stuck >2 years)
        'Totaal in de pijplijn (>2 jaar)',
        'Bouw gestart pijplijn (>2 jaar)',
        'Vergunningspijplijn (>2 jaar)',
        
        # CRITICAL: Major bottlenecks (projects stuck >5 years)
        'Totaal in de pijplijn (>5 jaar)',
        'Bouw gestart pijplijn (>5 jaar)',
        'Vergunningspijplijn (>5 jaar)',
    ],
    
    # From your screenshot Image 4 - Gebruiksfunctie dimension
    'gebruiksfunctie': [
        'Woning totaal',  # Focus on residential housing
        # We skip 'Niet-woning totaal' (commercial buildings - out of scope)
    ],
    
    # Extract all regions (gemeentes + provinces)
    'extract_all_regions': True,
    
    # Time period
    'perioden': {
        'start_year': int(os.getenv('EXTRACTION_START_YEAR', 2015)),
        'end_year': int(os.getenv('EXTRACTION_END_YEAR', 2025)),
        'granularity': 'quarter',  # This table has quarterly data!
    }
}

# ============================================================
# DATABASE CONFIGURATION
# ============================================================
# For later - when we load data to PostgreSQL/SQL Server
# Similar to Stock_Portfolio_Dashboard_V2 database setup
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'database': os.getenv('DB_NAME', 'dutch_housing_analytics'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', ''),
}

# SQLAlchemy connection string (for pandas.to_sql)
DATABASE_URL = (
    f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)

# Alternative: SQL Server (if Rijksoverheid uses MSSQL)
# DATABASE_URL = (
#     f"mssql+pyodbc://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
#     f"@{DB_CONFIG['host']}/{DB_CONFIG['database']}"
#     f"?driver=ODBC+Driver+17+for+SQL+Server"
# )

# ============================================================
# DATA QUALITY RULES
# ============================================================
# Similar to validation in Workout_Data_Pipeline
DATA_QUALITY_RULES = {
    'max_null_percentage': 0.15,  # Max 15% nulls allowed per column
    'min_year': 2015,
    'max_year': 2025,
    
    # Valid province codes (from CBS - your screenshot Image 2)
    'valid_province_codes': [
        'PV20',  # Groningen
        'PV21',  # Fryslân  
        'PV22',  # Drenthe
        'PV23',  # Overijssel
        'PV24',  # Flevoland
        'PV25',  # Gelderland
        'PV26',  # Utrecht
        'PV27',  # Noord-Holland
        'PV28',  # Zuid-Holland
        'PV29',  # Zeeland
        'PV30',  # Noord-Brabant
        'PV31',  # Limburg
    ],
    
    # Stedelijkheid categories (from your screenshot Image 2)
    'stedelijkheid_categories': [
        'Zeer sterk stedelijk',
        'Sterk stedelijk',
        'Matig stedelijk',
        'Weinig stedelijk',
        'Niet stedelijk',
        'Stedelijkheid onbekend',
    ],
    
    # Gemeente code pattern (GM followed by 4 digits)
    'gemeente_code_pattern': r'^GM\d{4}$',
}

# ============================================================
# LOGGING CONFIGURATION  
# ============================================================
# Similar to logging in stock-research-MAS
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
LOG_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'detailed': {
            'format': LOG_FORMAT,
            'datefmt': LOG_DATE_FORMAT
        },
        'simple': {
            'format': '%(levelname)s - %(message)s'
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': LOG_LEVEL,
            'formatter': 'simple',
            'stream': 'ext://sys.stdout'
        },
        'file': {
            'class': 'logging.handlers.RotatingFileHandler',
            'level': 'DEBUG',
            'formatter': 'detailed',
            'filename': str(LOGS_DIR / 'pipeline.log'),
            'maxBytes': 10485760,  # 10MB
            'backupCount': 5,
            'encoding': 'utf-8'
        }
    },
    'loggers': {
        '': {  # Root logger
            'handlers': ['console', 'file'],
            'level': 'DEBUG',
            'propagate': False
        }
    }
}

# ============================================================
# VISUALIZATION SETTINGS
# ============================================================
# Color scheme for Power BI dashboard (Dutch theme)
COLORS = {
    # Primary colors (Dutch flag inspired)
    'primary': '#E17000',      # Oranje (Dutch Orange)
    'secondary': '#0062A3',    # Dutch Blue
    'accent': '#FDD835',       # Yellow/Gold
    
    # Status colors (traffic light system)
    'success': '#2E7D32',      # Green - good performance
    'warning': '#F57C00',      # Orange - needs attention
    'danger': '#C62828',       # Red - critical issues
    'neutral': '#757575',      # Gray - neutral/unknown
    
    # Stedelijkheid gradient (urban → rural)
    'zeer_sterk_stedelijk': '#C62828',  # Dark red (most urban)
    'sterk_stedelijk': '#E17000',       # Orange
    'matig_stedelijk': '#FDD835',       # Yellow
    'weinig_stedelijk': '#7CB342',      # Light green
    'niet_stedelijk': '#2E7D32',        # Dark green (rural)
    'stedelijkheid_onbekend': '#BDBDBD',  # Gray
}

# ============================================================
# CALCULATED METRICS
# ============================================================
# Derived metrics we'll calculate during transformation
# These will become SQL views and DAX measures in Power BI
CALCULATED_METRICS = {
    'pijplijn_lekkage': {
        'formula': '(vergund - opgeleverd) / NULLIF(vergund, 0) * 100',
        'description': 'Percentage woningen dat "lekt" uit de pijplijn',
        'unit': 'percentage',
        'format': '{:.1f}%'
    },
    'woningen_per_1000_inwoners': {
        'formula': 'opgeleverd / (population / 1000)',
        'description': 'Opgeleverde woningen per 1000 inwoners (genormaliseerd)',
        'unit': 'woningen per 1000 inwoners',
        'format': '{:.1f}'
    },
    'bottleneck_ratio': {
        'formula': 'pijplijn_gt_2jaar / NULLIF(pijplijn_totaal, 0) * 100',
        'description': 'Percentage projecten >2 jaar in pijplijn (bottleneck indicator)',
        'unit': 'percentage',
        'format': '{:.1f}%'
    },
    'critical_bottleneck_ratio': {
        'formula': 'pijplijn_gt_5jaar / NULLIF(pijplijn_totaal, 0) * 100',
        'description': 'Percentage projecten >5 jaar in pijplijn (critical bottleneck)',
        'unit': 'percentage',
        'format': '{:.1f}%'
    },
    'yoy_growth': {
        'formula': '(current_year - previous_year) / NULLIF(previous_year, 0) * 100',
        'description': 'Jaar-op-jaar groeipercentage',
        'unit': 'percentage',
        'format': '{:.1f}%'
    }
}

# ============================================================
# HELPER FUNCTIONS
# ============================================================

def get_logger(name: str) -> logging.Logger:
    """
    Get configured logger instance.
    
    Args:
        name: Logger name (usually __name__)
        
    Returns:
        Configured logger instance
        
    Usage:
        from config import get_logger
        logger = get_logger(__name__)
        logger.info("Starting extraction...")
    """
    import logging.config
    logging.config.dictConfig(LOGGING_CONFIG)
    return logging.getLogger(name)


def validate_config():
    """
    Validate configuration settings.
    Similar to validation in stock-research-MAS.
    
    Raises:
        ValueError: If configuration is invalid
        
    Returns:
        True if validation passes
    """
    logger = get_logger(__name__)
    
    # Check if required directories exist
    required_dirs = [RAW_DATA_DIR, PROCESSED_DATA_DIR, SQL_EXPORTS_DIR, LOGS_DIR]
    for directory in required_dirs:
        if not directory.exists():
            logger.warning(f"Creating missing directory: {directory}")
            directory.mkdir(parents=True, exist_ok=True)
    
    # Check year ranges
    doorlooptijd_start = DOORLOOPTIJD_CONFIG['perioden']['start_year']
    doorlooptijd_end = DOORLOOPTIJD_CONFIG['perioden']['end_year']
    
    if doorlooptijd_start < DATA_QUALITY_RULES['min_year']:
        raise ValueError(
            f"Doorlooptijd start year ({doorlooptijd_start}) is before "
            f"minimum allowed year ({DATA_QUALITY_RULES['min_year']})"
        )
    
    if doorlooptijd_end > DATA_QUALITY_RULES['max_year']:
        raise ValueError(
            f"Doorlooptijd end year ({doorlooptijd_end}) is after "
            f"maximum allowed year ({DATA_QUALITY_RULES['max_year']})"
        )
    
    # Check pijplijn year range
    pijplijn_start = PIJPLIJN_CONFIG['perioden']['start_year']
    pijplijn_end = PIJPLIJN_CONFIG['perioden']['end_year']
    
    if pijplijn_start < DATA_QUALITY_RULES['min_year']:
        raise ValueError(
            f"Pijplijn start year ({pijplijn_start}) is before "
            f"minimum allowed year ({DATA_QUALITY_RULES['min_year']})"
        )
    
    if pijplijn_end > DATA_QUALITY_RULES['max_year']:
        raise ValueError(
            f"Pijplijn end year ({pijplijn_end}) is after "
            f"maximum allowed year ({DATA_QUALITY_RULES['max_year']})"
        )
    
    # Check CBS API base URL
    if not CBS_API_BASE_URL.startswith('https://'):
        raise ValueError(f"CBS API URL must use HTTPS: {CBS_API_BASE_URL}")
    
    logger.info("✓ Configuration validated successfully")
    return True


def print_config_summary():
    """
    Print configuration summary for verification.
    Useful for debugging and confirming settings before extraction.
    """
    print("\n" + "=" * 80)
    print("DUTCH HOUSING ANALYTICS - CONFIGURATION SUMMARY")
    print("=" * 80)
    
    print(f"\n📁 PROJECT PATHS:")
    print(f"   Root:         {PROJECT_ROOT}")
    print(f"   Raw data:     {RAW_DATA_DIR}")
    print(f"   Processed:    {PROCESSED_DATA_DIR}")
    print(f"   SQL exports:  {SQL_EXPORTS_DIR}")
    print(f"   Logs:         {LOGS_DIR}")
    
    print(f"\n🌐 CBS API:")
    print(f"   Base URL:     {CBS_API_BASE_URL}")
    print(f"   Tables:       {', '.join(CBS_TABLES.keys())}")
    print(f"   Timeout:      {ODATA_CONFIG['timeout']}s")
    print(f"   Batch size:   {ODATA_CONFIG['batch_size']:,} records")
    print(f"   Max retries:  {ODATA_CONFIG['max_retries']}")
    
    print(f"\n📊 EXTRACTION SCOPE:")
    print(f"   Doorlooptijden:")
    print(f"     • Table ID:   {DOORLOOPTIJD_CONFIG['table_id']}")
    print(f"     • Period:     {DOORLOOPTIJD_CONFIG['perioden']['start_year']}-"
          f"{DOORLOOPTIJD_CONFIG['perioden']['end_year']}")
    print(f"     • Measures:   {len(DOORLOOPTIJD_CONFIG['onderwerpen'])} selected")
    for measure in DOORLOOPTIJD_CONFIG['onderwerpen']:
        print(f"         - {measure}")
    
    print(f"\n   Woningen Pijplijn:")
    print(f"     • Table ID:   {PIJPLIJN_CONFIG['table_id']}")
    print(f"     • Period:     {PIJPLIJN_CONFIG['perioden']['start_year']}-"
          f"{PIJPLIJN_CONFIG['perioden']['end_year']}")
    print(f"     • Granularity: {PIJPLIJN_CONFIG['perioden']['granularity']}")
    print(f"     • Stages:     {len(PIJPLIJN_CONFIG['bouwstroom_phases'])} selected")
    print(f"         Including: >2 jaar bottleneck metrics ✓")
    print(f"         Including: >5 jaar critical bottleneck metrics ✓")
    
    print(f"\n⚙️  DATA QUALITY:")
    print(f"   Max null %:      {DATA_QUALITY_RULES['max_null_percentage']*100}%")
    print(f"   Valid provinces: {len(DATA_QUALITY_RULES['valid_province_codes'])}")
    print(f"   Stedelijkheid:   {len(DATA_QUALITY_RULES['stedelijkheid_categories'])} categories")
    
    print(f"\n💾 DATABASE (for later):")
    print(f"   Host:     {DB_CONFIG['host']}")
    print(f"   Port:     {DB_CONFIG['port']}")
    print(f"   Database: {DB_CONFIG['database']}")
    print(f"   User:     {DB_CONFIG['user']}")
    print(f"   Password: {'*' * len(DB_CONFIG['password']) if DB_CONFIG['password'] else '(not set)'}")
    
    print(f"\n🎨 VISUALIZATION:")
    print(f"   Color scheme: Dutch theme (Oranje + Blue)")
    print(f"   Stedelijkheid gradient: Red (urban) → Green (rural)")
    
    print(f"\n📐 CALCULATED METRICS:")
    print(f"   {len(CALCULATED_METRICS)} metrics defined:")
    for metric_name, metric_info in CALCULATED_METRICS.items():
        print(f"     • {metric_name}: {metric_info['description']}")
    
    print("\n" + "=" * 80 + "\n")


# ============================================================
# RUN VALIDATION ON IMPORT (like stock-research-MAS)
# ============================================================

# Only run validation and print summary if this file is executed directly
if __name__ == "__main__":
    print_config_summary()
    try:
        validate_config()
        print("✅ All configuration checks passed!\n")
    except ValueError as e:
        print(f"\n❌ Configuration validation failed: {e}\n")
        exit(1)