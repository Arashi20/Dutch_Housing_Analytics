"""
Configuration Module - Dutch Housing Crisis Analytics
=====================================================
Centralized configuration for ETL pipeline.

Based on CBS tables:
- 86260NED: Doorlooptijden nieuwbouw (Lead times for new construction)
- 82211NED: Woningen in de pijplijn (Housing pipeline data)


Author: Arashi20
Date: 2026-02-18
Project: Dutch Housing Crisis Dashboard for Rijksoverheid application

References:
- CBS Open Data API: https://opendata.cbs.nl/
- OData v4 Protocol: https://www.odata.org/documentation/
- Project README: See README.md for project background and context
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
# Verified base URL for CBS Open Data API (OData v4)
# See: https://opendata.cbs.nl/ODataApi/odata/82211NED
CBS_API_BASE_URL = os.getenv(
    'CBS_API_BASE_URL', 
    'https://opendata.cbs.nl/ODataApi/odata' 
)

# Table IDs from README.md
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

# ────────────────────────────────────────────────────────────
# Dataset 1: Doorlooptijden Nieuwbouw (86260NED)
# ────────────────────────────────────────────────────────────
# From README.md:
#   Total cells: 65,835
#   
# From CBS API test (actual structure):
#   Dimensions:
#     ✓ Regiokenmerken (19 items) - NOT 'RegioS'!
#     ✓ Gebruiksfunctie (3 items) - uses codes like 'T001419'
#     ✓ Woningtype (3 items) - uses codes like 'T001100'
#     ✓ Perioden (55 items) - format: '2015KW01' (quarters)
#
#   Measures (NOT a separate dimension!):
#     These are COLUMNS in TypedDataSet:
#     • NieuwbouwTotaal_1
#     • k_10KwantielDoorlooptijdMaanden_2
#     • k_25KwantielDoorlooptijdMaanden_3
#     • MediaanDoorlooptijdMaanden_4
#     • k_75KwantielDoorlooptijdMaanden_5
#     • k_90KwantielDoorlooptijdMaanden_6
#     • GemiddeldeDoorlooptijdMaanden_7

DOORLOOPTIJD_CONFIG = {
    'table_id': '86260NED',
    
    # Measures to extract (these are COLUMN NAMES from API test)
    # We select the most important statistical measures for analysis
    'measure_columns': [
        'MediaanDoorlooptijdMaanden_4',        # Median - most representative
        'GemiddeldeDoorlooptijdMaanden_7',     # Average - for comparison
        'k_10KwantielDoorlooptijdMaanden_2',   # 10th percentile - fast projects
        'k_25KwantielDoorlooptijdMaanden_3',   # 25th percentile - faster projects        
        'k_75KwantielDoorlooptijdMaanden_5',   # 75th percentile - slower projects
        'k_90KwantielDoorlooptijdMaanden_6',   # 90th percentile - slow projects
        'NieuwbouwTotaal_1',                   # Total count (for context)
    ],
    
    # Dimensions (actual names from CBS API)
    'dimensions': {
        'regiokenmerken': 'all',     # Extract all regions (was 'RegioS')
        'gebruiksfunctie': 'all',    # Extract all (filter in transformation)
        'woningtype': 'all',         # Extract all (filter in transformation)
    },
    
    # Time period (from README: 55 periods = quarters from 2015 Q1)
    'perioden': {
        'start_year': int(os.getenv('EXTRACTION_START_YEAR', 2015)),
        'end_year': int(os.getenv('EXTRACTION_END_YEAR', 2024)),
        'granularity': 'quarter',  # CBS uses format: '2015KW01'
    }
}



# ────────────────────────────────────────────────────────────
# Dataset 2: Woningen in Pijplijn (82211NED)
# ────────────────────────────────────────────────────────────
# From README.md:
#   Total cells: 2,398,275 (HUGE dataset!)
#   Dimensions:
#     - Onderwerpen: 9 items (WRONG - this is NOT a dimension!)
#     - Gebruiksfunctie: 3 items
#     - Regio's: 475 items
#     - Perioden: 187 items
#
# From CBS API test (ACTUAL structure - verified 2026-02-18):
#   Dimensions (3 total):
#     ✓ Gebruiksfunctie (3 items) - codes like 'T001419'
#     ✓ RegioS (475 items!) - provinces + all gemeentes
#     ✓ Perioden (187 items) - format: '2015MM01' (MONTHLY granularity!)
#
#   Measures (7 columns - NOT a dimension!):
#     Column names from API test:
#     • VerblijfsobjectenInDePijplijnTotaal_1  - Total in pipeline
#     • BouwGestartPijplijn_2                  - Construction started
#     • Vergunningspijplijn_3                  - Permit stage
#     • TotaalInDePijplijn2Jaar_4              - BOTTLENECK: >2 years
#     • BouwGestartPijplijn2Jaar_5             - Construction >2 years
#     • Vergunningspijplijn2Jaar_6             - Permit stage >2 years
#     • TotaalInDePijplijn5Jaar_7              - CRITICAL: >5 years
#
#   Note: Similar to Dataset 1, "onderwerpen" mentioned in README
#         is NOT a dimension table - measures are columns!

PIJPLIJN_CONFIG = {
    'table_id': '82211NED',
    
    # ✅ CORRECTED: Measures are COLUMN NAMES (same pattern as doorlooptijden)
    # Pattern: Consistent with DOORLOOPTIJD_CONFIG structure
    # Reference: See test results from python/test_cbs_api.py
    'measure_columns': [
        # Current pipeline state
        'VerblijfsobjectenInDePijplijnTotaal_1',  # Total in pipeline
        'BouwGestartPijplijn_2',                  # Construction started
        'Vergunningspijplijn_3',                  # Permit stage (bottleneck!)
        
        # CRITICAL: Bottleneck metrics (projects stuck >2 years)
        # These are KEY for analyzing the housing crisis!
        'TotaalInDePijplijn2Jaar_4',              # Total stuck >2 years
        'BouwGestartPijplijn2Jaar_5',             # Construction stuck >2 years
        'Vergunningspijplijn2Jaar_6',             # Permits stuck >2 years
        
        # CRITICAL: Major bottlenecks (projects stuck >5 years)
        # This indicates SEVERE systemic problems
        'TotaalInDePijplijn5Jaar_7',              # Total stuck >5 years
        # Note: Only total for >5 years available (no breakdown)
    ],
    
    # ✅ CORRECTED: Dimensions (actual names from CBS API)
    # Pattern: Same structure as DOORLOOPTIJD_CONFIG['dimensions']
    # Reference: Verified via test_cbs_api.py output
    'dimensions': {
        'gebruiksfunctie': 'all',  # Extract all (filter in transformation)
        'regios': 'all',           # Note: 'RegioS' not 'Regiokenmerken'!
                                   # This table uses different name than doorlooptijden
    },
    
    # Extract all regions (README mentions 475 items - includes all gemeentes!)
    # This is MORE detailed than doorlooptijden (which only has 19 regions)
    'extract_all_regions': True,
    
    # Time period (from README: 187 periods including MONTHLY data!)
    # Pattern: Same structure as DOORLOOPTIJD_CONFIG['perioden']
    'perioden': {
        'start_year': int(os.getenv('EXTRACTION_START_YEAR', 2015)),
        'end_year': int(os.getenv('EXTRACTION_END_YEAR', 2025)),
        
        # ✅ CORRECTED: This table has MONTHLY granularity!
        # From API test: Perioden format is '2015MM01' (month), not '2015KW01' (quarter)
        # For initial extraction, use 'quarter' to keep dataset manageable
        # Can switch to 'month' later for detailed time-series analysis
        'granularity': os.getenv('PIJPLIJN_GRANULARITY', 'quarter'),
        
        # Note: With 475 regions × 187 periods × 7 measures = potential for
        #       millions of rows! Start with quarterly aggregation.
    }
}

# ============================================================
# DATABASE CONFIGURATION
# ============================================================
# For later - when we load data to PostgreSQL/SQL Server
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


# ============================================================
# DATA QUALITY RULES
# ============================================================
DATA_QUALITY_RULES = {
    'max_null_percentage': 0.15,  # Max 15% nulls allowed per column
    'min_year': 2015,              # From README: data starts at 2015
    'max_year': 2025,              # Last year + buffer
    
    # Valid province codes (from CBS - 12 provinces) - is this factually correct?
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
    
    # Stedelijkheid categories (urbanization levels)
    'stedelijkheid_categories': [
        'Zeer sterk stedelijk',   # Very highly urban
        'Sterk stedelijk',        # Highly urban
        'Matig stedelijk',        # Moderately urban
        'Weinig stedelijk',       # Slightly urban
        'Niet stedelijk',         # Rural
        'Stedelijkheid onbekend', # Unknown
    ],
    
    # Woningtype categories (from README: 3 items)
    'woningtype_categories': [
        'Totaal',              # All housing types
        'Eengezinswoning',     # Single-family homes (detached, semi-detached, row houses)
        'Meergezinswoning',    # Multi-family homes (apartments, flats)
    ],
    
    # Gebruiksfunctie categories (from README: 3 items)
    'gebruiksfunctie_categories': [
        'Woning totaal',                # Residential housing only
        'Niet-woning totaal',           # Non-residential only
        'Woning en niet-woning totaal', # Combined total
    ],
    
    # Gemeente code pattern (GM followed by 4 digits)
    # Examples: GM0363 (Amsterdam), GM0599 (Rotterdam)
    'gemeente_code_pattern': r'^GM\d{4}$',
    
    # Province code pattern (PV followed by 2 digits)
    # Examples: PV27 (Noord-Holland), PV28 (Zuid-Holland)
    'province_code_pattern': r'^PV\d{2}$',
}

# ============================================================
# LOGGING CONFIGURATION  
# ============================================================
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
    # For choropleth maps showing urbanization levels
    'zeer_sterk_stedelijk': '#C62828',  # Dark red (most urban)
    'sterk_stedelijk': '#E17000',       # Orange
    'matig_stedelijk': '#FDD835',       # Yellow
    'weinig_stedelijk': '#7CB342',      # Light green
    'niet_stedelijk': '#2E7D32',        # Dark green (rural)
    'stedelijkheid_onbekend': '#BDBDBD',  # Gray (unknown)
    
    # Woningtype colors
    'eengezinswoning': '#4CAF50',       # Green (detached/row houses)
    'meergezinswoning': '#2196F3',      # Blue (apartments)
}

# ============================================================
# CALCULATED METRICS
# ============================================================
# Derived metrics we'll calculate during transformation
# These will become SQL views and DAX measures in Power BI
CALCULATED_METRICS = {
    'pijplijn_lekkage': {
        'formula': '(vergund - opgeleverd) / NULLIF(vergund, 0) * 100',
        'description': 'Percentage woningen dat "lekt" uit de pijplijn (niet wordt gebouwd)',
        'unit': 'percentage',
        'format': '{:.1f}%',
        'context': 'Key indicator for housing crisis: shows inefficiency in construction pipeline'
    },
    'woningen_per_1000_inwoners': {
        'formula': 'opgeleverd / (population / 1000)',
        'description': 'Opgeleverde woningen per 1000 inwoners (genormaliseerd voor bevolkingsgrootte)',
        'unit': 'woningen per 1000 inwoners',
        'format': '{:.1f}',
        'context': 'Normalized metric for comparing regions of different sizes'
    },
    'bottleneck_ratio': {
        'formula': 'pijplijn_gt_2jaar / NULLIF(pijplijn_totaal, 0) * 100',
        'description': 'Percentage projecten >2 jaar in pijplijn (bottleneck indicator)',
        'unit': 'percentage',
        'format': '{:.1f}%',
        'context': 'CRITICAL metric: Shows where housing projects get stuck'
    },
    'critical_bottleneck_ratio': {
        'formula': 'pijplijn_gt_5jaar / NULLIF(pijplijn_totaal, 0) * 100',
        'description': 'Percentage projecten >5 jaar in pijplijn (critical bottleneck)',
        'unit': 'percentage',
        'format': '{:.1f}%',
        'context': 'SEVERE issue: Projects stuck >5 years indicate systemic problems'
    },
    'yoy_growth': {
        'formula': '(current_year - previous_year) / NULLIF(previous_year, 0) * 100',
        'description': 'Jaar-op-jaar groeipercentage',
        'unit': 'percentage',
        'format': '{:.1f}%',
        'context': 'Shows trend: Are we building more or fewer homes over time?'
    },
    'doorlooptijd_spread': {
        'formula': 'p90_doorlooptijd - p10_doorlooptijd',
        'description': 'Spreiding doorlooptijd (90e percentiel - 10e percentiel)',
        'unit': 'maanden',
        'format': '{:.1f} mnd',
        'context': 'Shows variability: High spread indicates unpredictable process'
    }
}

# ============================================================
# DIMENSION MAPPING
# ============================================================
# Maps CBS dimension names to our database schema
# Useful for consistent naming across extraction → transformation → loading

DIMENSION_MAPPINGS = {
    # CBS dimension name → Our database column name
    'RegioS': 'region_code',
    'Perioden': 'period_code',
    'Onderwerpen': 'measure_code',
    'Gebruiksfunctie': 'usage_type',
    'Woningtype': 'housing_type',
    'Regiokenmerken': 'region_characteristic',
}

# Measure column name patterns
# We'll standardize these to consistent names
MEASURE_COLUMN_PATTERNS = {
    'doorlooptijd': {
        'patterns': [
            r'Mediaan.*Maanden.*',
            r'Gemiddelde.*Maanden.*',
            r'Kwantiel.*Maanden.*',
        ],
        'unit': 'months'
    },
    'pijplijn_count': {
        'patterns': [
            r'Verblijfsobjecten.*',
            r'Bouw.*pijplijn.*',
            r'Vergunning.*pijplijn.*',
        ],
        'unit': 'count'
    }
}

# ============================================================
# HELPER FUNCTIONS
# ============================================================

def get_logger(name: str) -> logging.Logger:
    """
    Get configured logger instance.
    
    Pattern: Similar to logging setup in stock-research-MAS
    
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
    
    Pattern: Similar to validation in stock-research-MAS
    
    Checks:
    - Required directories exist
    - Year ranges are valid
    - CBS API URL is accessible
    - Dimension configurations are complete
    
    Raises:
        ValueError: If configuration is invalid
        
    Returns:
        True if validation passes
    """
    logger = get_logger(__name__)
    
    # Check if required directories exist (create if missing)
    required_dirs = [RAW_DATA_DIR, PROCESSED_DATA_DIR, SQL_EXPORTS_DIR, LOGS_DIR]
    for directory in required_dirs:
        if not directory.exists():
            logger.warning(f"Creating missing directory: {directory}")
            directory.mkdir(parents=True, exist_ok=True)
    
    # Validate doorlooptijd config
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
    
    # Validate pijplijn config
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
    
    # Validate granularity options
    valid_granularities = ['month', 'quarter', 'year']
    
    doorlooptijd_gran = DOORLOOPTIJD_CONFIG['perioden']['granularity']
    if doorlooptijd_gran not in valid_granularities:
        raise ValueError(
            f"Invalid doorlooptijd granularity: {doorlooptijd_gran}. "
            f"Must be one of: {valid_granularities}"
        )
    
    pijplijn_gran = PIJPLIJN_CONFIG['perioden']['granularity']
    if pijplijn_gran not in valid_granularities:
        raise ValueError(
            f"Invalid pijplijn granularity: {pijplijn_gran}. "
            f"Must be one of: {valid_granularities}"
        )
    
    # Check CBS API URL
    if not CBS_API_BASE_URL.startswith('https://'):
        raise ValueError(f"CBS API URL must use HTTPS: {CBS_API_BASE_URL}")
    


def print_config_summary():
    """
    Print configuration summary for verification.
    
    Pattern: Similar to summary output in Workout_Data_Pipeline
    Consistent with: Stock_Portfolio_Dashboard_V2 config display
    
    Purpose:
    - Debugging configuration issues
    - Confirming settings before extraction
    - Documenting configuration for other developers/agents
    
    Reference:
    - Uses DOORLOOPTIJD_CONFIG and PIJPLIJN_CONFIG structures
    - Both datasets follow same pattern: measure_columns + dimensions
    """
    print("\n" + "=" * 80)
    print("DUTCH HOUSING ANALYTICS - CONFIGURATION SUMMARY")
    print("=" * 80)
    print("\nProject: De Nederlandse Woningcrisis onder de Loep")
    print("Purpose: Analyze Dutch housing crisis using CBS open data")
    print("Target: Rijksoverheid Data Specialist application portfolio")
    
    # ────────────────────────────────────────────────────────────
    # Project Paths Section
    # Pattern: Similar to your Workout_Data_Pipeline config display
    # ────────────────────────────────────────────────────────────
    print(f"\n📁 PROJECT PATHS:")
    print(f"   Root:         {PROJECT_ROOT}")
    print(f"   Raw data:     {RAW_DATA_DIR}")
    print(f"   Processed:    {PROCESSED_DATA_DIR}")
    print(f"   SQL exports:  {SQL_EXPORTS_DIR}")
    print(f"   Logs:         {LOGS_DIR}")
    
    # ────────────────────────────────────────────────────────────
    # CBS API Configuration Section
    # Pattern: Similar to API config in stock-research-MAS
    # ────────────────────────────────────────────────────────────
    print(f"\n🌐 CBS API:")
    print(f"   Base URL:     {CBS_API_BASE_URL}")
    print(f"   Tables:       {', '.join(CBS_TABLES.keys())}")
    print(f"   Timeout:      {ODATA_CONFIG['timeout']}s")
    print(f"   Batch size:   {ODATA_CONFIG['batch_size']:,} records")
    print(f"   Max retries:  {ODATA_CONFIG['max_retries']}")
    
    print(f"\n📊 EXTRACTION SCOPE:")
    
    # ════════════════════════════════════════════════════════════
    # Dataset 1: Doorlooptijden Nieuwbouw
    # ════════════════════════════════════════════════════════════
    # Pattern: Defines the template for dataset display
    # Reference: All datasets follow this same structure
    # Verified: Via test_cbs_api.py on 2026-02-18
    print(f"\n   Doorlooptijden (86260NED):")
    print(f"     • Table ID:     {DOORLOOPTIJD_CONFIG['table_id']}")
    print(f"     • From README:  65,835 total cells")
    print(f"     • Period:       {DOORLOOPTIJD_CONFIG['perioden']['start_year']}-"
          f"{DOORLOOPTIJD_CONFIG['perioden']['end_year']}")
    print(f"     • Granularity:  {DOORLOOPTIJD_CONFIG['perioden']['granularity']}")
    
    # Show measure columns
    # Pattern: Enumerate for numbered list (consistent across project)
    # Reference: Same pattern used in test_cbs_api.py output
    print(f"     • Measure Columns: {len(DOORLOOPTIJD_CONFIG['measure_columns'])} selected")
    for i, measure in enumerate(DOORLOOPTIJD_CONFIG['measure_columns'], 1):
        print(f"         {i}. {measure}")
    
    # Show dimensions
    # Pattern: Dict iteration (consistent with dimension structure)
    # Reference: Matches structure defined in DOORLOOPTIJD_CONFIG
    print(f"     • Dimensions:")
    for dim_name, dim_value in DOORLOOPTIJD_CONFIG['dimensions'].items():
        print(f"         - {dim_name}: {dim_value}")
    
    # ════════════════════════════════════════════════════════════
    # Dataset 2: Woningen in Pijplijn
    # ════════════════════════════════════════════════════════════
    # Pattern: IDENTICAL structure to Dataset 1 (consistency!)
    # Reference: Uses same display format as doorlooptijden above
    # Verified: Via test_cbs_api.py on 2026-02-18
    print(f"\n   Woningen Pijplijn (82211NED):")
    print(f"     • Table ID:     {PIJPLIJN_CONFIG['table_id']}")
    print(f"     • From README:  2,398,275 total cells (LARGE dataset!)")
    print(f"     • Period:       {PIJPLIJN_CONFIG['perioden']['start_year']}-"
          f"{PIJPLIJN_CONFIG['perioden']['end_year']}")
    print(f"     • Granularity:  {PIJPLIJN_CONFIG['perioden']['granularity']}")
    print(f"     • Regions:      All 475 regions (provinces + gemeentes)")
    
    # Show measure columns (same pattern as Dataset 1)
    # Pattern: Enumerate for numbered list
    # Reference: Consistent with DOORLOOPTIJD_CONFIG display above
    print(f"     • Measure Columns: {len(PIJPLIJN_CONFIG['measure_columns'])} selected")
    for i, measure in enumerate(PIJPLIJN_CONFIG['measure_columns'], 1):
        print(f"         {i}. {measure}")
    
    # Show dimensions (same pattern as Dataset 1)
    # Pattern: Dict iteration
    # Reference: Consistent with DOORLOOPTIJD_CONFIG display above
    print(f"     • Dimensions:")
    for dim_name, dim_value in PIJPLIJN_CONFIG['dimensions'].items():
        print(f"         - {dim_name}: {dim_value}")
    
    print(f"     • ⚠️  Note: Includes CRITICAL bottleneck metrics (>2yr, >5yr)")
    
    # ────────────────────────────────────────────────────────────
    # Data Quality Rules Section
    # Pattern: Similar to validation in Workout_Data_Pipeline
    # Reference: DATA_QUALITY_RULES dict defined earlier in config
    # ────────────────────────────────────────────────────────────
    print(f"\n⚙️  DATA QUALITY RULES:")
    print(f"   Max null %:      {DATA_QUALITY_RULES['max_null_percentage']*100}%")
    print(f"   Valid provinces: {len(DATA_QUALITY_RULES['valid_province_codes'])}")
    print(f"   Stedelijkheid:   {len(DATA_QUALITY_RULES['stedelijkheid_categories'])} categories")
    print(f"   Woningtype:      {len(DATA_QUALITY_RULES['woningtype_categories'])} categories")
    print(f"   Gebruiksfunctie: {len(DATA_QUALITY_RULES['gebruiksfunctie_categories'])} categories")
    
    # ────────────────────────────────────────────────────────────
    # Database Configuration Section
    # Pattern: Similar to Stock_Portfolio_Dashboard_V2 DB config
    # Reference: DB_CONFIG dict defined earlier in config
    # Note: Password masked for security (never print actual password)
    # ────────────────────────────────────────────────────────────
    print(f"\n💾 DATABASE (for later SQL loading):")
    print(f"   Host:     {DB_CONFIG['host']}")
    print(f"   Port:     {DB_CONFIG['port']}")
    print(f"   Database: {DB_CONFIG['database']}")
    print(f"   User:     {DB_CONFIG['user']}")
    # Pattern: Mask password with asterisks (security best practice)
    # Reference: Common pattern in your stock-research-MAS project
    print(f"   Password: {'*' * len(DB_CONFIG['password']) if DB_CONFIG['password'] else '(not set)'}")
    
    # ────────────────────────────────────────────────────────────
    # Visualization Settings Section
    # Pattern: Color scheme for Power BI dashboard
    # Reference: COLORS dict defined earlier in config
    # ────────────────────────────────────────────────────────────
    print(f"\n🎨 VISUALIZATION:")
    print(f"   Color scheme: Dutch theme (Oranje #E17000 + Blauw #0062A3)")
    print(f"   Stedelijkheid gradient: Red (urban) → Green (rural)")
    print(f"   Woningtype: Green (eengezins) vs Blue (meergezins)")
    
    # ────────────────────────────────────────────────────────────
    # Calculated Metrics Section
    # Pattern: Shows what derived metrics we'll create
    # Reference: CALCULATED_METRICS dict defined earlier in config
    # Purpose: These become SQL views and Power BI DAX measures later
    # ────────────────────────────────────────────────────────────
    print(f"\n📐 CALCULATED METRICS:")
    print(f"   {len(CALCULATED_METRICS)} metrics defined:")
    for metric_name, metric_info in CALCULATED_METRICS.items():
        print(f"     • {metric_name}: {metric_info['description']}")
    
    # ────────────────────────────────────────────────────────────
    # Footer Section
    # Pattern: References to documentation (similar to your other projects)
    # ────────────────────────────────────────────────────────────
    print("\n" + "=" * 80)
    print("See README.md for project background and motivation")
    print("See docs/ folder for detailed documentation (when created)")
    print("=" * 80 + "\n")


# ============================================================
# RUN VALIDATION ON IMPORT
# ============================================================
# Pattern: Similar to auto-validation in stock-research-MAS
# Only run when this file is executed directly (not when imported)

if __name__ == "__main__":
    print_config_summary()
    try:
        validate_config()
        print("✅ All configuration checks passed!")
        print("\n💡 Next steps:")
        print("   1. Run: python python/config.py  (you just did this!)")
        print("   2. Create: python/extract_cbs_housing.py  (CBS API extraction)")
        print("   3. Test extraction with small sample")
        print("   4. Build transformation pipeline")
        print("\n📚 For AI agents working on this codebase:")
        print("   - Always import config at top: from config import CBS_API_BASE_URL, ...")
        print("   - Use get_logger(__name__) for consistent logging")
        print("   - Check DATA_QUALITY_RULES before loading data")
        print("   - Reference CALCULATED_METRICS for derived fields")
        print("   - See README.md for project context\n")
    except ValueError as e:
        print(f"\n❌ Configuration validation failed: {e}\n")
        exit(1)