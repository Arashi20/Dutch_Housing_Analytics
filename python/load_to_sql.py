"""
Load processed CSV data into SQL database (SQLite).

Creates star schema:
- Fact tables: doorlooptijden, woningen_pijplijn
- Dimension tables: regiokenmerken, regios, gebruiksfunctie, woningtype, perioden

Usage:
    python python/load_to_sql.py
    
Output:
    data/housing_analytics.db (SQLite database)
"""

import sqlite3
import pandas as pd
from pathlib import Path
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/load_sql.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Directories
PROCESSED_DIR = Path('data/processed')
RAW_DIR = Path('data/raw')
DB_PATH = Path('data/housing_analytics.db')


class SQLDatabaseLoader:
    """Load processed CSV data into SQLite database"""
    
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        self.conn = None
        
    def connect(self):
        """Connect to database (creates if not exists)"""
        logger.info(f"Connecting to database: {self.db_path}")
        self.conn = sqlite3.connect(self.db_path)
        logger.info("  [OK] Connected")
        
    def create_schema(self):
        """Execute DDL from SQL file"""
        logger.info("\nCreating database schema...")
        
        schema_file = Path('sql/schema/01_create_tables.sql')
        if not schema_file.exists():
            raise FileNotFoundError(f"Schema file not found: {schema_file}")
            
        with open(schema_file, 'r', encoding='utf-8') as f:
            schema_sql = f.read()
        
        self.conn.executescript(schema_sql)
        self.conn.commit()
        logger.info("  [OK] Database schema created")
        
    def load_dimension_regiokenmerken(self):
        """Load dim_regiokenmerken from raw CSV"""
        logger.info("\nLoading dim_regiokenmerken...")
        
        files = sorted(RAW_DIR.glob('dim_regiokenmerken_86260NED_*.csv'), reverse=True)
        if not files:
            logger.warning("  [SKIP] No regiokenmerken dimension file found")
            return
            
        df = pd.read_csv(files[0])
        
        df_mapped = df.rename(columns={'Key': 'code', 'Title': 'naam'})[['code', 'naam']]
        df_mapped['type'] = 'regio'
        
        df_mapped.to_sql('dim_regiokenmerken', self.conn, if_exists='replace', index=False)
        logger.info(f"  [OK] Loaded {len(df_mapped)} rows")
        
    def load_dimension_regios(self):
        """Load dim_regios from raw CSV"""
        logger.info("\nLoading dim_regios...")
        
        files = sorted(RAW_DIR.glob('dim_regios_82211NED_*.csv'), reverse=True)
        if not files:
            logger.warning("  [SKIP] No regios dimension file found")
            return
            
        df = pd.read_csv(files[0])
        df_mapped = df.rename(columns={'Key': 'code', 'Title': 'naam'})[['code', 'naam']]
        df_mapped['provincie'] = None
        
        df_mapped.to_sql('dim_regios', self.conn, if_exists='replace', index=False)
        logger.info(f"  [OK] Loaded {len(df_mapped)} rows")
        
    def load_dimension_gebruiksfunctie(self):
        """Load dim_gebruiksfunctie from raw CSV"""
        logger.info("\nLoading dim_gebruiksfunctie...")
        
        files = (list(RAW_DIR.glob('dim_gebruiksfunctie_86260NED_*.csv')) + 
                 list(RAW_DIR.glob('dim_gebruiksfunctie_82211NED_*.csv')))
        files = sorted(files, reverse=True)
        
        if not files:
            logger.warning("  [SKIP] No gebruiksfunctie dimension file found")
            return
            
        df = pd.read_csv(files[0])
        df_mapped = df.rename(columns={'Key': 'code', 'Title': 'naam'})[['code', 'naam']]
        df_mapped = df_mapped.drop_duplicates(subset=['code'])
        
        df_mapped.to_sql('dim_gebruiksfunctie', self.conn, if_exists='replace', index=False)
        logger.info(f"  [OK] Loaded {len(df_mapped)} rows")
        
    def load_dimension_woningtype(self):
        """Load dim_woningtype from raw CSV"""
        logger.info("\nLoading dim_woningtype...")
        
        files = sorted(RAW_DIR.glob('dim_woningtype_86260NED_*.csv'), reverse=True)
        if not files:
            logger.warning("  [SKIP] No woningtype dimension file found")
            return
            
        df = pd.read_csv(files[0])
        df_mapped = df.rename(columns={'Key': 'code', 'Title': 'naam'})[['code', 'naam']]
        
        df_mapped.to_sql('dim_woningtype', self.conn, if_exists='replace', index=False)
        logger.info(f"  [OK] Loaded {len(df_mapped)} rows")
        
    def load_dimension_perioden(self):
        """Load dim_perioden from raw CSV (combined from both datasets)"""
        logger.info("\nLoading dim_perioden...")
        
        files = (list(RAW_DIR.glob('dim_perioden_86260NED_*.csv')) + 
                 list(RAW_DIR.glob('dim_perioden_82211NED_*.csv')))
        files = sorted(files, reverse=True)
        
        if not files:
            logger.warning("  [SKIP] No perioden dimension files found")
            return
        
        dfs = []
        for file in files[:2]:
            df = pd.read_csv(file)
            dfs.append(df)
        
        df_combined = pd.concat(dfs, ignore_index=True)
        df_mapped = df_combined.rename(columns={'Key': 'code', 'Title': 'naam'})[['code', 'naam']]
        df_mapped = df_mapped.drop_duplicates(subset=['code'])
        
        df_mapped['jaar'] = df_mapped['code'].str[:4].astype(int)
        df_mapped['kwartaal'] = df_mapped['code'].str.extract(r'KW(\d+)')[0].astype(float)
        df_mapped['maand'] = df_mapped['code'].str.extract(r'MM(\d+)')[0].astype(float)
        
        df_mapped.to_sql('dim_perioden', self.conn, if_exists='replace', index=False)
        logger.info(f"  [OK] Loaded {len(df_mapped)} rows")
        
    def load_fact_doorlooptijden(self):
        """Load fact_doorlooptijden from processed CSV"""
        logger.info("\nLoading fact_doorlooptijden...")
        
        csv_file = PROCESSED_DIR / 'doorlooptijden_latest.csv'
        if not csv_file.exists():
            raise FileNotFoundError(f"Processed file not found: {csv_file}")
            
        df = pd.read_csv(csv_file)
        
        df_mapped = df.rename(columns={
            'ID': 'id', 'Regiokenmerken': 'regiokenmerk_code',
            'Gebruiksfunctie': 'gebruiksfunctie_code', 'Woningtype': 'woningtype_code',
            'Perioden': 'periode_code', 'Jaar': 'jaar', 'Kwartaal': 'kwartaal',
            'Doorlooptijd_Mediaan': 'doorlooptijd_mediaan',
            'Doorlooptijd_Gemiddelde': 'doorlooptijd_gemiddelde',
            'Doorlooptijd_P10': 'doorlooptijd_p10', 'Doorlooptijd_P25': 'doorlooptijd_p25',
            'Doorlooptijd_P75': 'doorlooptijd_p75', 'Doorlooptijd_P90': 'doorlooptijd_p90',
            'Doorlooptijd_IQR': 'doorlooptijd_iqr',
            'Doorlooptijd_P10_P90_Range': 'doorlooptijd_p10_p90_range',
            'Doorlooptijd_CV': 'doorlooptijd_cv', 'Nieuwbouw_Aantal': 'nieuwbouw_aantal',
            'Hoge_Variabiliteit': 'hoge_variabiliteit'
        })
        
        db_columns = ['id', 'regiokenmerk_code', 'gebruiksfunctie_code', 'woningtype_code',
                      'periode_code', 'jaar', 'kwartaal', 'doorlooptijd_mediaan',
                      'doorlooptijd_gemiddelde', 'doorlooptijd_p10', 'doorlooptijd_p25',
                      'doorlooptijd_p75', 'doorlooptijd_p90', 'doorlooptijd_iqr',
                      'doorlooptijd_p10_p90_range', 'doorlooptijd_cv', 'nieuwbouw_aantal',
                      'hoge_variabiliteit']
        
        df_mapped = df_mapped[[col for col in db_columns if col in df_mapped.columns]]
        df_mapped.to_sql('fact_doorlooptijden', self.conn, if_exists='replace', index=False)
        logger.info(f"  [OK] Loaded {len(df_mapped):,} rows")
        
    def load_fact_woningen_pijplijn(self):
        """Load fact_woningen_pijplijn from processed CSV"""
        logger.info("\nLoading fact_woningen_pijplijn...")
        
        csv_file = PROCESSED_DIR / 'woningen_pijplijn_latest.csv'
        if not csv_file.exists():
            raise FileNotFoundError(f"Processed file not found: {csv_file}")
            
        df = pd.read_csv(csv_file)
        
        df_mapped = df.rename(columns={
            'ID': 'id', 'RegioS': 'regio_code', 'Gebruiksfunctie': 'gebruiksfunctie_code',
            'Perioden': 'periode_code', 'Jaar': 'jaar', 'Maand': 'maand',
            'Pijplijn_Totaal': 'pijplijn_totaal', 'Pijplijn_BouwGestart': 'pijplijn_bouw_gestart',
            'Pijplijn_Vergunning': 'pijplijn_vergunning', 'Pijplijn_Vast_2Jaar': 'pijplijn_vast_2jaar',
            'Pijplijn_BouwGestart_2Jaar': 'pijplijn_bouw_gestart_2jaar',
            'Pijplijn_Vergunning_2Jaar': 'pijplijn_vergunning_2jaar',
            'Pijplijn_Vast_5Jaar': 'pijplijn_vast_5jaar',
            'BouwGestartPijplijn5Jaar_8': 'pijplijn_bouw_gestart_5jaar',
            'Vergunningspijplijn5Jaar_9': 'pijplijn_vergunning_5jaar',
            'Bottleneck_2Jaar_Pct': 'bottleneck_2jaar_pct', 'Bottleneck_5Jaar_Pct': 'bottleneck_5jaar_pct',
            'Vergunning_Bottleneck_Pct': 'vergunning_bottleneck_pct',
            'Bouw_Bottleneck_Pct': 'bouw_bottleneck_pct',
            'Vergunning_Fase_Pct': 'vergunning_fase_pct', 'Bouw_Fase_Pct': 'bouw_fase_pct',
            'Crisis_Regio': 'crisis_regio'
        })
        
        db_columns = ['id', 'regio_code', 'gebruiksfunctie_code', 'periode_code', 'jaar', 'maand',
                      'pijplijn_totaal', 'pijplijn_bouw_gestart', 'pijplijn_vergunning',
                      'pijplijn_vast_2jaar', 'pijplijn_bouw_gestart_2jaar', 'pijplijn_vergunning_2jaar',
                      'pijplijn_vast_5jaar', 'pijplijn_bouw_gestart_5jaar', 'pijplijn_vergunning_5jaar',
                      'bottleneck_2jaar_pct', 'bottleneck_5jaar_pct', 'vergunning_bottleneck_pct',
                      'bouw_bottleneck_pct', 'vergunning_fase_pct', 'bouw_fase_pct', 'crisis_regio']
        
        df_mapped = df_mapped[[col for col in db_columns if col in df_mapped.columns]]
        df_mapped.to_sql('fact_woningen_pijplijn', self.conn, if_exists='replace', index=False)
        logger.info(f"  [OK] Loaded {len(df_mapped):,} rows")
        
    def create_indexes(self):
        """Execute index creation SQL"""
        logger.info("\nCreating indexes...")
        
        index_file = Path('sql/schema/02_create_indexes.sql')
        if not index_file.exists():
            raise FileNotFoundError(f"Index file not found: {index_file}")
            
        with open(index_file, 'r', encoding='utf-8') as f:
            index_sql = f.read()
        
        self.conn.executescript(index_sql)
        self.conn.commit()
        logger.info("  [OK] Indexes created")
        
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("\n[OK] Database connection closed")
            
    def run(self):
        """Complete loading workflow"""
        logger.info("=" * 70)
        logger.info("LOADING DATA TO SQL DATABASE")
        logger.info("=" * 70)
        
        try:
            self.connect()
            self.create_schema()
            
            logger.info("\n" + "=" * 70)
            logger.info("LOADING DIMENSION TABLES")
            logger.info("=" * 70)
            self.load_dimension_regiokenmerken()
            self.load_dimension_regios()
            self.load_dimension_gebruiksfunctie()
            self.load_dimension_woningtype()
            self.load_dimension_perioden()
            
            logger.info("\n" + "=" * 70)
            logger.info("LOADING FACT TABLES")
            logger.info("=" * 70)
            self.load_fact_doorlooptijden()
            self.load_fact_woningen_pijplijn()
            
            self.create_indexes()
            
            logger.info("\n" + "=" * 70)
            logger.info("[OK] DATABASE LOADING COMPLETE")
            logger.info(f"[OK] Database: {self.db_path.absolute()}")
            logger.info("=" * 70)
            
        except Exception as e:
            logger.error(f"Database loading failed: {str(e)}", exc_info=True)
            raise
        finally:
            self.close()


def main():
    """Main entry point"""
    loader = SQLDatabaseLoader()
    loader.run()


if __name__ == "__main__":
    main()