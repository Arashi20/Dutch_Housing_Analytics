"""
Housing Data Transformation
===========================
Transform raw CBS data into analysis-ready format.

Purpose:
- Join dimension tables (codes → readable names)
- Clean and validate data
- Create derived metrics
- Prepare for statistical analysis and SQL loading

Pattern: Similar to transformation in Workout_Data_Pipeline
Reference: See README.md for research questions

Author: Arashi20
Date: 2026-02-20
Project: Dutch Housing Analytics - Rijksoverheid Portfolio
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional

from config import (
    RAW_DATA_DIR,
    PROCESSED_DATA_DIR,
    DATA_QUALITY_RULES,
    get_logger
)

# Initialize logger
logger = get_logger(__name__)


class HousingDataTransformer:
    """
    Transformer for CBS housing data.
    
    Pattern: Orchestrator class for transformation workflow
    Responsibilities:
    - Load raw data and dimension tables
    - Join dimensions to fact data (codes → names)
    - Clean and validate data
    - Create derived metrics
    - Save processed data
    
    Similar to: Data transformation classes in your other projects
    """
    
    def __init__(self):
        """Initialize transformer."""
        self.timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        logger.info("Housing Data Transformer initialized")
    
    
    def find_latest_file(self, pattern: str, directory: Path = RAW_DATA_DIR) -> Optional[Path]:
        """
        Find most recent file matching pattern.
        
        Pattern: Utility method for finding timestamped files
        
        Args:
            pattern: Glob pattern to match (e.g., 'fact_doorlooptijden_*.csv')
            directory: Directory to search in
            
        Returns:
            Path to most recent file, or None if not found
            
        Example:
            >>> transformer = HousingDataTransformer()
            >>> file = transformer.find_latest_file('fact_doorlooptijden_*.csv')
            >>> print(file)
            fact_doorlooptijden_2023_2024_86260NED_20260220_195500.csv
        """
        files = list(directory.glob(pattern))
        
        if not files:
            logger.warning(f"No files found matching: {pattern}")
            return None
        
        # Sort by modification time, get most recent
        latest_file = max(files, key=lambda p: p.stat().st_mtime)
        
        logger.info(f"Found latest file: {latest_file.name}")
        
        return latest_file
    
    
    def load_dimensions(self, table_id: str) -> Dict[str, pd.DataFrame]:
        """
        Load all dimension tables for a given CBS table.
        
        Args:
            table_id: CBS table ID (e.g., '86260NED')
            
        Returns:
            Dictionary mapping dimension names to DataFrames
            
        Example:
            >>> transformer = HousingDataTransformer()
            >>> dims = transformer.load_dimensions('86260NED')
            >>> print(dims.keys())
            dict_keys(['regiokenmerken', 'gebruiksfunctie', 'woningtype', 'perioden'])
        """
        logger.info(f"Loading dimension tables for {table_id}")
        
        dimensions = {}
        
        # Expected dimension names (lowercase for consistency)
        dim_names = ['regiokenmerken', 'gebruiksfunctie', 'woningtype', 'perioden']
        
        for dim_name in dim_names:
            # Find latest dimension file
            pattern = f"dim_{dim_name}_{table_id}_*.csv"
            file_path = self.find_latest_file(pattern)
            
            if file_path:
                df = pd.read_csv(file_path)
                dimensions[dim_name] = df
                logger.info(f"  ✓ Loaded {dim_name}: {len(df)} rows")
            else:
                logger.warning(f"  ✗ Dimension {dim_name} not found")
                dimensions[dim_name] = pd.DataFrame()
        
        return dimensions
    
    
    def transform_doorlooptijden(
        self,
        start_year: int,
        end_year: int
    ) -> pd.DataFrame:
        """
        Transform Doorlooptijden dataset.
        
        Steps:
        1. Load raw fact data
        2. Load dimension tables (legenda's)
        3. Join dimensions to fact data (codes → readable names)
        4. Parse and clean columns
        5. Validate data quality
        6. Create derived metrics
        
        Args:
            start_year: Start year of extracted data
            end_year: End year of extracted data
            
        Returns:
            Transformed DataFrame ready for analysis
        """
        logger.info("="*60)
        logger.info(f"TRANSFORMING DOORLOOPTIJDEN ({start_year}-{end_year})")
        logger.info("="*60)
        
        # ────────────────────────────────────────────────────────────
        # Step 1: Load raw fact data
        # ────────────────────────────────────────────────────────────
        logger.info("Step 1/6: Loading raw fact data")
        
        pattern = f"fact_doorlooptijden_{start_year}_{end_year}_*.csv"
        fact_file = self.find_latest_file(pattern)
        
        if not fact_file:
            raise FileNotFoundError(f"Fact file not found: {pattern}")
        
        df = pd.read_csv(fact_file)
        logger.info(f"  Loaded {len(df):,} rows, {len(df.columns)} columns")
        
        # ───────────────────────────────���────────────────────────────
        # Step 2: Load dimension tables
        # ────────────────────────────────────────────────────────────
        logger.info("Step 2/6: Loading dimension tables (legenda's)")
        
        dimensions = self.load_dimensions('86260NED')
        
        # ────────────────────────────────────────────────────────────
        # Step 3: Join dimensions (codes → readable names)
        # ────────────────────────────────────────────────────────────
        logger.info("Step 3/6: Joining dimensions (codes → names)")
        
        # Join Regiokenmerken (Region)
        if not dimensions['regiokenmerken'].empty:
            df = df.merge(
                dimensions['regiokenmerken'][['Key', 'Title']],
                left_on='Regiokenmerken',
                right_on='Key',
                how='left',
                suffixes=('', '_dim')
            )
            df = df.rename(columns={'Title': 'Regio_Naam'})
            df = df.drop(columns=['Key'], errors='ignore')
            logger.info("  ✓ Joined Regiokenmerken → Regio_Naam")
        
        # Join Gebruiksfunctie (Usage Type)
        if not dimensions['gebruiksfunctie'].empty:
            df = df.merge(
                dimensions['gebruiksfunctie'][['Key', 'Title']],
                left_on='Gebruiksfunctie',
                right_on='Key',
                how='left',
                suffixes=('', '_dim')
            )
            df = df.rename(columns={'Title': 'Gebruiksfunctie_Naam'})
            df = df.drop(columns=['Key'], errors='ignore')
            logger.info("  ✓ Joined Gebruiksfunctie → Gebruiksfunctie_Naam")
        
        # Join Woningtype (Housing Type)
        if not dimensions['woningtype'].empty:
            df = df.merge(
                dimensions['woningtype'][['Key', 'Title']],
                left_on='Woningtype',
                right_on='Key',
                how='left',
                suffixes=('', '_dim')
            )
            df = df.rename(columns={'Title': 'Woningtype_Naam'})
            df = df.drop(columns=['Key'], errors='ignore')
            logger.info("  ✓ Joined Woningtype → Woningtype_Naam")
        
        # Join Perioden (Time Period)
        if not dimensions['perioden'].empty:
            df = df.merge(
                dimensions['perioden'][['Key', 'Title']],
                left_on='Perioden',
                right_on='Key',
                how='left',
                suffixes=('', '_dim')
            )
            df = df.rename(columns={'Title': 'Periode_Naam'})
            df = df.drop(columns=['Key'], errors='ignore')
            logger.info("  ✓ Joined Perioden → Periode_Naam")
        
        # ────────────────────────────────────────────────────────────
        # Step 4: Parse and clean columns
        # ────────────────────────────────────────────────────────────
        logger.info("Step 4/6: Parsing and cleaning columns")
        
        # Extract year and quarter from period code
        # Format: '2023KW01' → year=2023, quarter=1
        df['Jaar'] = df['Perioden'].str[:4].astype(int)
        df['Kwartaal'] = df['Perioden'].str[-2:].astype(int)
        logger.info("  ✓ Extracted Jaar and Kwartaal from Perioden")
        
        # Rename measure columns to more readable names
        column_mapping = {
            'NieuwbouwTotaal_1': 'Nieuwbouw_Aantal',
            'k_10KwantielDoorlooptijdMaanden_2': 'Doorlooptijd_P10',
            'k_25KwantielDoorlooptijdMaanden_3': 'Doorlooptijd_P25',
            'MediaanDoorlooptijdMaanden_4': 'Doorlooptijd_Mediaan',
            'k_75KwantielDoorlooptijdMaanden_5': 'Doorlooptijd_P75',
            'k_90KwantielDoorlooptijdMaanden_6': 'Doorlooptijd_P90',
            'GemiddeldeDoorlooptijdMaanden_7': 'Doorlooptijd_Gemiddelde'
        }
        
        df = df.rename(columns=column_mapping)
        logger.info("  ✓ Renamed measure columns")
        
        # ──────────────────────────��─────────────────────────────────
        # Step 5: Data quality validation
        # ────────────────────────────────────────────────────────────
        logger.info("Step 5/6: Data quality validation")
        
        # Check for null values
        null_counts = df.isnull().sum()
        total_cells = len(df) * len(df.columns)
        total_nulls = null_counts.sum()
        null_pct = (total_nulls / total_cells) * 100
        
        logger.info(f"  Null values: {total_nulls:,} ({null_pct:.2f}% of all cells)")
        
        if null_pct > DATA_QUALITY_RULES['max_null_percentage'] * 100:
            logger.warning(f"  ⚠️  High null percentage: {null_pct:.2f}%")
        
        # Show columns with nulls
        if total_nulls > 0:
            logger.info("  Columns with null values:")
            for col, count in null_counts[null_counts > 0].items():
                pct = (count / len(df)) * 100
                logger.info(f"    • {col}: {count} ({pct:.1f}%)")
        
        # Check year range
        year_min, year_max = df['Jaar'].min(), df['Jaar'].max()
        logger.info(f"  Year range: {year_min}-{year_max}")
        
        if year_min < DATA_QUALITY_RULES['min_year']:
            logger.warning(f"  ⚠️  Data contains years before {DATA_QUALITY_RULES['min_year']}")
        
        if year_max > DATA_QUALITY_RULES['max_year']:
            logger.warning(f"  ⚠️  Data contains years after {DATA_QUALITY_RULES['max_year']}")
        
        # Check for outliers in doorlooptijd (basic check)
        for col in ['Doorlooptijd_Mediaan', 'Doorlooptijd_Gemiddelde']:
            if col in df.columns:
                q1 = df[col].quantile(0.25)
                q3 = df[col].quantile(0.75)
                iqr = q3 - q1
                lower_bound = q1 - 3 * iqr
                upper_bound = q3 + 3 * iqr
                
                outliers = df[(df[col] < lower_bound) | (df[col] > upper_bound)]
                
                if len(outliers) > 0:
                    logger.info(f"  Potential outliers in {col}: {len(outliers)} rows")
                    logger.info(f"    Range: [{lower_bound:.1f}, {upper_bound:.1f}] months")
        
        # ────────────────────────────────────────────────────────────
        # Step 6: Create derived metrics
        # ────────────────────────────────────────────────────────────
        logger.info("Step 6/6: Creating derived metrics")
        
        # Interquartile range (spread/variability)
        df['Doorlooptijd_IQR'] = df['Doorlooptijd_P75'] - df['Doorlooptijd_P25']
        logger.info("  ✓ Created Doorlooptijd_IQR (P75 - P25)")
        
        # 80th percentile range (P10 to P90)
        df['Doorlooptijd_P10_P90_Range'] = df['Doorlooptijd_P90'] - df['Doorlooptijd_P10']
        logger.info("  ✓ Created Doorlooptijd_P10_P90_Range")
        
        # Flag for high variability (IQR > threshold)
        # Threshold: If IQR > 10 months, consider it high variability
        df['Hoge_Variabiliteit'] = (df['Doorlooptijd_IQR'] > 10).astype(int)
        high_var_count = df['Hoge_Variabiliteit'].sum()
        logger.info(f"  ✓ Flagged {high_var_count} rows with high variability (IQR > 10)")
        
        # Coefficient of variation (relative spread)
        # CV = (P75 - P25) / Median
        df['Doorlooptijd_CV'] = df['Doorlooptijd_IQR'] / df['Doorlooptijd_Mediaan']
        logger.info("  ✓ Created Doorlooptijd_CV (coefficient of variation)")
        
        # ─────────────────────���──────────────────────────────────────
        # Final Summary
        # ────────────────────────────────────────────────────────────
        logger.info("Transformation summary:")
        logger.info(f"  Final shape: {df.shape[0]:,} rows × {df.shape[1]} columns")
        logger.info(f"  Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
        logger.info(f"  Date range: {year_min} Q{df['Kwartaal'].min()} - {year_max} Q{df['Kwartaal'].max()}")
        logger.info(f"  Unique regions: {df['Regio_Naam'].nunique()}")
        logger.info(f"  Unique housing types: {df['Woningtype_Naam'].nunique()}")
        
        return df
    
    
    def save_transformed_data(
        self,
        df: pd.DataFrame,
        dataset_name: str,
        formats: List[str] = ['csv', 'parquet']
    ):
        """
        Save transformed data in multiple formats.
        
        Args:
            df: Transformed DataFrame
            dataset_name: Name of dataset (e.g., 'doorlooptijden')
            formats: List of formats to save
        """
        logger.info("Saving transformed data")
        
        filename_base = f"{dataset_name}_transformed_{self.timestamp}"
        
        for fmt in formats:
            if fmt == 'csv':
                filepath = PROCESSED_DATA_DIR / f"{filename_base}.csv"
                df.to_csv(filepath, index=False)
                logger.info(f"  ✓ Saved CSV: {filepath.name}")
                
            elif fmt == 'parquet':
                filepath = PROCESSED_DATA_DIR / f"{filename_base}.parquet"
                df.to_parquet(filepath, index=False, engine='pyarrow')
                logger.info(f"  ✓ Saved Parquet: {filepath.name}")
        
        # Also save a "latest" version (overwrite) for easy access
        latest_csv = PROCESSED_DATA_DIR / f"{dataset_name}_latest.csv"
        df.to_csv(latest_csv, index=False)
        logger.info(f"  ✓ Saved latest version: {latest_csv.name}")


# ============================================================
# MAIN EXECUTION
# ============================================================

def main():
    """
    Main transformation workflow.
    
    Usage:
        python python/transform_housing_data.py
    """
    
    print("\n" + "="*70)
    print("CBS HOUSING DATA TRANSFORMATION")
    print("Project: Dutch Housing Analytics (Rijksoverheid Portfolio)")
    print("="*70 + "\n")
    
    # Configuration (should match extraction period)
    START_YEAR = 2015
    END_YEAR = 2025
    
    print(f"Transforming data: {START_YEAR}-{END_YEAR}")
    print()
    
    # Start transformation
    start_time = datetime.now()
    
    try:
        transformer = HousingDataTransformer()
        
        # Transform Doorlooptijden
        print("🔄 " * 35)
        print("TRANSFORMING DOORLOOPTIJDEN")
        print("🔄 " * 35 + "\n")
        
        df_doorlooptijden = transformer.transform_doorlooptijden(
            START_YEAR,
            END_YEAR
        )
        
        # Save transformed data
        transformer.save_transformed_data(
            df_doorlooptijden,
            'doorlooptijden',
            formats=['csv', 'parquet']
        )
        
        print(f"\n✓ Transformation complete: {len(df_doorlooptijden):,} rows")
        
        # Show preview
        print("\n" + "="*70)
        print("PREVIEW: First 5 rows (human-readable!)")
        print("="*70)
        
        preview_cols = [
            'Regio_Naam', 'Woningtype_Naam', 'Jaar', 'Kwartaal',
            'Doorlooptijd_Mediaan', 'Doorlooptijd_Gemiddelde', 
            'Nieuwbouw_Aantal'
        ]
        
        print(df_doorlooptijden[preview_cols].head().to_string(index=False))
        print()
        
        # Summary statistics
        print("="*70)
        print("SUMMARY STATISTICS")
        print("="*70)
        
        print("\nDoorlooptijd Mediaan (months):")
        print(df_doorlooptijden['Doorlooptijd_Mediaan'].describe())
        
        print("\nBy Region (Top 5 slowest):")
        by_region = df_doorlooptijden.groupby('Regio_Naam')['Doorlooptijd_Mediaan'].mean().sort_values(ascending=False)
        print(by_region.head())
        
        print("\nBy Housing Type:")
        by_type = df_doorlooptijden.groupby('Woningtype_Naam')['Doorlooptijd_Mediaan'].mean()
        print(by_type)
        
        # Duration
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print("\n" + "="*70)
        print("TRANSFORMATION COMPLETE!")
        print("="*70)
        print(f"Duration: {duration:.1f} seconds")
        print(f"Data saved to: {PROCESSED_DATA_DIR}")
        print()
        print("Next steps:")
        print("  1. Open doorlooptijden_latest.csv in Excel (human-readable!)")
        print("  2. Run statistical analysis (python/analyze_statistics.py)")
        print("  3. Load to SQL database (python/load_to_sql.py)")
        print("="*70 + "\n")
        
    except FileNotFoundError as e:
        logger.error(f"File not found: {str(e)}")
        print(f"\n❌ Error: {str(e)}")
        print("\nMake sure you've run extraction first:")
        print("  python python/extract_cbs_housing.py")
        
    except Exception as e:
        logger.error(f"Transformation failed: {str(e)}", exc_info=True)
        print(f"\n❌ Transformation failed: {str(e)}")
        print("Check logs/pipeline.log for details")
        raise


if __name__ == "__main__":
    main()