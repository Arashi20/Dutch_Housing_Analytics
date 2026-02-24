"""
CBS Housing Data Extraction
============================
Main extraction script for Dutch Housing Analytics project.

Purpose:
- Extract data from CBS StatLine OData API
- Save raw data to data/raw/ for further processing
- Focus on Doorlooptijden and Woningen Pijplijn tables

Pattern: Similar to main extraction scripts in Workout_Data_Pipeline
Reference: See README.md for research questions and motivation

Author: Arashi20
Date: 2026-02-19
Project: Dutch Housing Analytics - Rijksoverheid Portfolio
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, Tuple, Optional 

from config import (
    CBS_TABLES,
    DOORLOOPTIJD_CONFIG,
    PIJPLIJN_CONFIG,
    RAW_DATA_DIR,
    get_logger
)

from cbs_api_client import CBSAPIClient, build_period_filter

# Initialize logger
logger = get_logger(__name__)


class HousingDataExtractor:
    """
    Extractor for CBS housing data.
    
    Pattern: Orchestrator class (similar to your other projects)
    Responsibilities:
    - Coordinate API calls via CBSAPIClient
    - Apply business logic (filters, transformations)
    - Save data to appropriate formats
    - Log extraction progress and statistics
    """
    
    def __init__(self):
        """Initialize extractor with CBS API client."""
        self.client = CBSAPIClient()
        self.timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        logger.info("Housing Data Extractor initialized")
    
    
    def extract_doorlooptijden(
        self,
        start_year: int,
        end_year: int,
        save_formats: list = ['csv', 'parquet']
    ) -> Tuple[pd.DataFrame, Dict[str, pd.DataFrame]]:
        """
        Extract Doorlooptijden dataset (86260NED).
        
        Pattern: Dataset-specific extraction method
        
        Args:
            start_year: Start year for data extraction
            end_year: End year for data extraction
            save_formats: List of formats to save ('csv', 'parquet')
            
        Returns:
            Tuple of (fact_data, dimensions_dict)
            
        Example:
            >>> extractor = HousingDataExtractor()
            >>> facts, dims = extractor.extract_doorlooptijden(2023, 2024)
            >>> print(f"Extracted {len(facts)} rows")
        """
        table_id = DOORLOOPTIJD_CONFIG['table_id']
        
        logger.info("="*60)
        logger.info(f"EXTRACTING DOORLOOPTIJDEN ({table_id})")
        logger.info(f"Period: {start_year}-{end_year}")
        logger.info("="*60)
        
        # ────────────────────────────────────────────────────────────
        # Step 1: Extract dimension tables (lookup data)
        # ────────────────────────────────────────────────────────────
        logger.info("Step 1/3: Extracting dimension tables")
        
        dimensions = self.client.get_all_dimensions(table_id)
        
        for dim_name, dim_df in dimensions.items():
            logger.info(f"  • {dim_name}: {len(dim_df)} items")
            
            # Save dimension table
            dim_filename = f"dim_{dim_name.lower()}_{table_id}_{self.timestamp}"
            self._save_dataframe(dim_df, dim_filename, save_formats)
        
        # ────────────────────────────────────────────────────────────
        # Step 2: Build filters for fact data
        # ────────────────────────────────────────────────────────────
        logger.info("Step 2/3: Building filters for fact data")
        
        filters = []
        
        # Period filter
        period_filter = build_period_filter(
            start_year,
            end_year,
            DOORLOOPTIJD_CONFIG['perioden']['granularity']
        )
        filters.append(period_filter)
        logger.info(f"  • Period filter: {period_filter}")
        
        # We don't filter on dimensions (gebruiksfunctie, woningtype, regio)
        # because we want ALL combinations for comprehensive analysis
        # Filtering will happen during transformation phase
        
        # ────────────────────────────────���───────────────────────────
        # Step 3: Extract fact data (measurements)
        # ────────────────────────────────────────────────────────────
        logger.info("Step 3/3: Extracting fact data (paginated)")
        
        # Select only columns we need (from config)
        # This reduces data transfer and storage
        measure_cols = DOORLOOPTIJD_CONFIG['measure_columns']
        dimension_cols = ['ID', 'Regiokenmerken', 'Gebruiksfunctie', 
                         'Woningtype', 'Perioden']
        select_cols = dimension_cols + measure_cols
        
        logger.info(f"  • Selecting {len(select_cols)} columns")
        logger.info(f"    Dimensions: {dimension_cols}")
        logger.info(f"    Measures: {measure_cols}")
        
        fact_data = self.client.get_data_paginated(
            table_id=table_id,
            filters=filters,
            select=select_cols
        )
        
        logger.info(f"  • Extracted {len(fact_data)} rows")
        
        # ────────────────────────────────────────────────────────────
        # Step 4: Data quality checks
        # ────────────────────────────────────────────────────────────
        logger.info("Data quality summary:")
        logger.info(f"  • Total rows: {len(fact_data):,}")
        logger.info(f"  • Total columns: {len(fact_data.columns)}")
        logger.info(f"  • Memory usage: {fact_data.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
        
        # Check for nulls
        null_counts = fact_data.isnull().sum()
        if null_counts.sum() > 0:
            logger.warning("Null values detected:")
            for col, count in null_counts[null_counts > 0].items():
                pct = (count / len(fact_data)) * 100
                logger.warning(f"  • {col}: {count} ({pct:.1f}%)")
        
        # ────────────────────────────────────────────────────────────
        # Step 5: Save fact data
        # ────────────────────────────────────────────────────────────
        fact_filename = (
            f"fact_doorlooptijden_{start_year}_{end_year}_"
            f"{table_id}_{self.timestamp}"
        )
        
        self._save_dataframe(fact_data, fact_filename, save_formats)
        
        logger.info("✓ Doorlooptijden extraction complete!")
        
        return fact_data, dimensions
    
    
    def extract_woningen_pijplijn_feed_api(
        self,
        start_year: int,
        end_year: int,
        save_formats: list = ['csv', 'parquet']
    ) -> Tuple[pd.DataFrame, Dict[str, pd.DataFrame]]:
        """
        Extract Woningen Pijplijn dataset (82211NED) using CBS Feed API.
        
        The CBS Standard API has a 10,000 cell limit, which is too small for
        this dataset (475 regions × 3 functions × 187 months = 266k+ rows).
        The Feed API has no cell limit, allowing complete extraction in a
        single query without chunking.
        
        Args:
            start_year: Start year for data extraction (e.g., 2015)
            end_year: End year for data extraction (e.g., 2025)
            save_formats: List of formats to save ('csv', 'parquet')
            
        Returns:
            Tuple of (fact_data, dimensions_dict)
            
        Example:
            >>> extractor = HousingDataExtractor()
            >>> facts, dims = extractor.extract_woningen_pijplijn_feed_api(2015, 2025)
            >>> print(f"Extracted {len(facts):,} rows")
        """
        table_id = PIJPLIJN_CONFIG['table_id']
        
        logger.info("=" * 70)
        logger.info(f"EXTRACTING WONINGEN PIJPLIJN ({table_id}) - FEED API")
        logger.info(f"Period: {start_year}-{end_year}")
        logger.info("Using CBS Feed API (unlimited cells - no chunking needed!)")
        logger.info("=" * 70)
        
        # ────────────────────────────────────────────────────────────
        # Step 1: Extract dimension tables using Standard API (small data)
        # ────────────────────────────────────────────────────────────
        logger.info("Step 1/3: Extracting dimension tables")
        
        dimensions = self.client.get_all_dimensions(table_id)
        
        for dim_name, dim_df in dimensions.items():
            logger.info(f"  ✓ Loaded {dim_name}: {len(dim_df)} rows")
            
            dim_filename = f"dim_{dim_name.lower()}_{table_id}_{self.timestamp}"
            self._save_dataframe(dim_df, dim_filename, save_formats)
        
        # ────────────────────────────────────────────────────────────
        # Step 2: Extract fact data using Feed API (large data - unlimited!)
        # ────────────────────────────────────────────────────────────
        logger.info("Step 2/3: Extracting fact data (Feed API - single query)")
        
        period_start = f"{start_year}MM01"
        period_end = f"{end_year}MM12"
        filters = [f"Perioden ge '{period_start}' and Perioden le '{period_end}'"]
        
        logger.info(f"  • Period filter: {period_start} to {period_end}")
        
        measure_cols = PIJPLIJN_CONFIG['measure_columns']
        dimension_cols = ['ID', 'Gebruiksfunctie', 'RegioS', 'Perioden']
        select_cols = dimension_cols + measure_cols
        
        with CBSAPIClient(api_type='feed') as feed_client:
            fact_data = feed_client.get_data_paginated(
                table_id=table_id,
                filters=filters,
                select=select_cols
            )
        
        logger.info(f"  ✓ Extracted {len(fact_data):,} rows successfully!")
        
        # ────────────────────────────────────────────────────────────
        # Step 3: Data quality checks and save
        # ────────────────────────────────────────────────────────────
        logger.info("Step 3/3: Data quality checks and saving")
        
        logger.info("Data quality summary:")
        logger.info(f"  • Total rows: {len(fact_data):,}")
        logger.info(f"  • Total columns: {len(fact_data.columns)}")
        logger.info(
            f"  • Memory usage: "
            f"{fact_data.memory_usage(deep=True).sum() / 1024**2:.2f} MB"
        )
        
        # Check for nulls
        null_counts = fact_data.isnull().sum()
        if null_counts.sum() > 0:
            logger.warning("Null values detected:")
            for col, count in null_counts[null_counts > 0].items():
                pct = (count / len(fact_data)) * 100
                logger.warning(f"  • {col}: {count} ({pct:.1f}%)")
        
        # Save fact data
        fact_filename = (
            f"fact_woningen_pijplijn_{start_year}_{end_year}_"
            f"{table_id}_{self.timestamp}"
        )
        
        self._save_dataframe(fact_data, fact_filename, save_formats)
        
        logger.info("✓ Woningen Pijplijn (Feed API) extraction complete!")
        
        return fact_data, dimensions
    
    
    def _save_dataframe(
        self,
        df: pd.DataFrame,
        filename: str,
        formats: list
    ):
        """
        Save DataFrame in multiple formats.
        
        Pattern: Utility method for consistent file saving
        
        Args:
            df: DataFrame to save
            filename: Base filename (without extension)
            formats: List of formats ('csv', 'parquet')
        """
        for fmt in formats:
            if fmt == 'csv':
                filepath = RAW_DATA_DIR / f"{filename}.csv"
                df.to_csv(filepath, index=False)
                logger.info(f"  ✓ Saved CSV: {filepath.name}")
                
            elif fmt == 'parquet':
                filepath = RAW_DATA_DIR / f"{filename}.parquet"
                df.to_parquet(filepath, index=False, engine='pyarrow')
                logger.info(f"  ✓ Saved Parquet: {filepath.name}")
                
            else:
                logger.warning(f"Unknown format: {fmt}")
    
    
    def close(self):
        """Close API client (cleanup)."""
        self.client.close()
        logger.info("Extractor closed")
    
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit (auto-cleanup)."""
        self.close()


# ============================================================
def cleanup_old_extractions(keep_last_n: int = 1):
    """
    Clean up old extraction files, keeping only the N most recent EXTRACTIONS.
    
    Pattern: Similar to log rotation in production systems
    Purpose: Prevent disk space waste from duplicate timestamped files
    
    IMPORTANT: Treats CSV + Parquet with SAME timestamp as ONE extraction.
               Keeps or deletes them TOGETHER (not separately).
    
    Args:
        keep_last_n: Number of recent extractions to keep (default: 1)
                    If extraction creates CSV + Parquet, both are kept/deleted together
    
    Example:
        Extraction 1 (timestamp: 205059):
          - file_205059.csv
          - file_205059.parquet
        
        Extraction 2 (timestamp: 205140):
          - file_205140.csv
          - file_205140.parquet
        
        cleanup_old_extractions(keep_last_n=1)
        → Keeps BOTH csv and parquet from Extraction 2
        → Deletes BOTH csv and parquet from Extraction 1
    """
    logger.info(f"Cleaning up old extractions (keeping last {keep_last_n})")
    
    # ────────────────────────────────────────────────────────────
    # Step 1: Find all files and extract their timestamps
    # ────────────────────────────────────────────────────────────
    files_by_timestamp = {}  # {timestamp: [list of files]}
    
    for file in RAW_DATA_DIR.glob('*'):
        if file.suffix not in ['.csv', '.parquet']:
            continue  # Skip non-data files
        
        # Extract timestamp from filename
        # Format: *_YYYYMMDD_HHMMSS.ext
        timestamp = _extract_timestamp(file)
        
        if timestamp:
            if timestamp not in files_by_timestamp:
                files_by_timestamp[timestamp] = []
            
            files_by_timestamp[timestamp].append(file)
    
    if not files_by_timestamp:
        logger.info("✓ No extraction files found (clean start)")
        return
    
    # ────────────────────────────────────────────────────────────
    # Step 2: Sort timestamps (newest first)
    # ────────────────────────────────────────────────────────────
    sorted_timestamps = sorted(files_by_timestamp.keys(), reverse=True)
    
    logger.info(f"Found {len(sorted_timestamps)} extraction(s):")
    for i, ts in enumerate(sorted_timestamps, 1):
        file_count = len(files_by_timestamp[ts])
        logger.info(f"  {i}. Timestamp {ts}: {file_count} files")
    
    # ────────────────────────────────────────────────────────────
    # Step 3: Keep last N extractions, delete the rest
    # ────────────────────────────────────────────────────────────
    if len(sorted_timestamps) <= keep_last_n:
        logger.info(f"✓ Only {len(sorted_timestamps)} extraction(s), nothing to clean")
        return
    
    # Timestamps to keep (newest N)
    timestamps_to_keep = sorted_timestamps[:keep_last_n]
    
    # Timestamps to delete (older ones)
    timestamps_to_delete = sorted_timestamps[keep_last_n:]
    
    logger.info(f"Keeping {len(timestamps_to_keep)} extraction(s): {timestamps_to_keep}")
    logger.info(f"Deleting {len(timestamps_to_delete)} extraction(s): {timestamps_to_delete}")
    
    # Delete all files from old extractions
    total_removed = 0
    
    for ts in timestamps_to_delete:
        for file in files_by_timestamp[ts]:
            logger.info(f"  Removing: {file.name}")
            file.unlink()
            total_removed += 1
    
    if total_removed > 0:
        logger.info(f"✓ Removed {total_removed} old files")
        print(f"🧹 Cleaned up {total_removed} old extraction files")
    else:
        logger.info("✓ No old files to remove")


def _extract_timestamp(file: Path) -> Optional[str]:
    """
    Extract timestamp from file name.
    
    Helper function for cleanup_old_extractions()
    
    Args:
        file: Path to file with timestamp in name
        
    Returns:
        Timestamp string (YYYYMMDD_HHMMSS), or None if not found
        
    Example:
        >>> _extract_timestamp(Path('fact_doorlooptijden_2023_2024_86260NED_20260220_205140.csv'))
        '20260220_205140'
    """
    parts = file.stem.split('_')
    
    # Find timestamp pattern: 8 digits (YYYYMMDD) followed by 6 digits (HHMMSS)
    for i, part in enumerate(parts):
        if len(part) == 8 and part.isdigit():
            if i + 1 < len(parts) and len(parts[i + 1]) == 6 and parts[i + 1].isdigit():
                return f"{part}_{parts[i + 1]}"
    
    return None


# ============================================================
# MAIN EXECUTION
# ============================================================

def main():
    """
    Main extraction workflow.
    
    Pattern: Entry point for extraction process
    Usage:
        python python/extract_cbs_housing.py
    """
    
    print("\n" + "="*70)
    print("CBS HOUSING DATA EXTRACTION")
    print("Project: Dutch Housing Analytics (Rijksoverheid Portfolio)")
    print("="*70 + "\n")
    
    # Configuration
    START_YEAR = 2015  
    END_YEAR = 2025  # Full historical range
    
    print(f"Extraction period: {START_YEAR}-{END_YEAR}")
    print("(Full historical range - may take longer to extract)")
    print()
    
    # Confirm before proceeding
    response = input("Proceed with extraction? (y/n): ")
    if response.lower() != 'y':
        print("Extraction cancelled.")
        return
    
    # Start extraction
    start_time = datetime.now()
    
    try:
        with HousingDataExtractor() as extractor:
            # Extract Dataset 1: Doorlooptijden
            print("\n" + "🏗️ " * 35)
            print("DATASET 1: DOORLOOPTIJDEN")
            print("🏗️ " * 35 + "\n")
            
            facts_1, dims_1 = extractor.extract_doorlooptijden(
                START_YEAR,
                END_YEAR,
                save_formats=['csv', 'parquet']
            )
            
            print(f"\n✓ Dataset 1 complete: {len(facts_1):,} rows extracted")
            
            # Extract Dataset 2 later (after Dataset 1 is verified)
            print("\n" + "🏢 " * 35)
            print("DATASET 2: WONINGEN PIJPLIJN (FEED API)")
            print("🏢 " * 35 + "\n")
             
            facts_2, dims_2 = extractor.extract_woningen_pijplijn_feed_api(
                START_YEAR,
                END_YEAR,
                save_formats=['csv', 'parquet']
            )
             
            print(f"\n✓ Dataset 2 complete: {len(facts_2):,} rows extracted")

        print("\n" + "🧹 " * 35)
        cleanup_old_extractions(keep_last_n=1)
        print("🧹 " * 35)
        
        # Summary
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print("\n" + "="*70)
        print("EXTRACTION COMPLETE!")
        print("="*70)
        print(f"Duration: {duration:.1f} seconds")
        print(f"Data saved to: {RAW_DATA_DIR}")
        print()
        print("Next steps:")
        print("  1. Verify data quality (check CSV files)")
        print("  2. Run transformation (python/transform_housing_data.py)")
        print("="*70 + "\n")
        
    except KeyboardInterrupt:
        logger.warning("Extraction interrupted by user")
        print("\n⚠️  Extraction interrupted!")
        
    except Exception as e:
        logger.error(f"Extraction failed: {str(e)}", exc_info=True)
        print(f"\n❌ Extraction failed: {str(e)}")
        print("Check logs/pipeline.log for details")
        raise


if __name__ == "__main__":
    main()