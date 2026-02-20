"""
CBS OData API Client
====================
Reusable client for interacting with CBS StatLine OData API.

Purpose: Abstract away API complexity, provide clean interface
Reference: https://www.cbs.nl/nl-nl/onze-diensten/open-data

Author: Arashi20
Date: 2026-02-19
Project: Dutch Housing Analytics - Rijksoverheid Portfolio
"""

import requests
import pandas as pd
import time
from typing import Dict, List, Optional, Union
from urllib.parse import urljoin

from config import (
    CBS_API_BASE_URL,
    ODATA_CONFIG,
    get_logger
)

# Initialize logger
logger = get_logger(__name__)


class CBSAPIClient:
    """
    Client for CBS StatLine OData API.
    
    Pattern: Similar to base API clients in stock-research-MAS
    Features:
    - Automatic retry with exponential backoff
    - Pagination handling for large datasets
    - Rate limiting protection
    - Comprehensive error handling
    - Logging of all operations
    
    Usage:
        client = CBSAPIClient()
        data = client.get_data('86260NED', filters=["Perioden eq '2023KW01'"])
    """
    
    def __init__(
        self,
        base_url: str = CBS_API_BASE_URL,
        timeout: int = ODATA_CONFIG['timeout'],
        max_retries: int = ODATA_CONFIG['max_retries'],
        batch_size: int = ODATA_CONFIG['batch_size']
    ):
        """
        Initialize CBS API client.
        
        Args:
            base_url: Base URL for CBS OData API
            timeout: Request timeout in seconds
            max_retries: Maximum number of retry attempts
            batch_size: Number of records per page (pagination)
        """
        self.base_url = base_url
        self.timeout = timeout
        self.max_retries = max_retries
        self.batch_size = batch_size
        
        # Session for connection pooling (performance)
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': 'application/json',
            'User-Agent': 'Dutch-Housing-Analytics/1.0 (github.com/Arashi20)'
        })
        
        logger.info(f"CBS API Client initialized: {base_url}")
    
    
    def _make_request(
        self,
        url: str,
        params: Optional[Dict] = None,
        retry_count: int = 0
    ) -> Dict:
        """
        Make HTTP GET request with retry logic.
        
        Pattern: Similar to retry logic in stock-research-MAS agent_base.py
        
        Args:
            url: Full URL to request
            params: Query parameters
            retry_count: Current retry attempt number
            
        Returns:
            JSON response as dictionary
            
        Raises:
            requests.exceptions.RequestException: If all retries fail
        """
        try:
            logger.debug(f"GET {url} (params: {params})")
            
            response = self.session.get(
                url,
                params=params,
                timeout=self.timeout
            )
            response.raise_for_status()
            
            return response.json()
            
        except requests.exceptions.Timeout:
            logger.warning(f"Request timeout (attempt {retry_count + 1}/{self.max_retries})")
            
            if retry_count < self.max_retries:
                wait_time = ODATA_CONFIG['retry_delay_base'] ** retry_count
                logger.info(f"Retrying in {wait_time}s...")
                time.sleep(wait_time)
                return self._make_request(url, params, retry_count + 1)
            else:
                logger.error(f"Max retries reached for {url}")
                raise
                
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:  # Rate limited
                logger.warning("Rate limited by CBS API (429)")
                time.sleep(ODATA_CONFIG['rate_limit_delay'])
                
                if retry_count < self.max_retries:
                    return self._make_request(url, params, retry_count + 1)
                else:
                    raise
                    
            elif e.response.status_code >= 500:  # Server error
                logger.warning(f"Server error {e.response.status_code} (attempt {retry_count + 1})")
                
                if retry_count < self.max_retries:
                    wait_time = ODATA_CONFIG['retry_delay_base'] ** retry_count
                    time.sleep(wait_time)
                    return self._make_request(url, params, retry_count + 1)
                else:
                    raise
            else:
                # Client error (4xx) - don't retry
                logger.error(f"HTTP {e.response.status_code}: {e.response.text[:200]}")
                raise
                
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection error: {str(e)}")
            
            if retry_count < self.max_retries:
                wait_time = ODATA_CONFIG['retry_delay_base'] ** retry_count
                time.sleep(wait_time)
                return self._make_request(url, params, retry_count + 1)
            else:
                raise
    
    
    def get_table_info(self, table_id: str) -> Dict:
        """
        Get metadata about a CBS table.
        
        Args:
            table_id: CBS table ID (e.g., '86260NED')
            
        Returns:
            Dictionary with table metadata including available endpoints
            
        Example:
            >>> client = CBSAPIClient()
            >>> info = client.get_table_info('86260NED')
            >>> print(info['value'])  # List of available endpoints
        """
        url = urljoin(self.base_url + '/', table_id)
        
        logger.info(f"Fetching table info for {table_id}")
        
        data = self._make_request(url)
        
        # Extract endpoint names
        endpoints = [item['name'] for item in data.get('value', [])]
        logger.info(f"Found {len(endpoints)} endpoints: {endpoints}")
        
        return data
    
    
    def get_dimension(
        self,
        table_id: str,
        dimension_name: str
    ) -> pd.DataFrame:
        """
        Get dimension table (lookup table).
        
        Pattern: Similar to reference data fetching in your other projects
        
        Args:
            table_id: CBS table ID
            dimension_name: Name of dimension (e.g., 'RegioS', 'Perioden')
            
        Returns:
            DataFrame with dimension data
            
        Example:
            >>> client = CBSAPIClient()
            >>> regions = client.get_dimension('86260NED', 'Regiokenmerken')
            >>> print(regions[['Key', 'Title']].head())
        """
        url = urljoin(self.base_url + '/', f"{table_id}/{dimension_name}")
        
        logger.info(f"Fetching dimension: {dimension_name} from {table_id}")
        
        data = self._make_request(url, params={'$format': 'json'})
        
        if 'value' not in data:
            logger.warning(f"No 'value' key in response for {dimension_name}")
            return pd.DataFrame()
        
        df = pd.DataFrame(data['value'])
        
        logger.info(f"Fetched {len(df)} rows from {dimension_name}")
        
        return df
    
    
    def get_data(
        self,
        table_id: str,
        filters: Optional[List[str]] = None,
        select: Optional[List[str]] = None,
        top: Optional[int] = None,
        skip: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Get fact data from TypedDataSet endpoint.
        
        Pattern: Main data extraction method (like your Workout_Data_Pipeline)
        
        Args:
            table_id: CBS table ID
            filters: List of OData filter expressions
                    Example: ["Perioden ge '2023KW01'", "RegioS eq 'PV27'"]
            select: List of columns to retrieve (None = all columns)
            top: Limit number of rows (for testing)
            skip: Skip number of rows (for pagination)
            
        Returns:
            DataFrame with fact data
            
        Example:
            >>> client = CBSAPIClient()
            >>> data = client.get_data(
            ...     '86260NED',
            ...     filters=["Perioden ge '2023KW01'"],
            ...     top=1000
            ... )
        """
        url = urljoin(self.base_url + '/', f"{table_id}/TypedDataSet")
        
        # Build query parameters
        params = {'$format': 'json'}
        
        if filters:
            # Join multiple filters with 'and'
            filter_str = ' and '.join(filters)
            params['$filter'] = filter_str
            logger.info(f"Applying filters: {filter_str}")
        
        if select:
            params['$select'] = ','.join(select)
        
        if top:
            params['$top'] = top
        
        if skip:
            params['$skip'] = skip
        
        logger.info(f"Fetching data from {table_id}/TypedDataSet")
        
        data = self._make_request(url, params)
        
        if 'value' not in data:
            logger.warning("No 'value' key in response")
            return pd.DataFrame()
        
        df = pd.DataFrame(data['value'])
        
        logger.info(f"Fetched {len(df)} rows, {len(df.columns)} columns")
        
        return df
    
    
    def get_data_paginated(
        self,
        table_id: str,
        filters: Optional[List[str]] = None,
        select: Optional[List[str]] = None,
        max_rows: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Get fact data with automatic pagination.
        
        Pattern: Handles large datasets by fetching in batches
        Similar to: Pagination in stock-research-MAS data fetching
        
        Args:
            table_id: CBS table ID
            filters: List of OData filter expressions
            select: List of columns to retrieve
            max_rows: Maximum total rows to fetch (None = all available)
            
        Returns:
            DataFrame with all fetched data
            
        Example:
            >>> client = CBSAPIClient()
            >>> # Fetch all 2023-2024 data (auto-paginated)
            >>> data = client.get_data_paginated(
            ...     '86260NED',
            ...     filters=["Perioden ge '2023KW01'"]
            ... )
        """
        all_data = []
        skip = 0
        total_fetched = 0
        
        logger.info(f"Starting paginated fetch from {table_id}")
        logger.info(f"Batch size: {self.batch_size}, Max rows: {max_rows or 'unlimited'}")
        
        while True:
            # Determine how many rows to fetch this iteration
            if max_rows:
                remaining = max_rows - total_fetched
                rows_to_fetch = min(self.batch_size, remaining)
                
                if rows_to_fetch <= 0:
                    logger.info("Reached max_rows limit")
                    break
            else:
                rows_to_fetch = self.batch_size
            
            # Fetch batch
            batch_df = self.get_data(
                table_id=table_id,
                filters=filters,
                select=select,
                top=rows_to_fetch,
                skip=skip
            )
            
            if batch_df.empty:
                logger.info("No more data available")
                break
            
            all_data.append(batch_df)
            total_fetched += len(batch_df)
            skip += len(batch_df)
            
            logger.info(f"Progress: {total_fetched} rows fetched")
            
            # If we got fewer rows than requested, we've reached the end
            if len(batch_df) < rows_to_fetch:
                logger.info("Reached end of available data")
                break
        
        if not all_data:
            logger.warning("No data fetched")
            return pd.DataFrame()
        
        # Combine all batches
        result_df = pd.concat(all_data, ignore_index=True)
        
        logger.info(f"Pagination complete: {len(result_df)} total rows")
        
        return result_df
    
    
    def get_all_dimensions(self, table_id: str) -> Dict[str, pd.DataFrame]:
        """
        Fetch all dimension tables for a given CBS table.
        
        Pattern: Batch operation for convenience
        
        Args:
            table_id: CBS table ID
            
        Returns:
            Dictionary mapping dimension names to DataFrames
            
        Example:
            >>> client = CBSAPIClient()
            >>> dims = client.get_all_dimensions('86260NED')
            >>> print(dims.keys())
            dict_keys(['Regiokenmerken', 'Gebruiksfunctie', 'Woningtype', 'Perioden'])
        """
        logger.info(f"Fetching all dimensions for {table_id}")
        
        # Get table info to find available dimensions
        table_info = self.get_table_info(table_id)
        
        # Filter for dimension endpoints (exclude metadata endpoints)
        excluded = ['TableInfos', 'UntypedDataSet', 'TypedDataSet', 
                   'DataProperties', 'CategoryGroups']
        
        dimension_names = [
            item['name'] for item in table_info.get('value', [])
            if item['name'] not in excluded
        ]
        
        logger.info(f"Found {len(dimension_names)} dimensions: {dimension_names}")
        
        # Fetch each dimension
        dimensions = {}
        for dim_name in dimension_names:
            try:
                df = self.get_dimension(table_id, dim_name)
                dimensions[dim_name] = df
            except Exception as e:
                logger.error(f"Failed to fetch {dim_name}: {str(e)}")
                dimensions[dim_name] = pd.DataFrame()
        
        logger.info(f"Successfully fetched {len(dimensions)} dimension tables")
        
        return dimensions
    
    
    def close(self):
        """Close the session (cleanup)."""
        self.session.close()
        logger.info("CBS API Client session closed")
    
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit (auto-cleanup)."""
        self.close()


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def build_period_filter(start_year: int, end_year: int, granularity: str = 'quarter') -> str:
    """
    Build OData filter for period range.
    
    Pattern: Helper function for common filtering scenario
    
    Args:
        start_year: Start year (inclusive)
        end_year: End year (inclusive)
        granularity: 'quarter' or 'month' or 'year'
        
    Returns:
        OData filter string
        
    Example:
        >>> filter_str = build_period_filter(2023, 2024, 'quarter')
        >>> print(filter_str)
        "Perioden ge '2023KW01' and Perioden le '2024KW04'"
    """
    if granularity == 'quarter':
        start_period = f"{start_year}KW01"
        end_period = f"{end_year}KW04"
    elif granularity == 'month':
        start_period = f"{start_year}MM01"
        end_period = f"{end_year}MM12"
    elif granularity == 'year':
        start_period = f"{start_year}JJ00"
        end_period = f"{end_year}JJ00"
    else:
        raise ValueError(f"Invalid granularity: {granularity}")
    
    return f"Perioden ge '{start_period}' and Perioden le '{end_period}'"


# ============================================================
# MAIN (for testing)
# ============================================================

if __name__ == "__main__":
    """
    Test the CBS API client.
    
    Usage:
        python python/cbs_api_client.py
    """
    
    print("\n" + "="*60)
    print("CBS API CLIENT - TEST RUN")
    print("="*60 + "\n")
    
    # Test with context manager (auto-cleanup)
    with CBSAPIClient() as client:
        # Test 1: Get table info
        print("TEST 1: Get table info")
        info = client.get_table_info('86260NED')
        endpoints = [item['name'] for item in info['value']]
        print(f"✓ Found {len(endpoints)} endpoints")
        
        # Test 2: Get dimension
        print("\nTEST 2: Get Regiokenmerken dimension")
        regions = client.get_dimension('86260NED', 'Regiokenmerken')
        print(f"✓ Fetched {len(regions)} regions")
        print(f"  Sample: {regions['Title'].head(3).tolist()}")
        
        # Test 3: Get sample data
        print("\nTEST 3: Get sample data (top 5)")
        data = client.get_data('86260NED', top=5)
        print(f"✓ Fetched {len(data)} rows, {len(data.columns)} columns")
        print(f"  Columns: {list(data.columns)[:5]}...")
        
        # Test 4: Get data with filter
        print("\nTEST 4: Get 2023 data with filter")
        filter_str = build_period_filter(2023, 2023, 'quarter')
        print(f"  Filter: {filter_str}")
        data_2023 = client.get_data('86260NED', filters=[filter_str], top=10)
        print(f"✓ Fetched {len(data_2023)} rows")
    
    print("\n" + "="*60)
    print("✓ All tests completed!")
    print("="*60 + "\n")