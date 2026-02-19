"""
CBS API Test - Test Both Datasets
===================================
Tests if we can connect to CBS and if dimension names match.

Pattern: Similar to API validation in stock-research-MAS
Consistent with: config.py structure (uses same table IDs)

Author: Arashi20
Date: 2026-02-18
"""

import requests
from config import CBS_API_BASE_URL, CBS_TABLES

def print_section_header(title: str):
    """Print a nice section header."""
    print("\n" + "=" * 80)
    print(f" {title}")
    print("=" * 80 + "\n")


def test_table_connection(table_name: str, table_id: str):
    """
    Test if we can connect to a specific CBS table.
    
    Pattern: Similar to connection testing in stock-research-MAS
    
    Args:
        table_name: Human-readable name (e.g., 'Doorlooptijden')
        table_id: CBS table ID (e.g., '86260NED')
        
    Returns:
        (success: bool, endpoints: list)
    """
    print_section_header(f"Testing Table: {table_name} ({table_id})")
    
    url = f"{CBS_API_BASE_URL}/{table_id}"
    
    print(f"🌐 URL: {url}")
    print(f"⏳ Making request...\n")
    
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        data = response.json()
        
        print(f"✅ SUCCESS - CBS API is reachable!")
        print(f"   HTTP Status: {response.status_code}")
        
        # Extract available endpoints
        if 'value' in data:
            endpoints = [item['name'] for item in data['value']]
            print(f"\n📊 Found {len(endpoints)} endpoints:")
            for endpoint in endpoints:
                print(f"      • {endpoint}")
            
            return True, endpoints
        else:
            print("⚠️  Unexpected response structure")
            return False, []
            
    except requests.exceptions.Timeout:
        print("❌ FAILED - Request timed out")
        return False, []
        
    except requests.exceptions.ConnectionError:
        print("❌ FAILED - Could not connect")
        return False, []
        
    except Exception as e:
        print(f"❌ FAILED - Error: {str(e)}")
        return False, []


def test_sample_data(table_name: str, table_id: str, sample_size: int = 3):
    """
    Test if we can retrieve sample data from a CBS table.
    
    Pattern: Similar to data validation in Workout_Data_Pipeline
    
    Args:
        table_name: Human-readable name
        table_id: CBS table ID
        sample_size: Number of rows to retrieve
        
    Returns:
        (success: bool, columns: list)
    """
    print_section_header(f"Sample Data from {table_name}")
    
    url = f"{CBS_API_BASE_URL}/{table_id}/TypedDataSet"
    params = {'$format': 'json', '$top': sample_size}
    
    print(f"🔍 Requesting {sample_size} sample rows...")
    print(f"   URL: {url}\n")
    
    try:
        response = requests.get(url, params=params, timeout=20)
        response.raise_for_status()
        data = response.json()
        
        if 'value' not in data:
            print("❌ No data returned")
            return False, []
        
        rows = data['value']
        print(f"✅ SUCCESS - Got {len(rows)} rows\n")
        
        if not rows:
            print("⚠️  Data is empty")
            return True, []
        
        # Show structure of first row
        first_row = rows[0]
        columns = list(first_row.keys())
        
        print(f"📊 Data has {len(columns)} columns:\n")
        
        # Categorize columns for better understanding
        # Pattern: Similar to data exploration in your Workout_Data_Pipeline
        dimension_cols = []
        measure_cols = []
        
        for col in columns:
            # Check if column looks like a dimension (short code-like values)
            if col in ['ID', 'Regiokenmerken', 'RegioS', 'Perioden', 
                      'Onderwerpen', 'Gebruiksfunctie', 'Woningtype',
                      'BouwstroomVanTotEnMet']:
                dimension_cols.append(col)
            # Check if column looks like a measure (has _1, _2 or looks numeric)
            elif any(x in col for x in ['_1', '_2', '_3', '_4', '_5', '_6', '_7',
                                        'Maanden', 'Aantal', 'Totaal']):
                measure_cols.append(col)
        
        # Display dimension columns
        if dimension_cols:
            print("   📌 Dimension Columns (link to lookup tables):")
            for col in dimension_cols:
                value = str(first_row[col])[:40]
                print(f"      • {col:35s} = {value}")
        
        # Display measure columns
        if measure_cols:
            print(f"\n   📊 Measure Columns (the actual data):")
            for col in measure_cols:
                value = str(first_row[col])[:40]
                print(f"      • {col:35s} = {value}")
        
        print(f"\n   Total: {len(dimension_cols)} dimensions + {len(measure_cols)} measures")
        
        return True, columns
        
    except Exception as e:
        print(f"❌ FAILED - {str(e)}")
        return False, []


def analyze_dimensions(endpoints: list, expected_dims: list):
    """
    Analyze which expected dimensions exist in CBS table.
    
    Pattern: Validation similar to DATA_QUALITY_RULES checks in config.py
    
    Args:
        endpoints: List of actual endpoints from CBS API
        expected_dims: List of dimension names we expect
        
    Returns:
        (all_found: bool, missing: list, found: list)
    """
    print(f"\n🔍 Dimension Analysis:\n")
    
    print("Expected dimensions (from README/config):")
    for dim in expected_dims:
        print(f"   • {dim}")
    
    print("\nVerification against CBS API:")
    
    found = []
    missing = []
    
    for dim in expected_dims:
        if dim in endpoints:
            print(f"   ✅ '{dim}' → FOUND")
            found.append(dim)
        else:
            print(f"   ❌ '{dim}' → NOT FOUND")
            missing.append(dim)
            
            # Suggest similar names (case-insensitive search)
            # Pattern: Similar to error suggestion in stock-research-MAS
            similar = [e for e in endpoints 
                      if dim.lower() in e.lower() or e.lower() in dim.lower()]
            if similar:
                print(f"      💡 Did you mean: {similar[0]}?")
    
    all_found = len(missing) == 0
    
    if all_found:
        print(f"\n✅ All {len(expected_dims)} dimensions found!")
    else:
        print(f"\n⚠️  {len(missing)} dimension(s) not found: {missing}")
        print(f"   Found: {found}")
    
    return all_found, missing, found


# ============================================================
# MAIN TEST EXECUTION
# ============================================================

if __name__ == "__main__":
    """
    Run comprehensive CBS API tests for both datasets.
    
    Pattern: Similar to test execution in stock-research-MAS test suites
    """
    
    print("""
╔════════════════════════════════════════════════════════════════════════╗
║                                                                        ║
║           CBS API Test - Both Datasets                                 ║
║                                                                        ║
║  Tests:                                                                ║
║    1. Dataset 1: Doorlooptijden (86260NED)                            ║
║    2. Dataset 2: Woningen in Pijplijn (82211NED)                     ║
║                                                                        ║
╚════════════════════════════════════════════════════════════════════════╝

This will test:
  ✓ Connection to CBS API
  ✓ Dimension name verification
  ✓ Sample data retrieval
  ✓ Data structure analysis

Press Enter to start...
    """)
    
    input()  # Wait for user
    
    # Track results
    results = {}
    
    # ================================================================
    # DATASET 1: DOORLOOPTIJDEN (86260NED)
    # ================================================================
    table_name_1 = 'Doorlooptijden Nieuwbouw'
    table_id_1 = CBS_TABLES['doorlooptijden']  # '86260NED'
    
    # Test 1.1: Connection & Endpoints
    success_1, endpoints_1 = test_table_connection(table_name_1, table_id_1)
    
    if success_1:
        # Test 1.2: Dimension Verification
        # From README: Regiokenmerken, Gebruiksfunctie, Woningtype, Perioden
        # Note: "Onderwerpen" is NOT a dimension - measures are columns!
        expected_dims_1 = [
            'Regiokenmerken',    # NOT 'RegioS'!
            'Gebruiksfunctie',
            'Woningtype',
            'Perioden',
        ]
        
        all_found_1, missing_1, found_1 = analyze_dimensions(endpoints_1, expected_dims_1)
        
        # Test 1.3: Sample Data
        success_data_1, columns_1 = test_sample_data(table_name_1, table_id_1)
        
        results['doorlooptijden'] = {
            'connection': success_1,
            'all_dims_found': all_found_1,
            'missing_dims': missing_1,
            'found_dims': found_1,
            'sample_data': success_data_1,
            'columns': columns_1
        }
    else:
        print("\n❌ Cannot continue tests for Dataset 1 - connection failed")
        results['doorlooptijden'] = {'connection': False}
    
    # ================================================================
    # DATASET 2: WONINGEN IN PIJPLIJN (82211NED)
    # ================================================================
    # Pattern: Same test structure as Dataset 1 (consistency!)
    # Reference: Uses same functions as doorlooptijden test above
    # Verified: 2026-02-18 - Both datasets have similar structure
    print("\n" + "🔄" * 40)
    input("\nPress Enter to test Dataset 2...")
    
    table_name_2 = 'Woningen in de Pijplijn'
    table_id_2 = CBS_TABLES['woningen_pijplijn']  # '82211NED'
    
    # Test 2.1: Connection & Endpoints
    # Pattern: Same as test_table_connection() for Dataset 1
    success_2, endpoints_2 = test_table_connection(table_name_2, table_id_2)
    
    if success_2:
        # Test 2.2: Dimension Verification
        # Pattern: Same as Dataset 1 - verify actual dimension names
        # Reference: Dataset 1 showed "Onderwerpen" is NOT a dimension
        #            Same applies here - measures are COLUMNS!
        #
        # From CBS API test results (verified 2026-02-18):
        #   Actual dimensions: Gebruiksfunctie, RegioS, Perioden
        #   Measures: VerblijfsobjectenInDePijplijnTotaal_1, etc. (7 columns)
        #
        # Note: Dataset 2 uses 'RegioS' (not 'Regiokenmerken' like Dataset 1)
        #       CBS has inconsistent naming across tables!
        expected_dims_2 = [
            'Gebruiksfunctie',     # ✓ Exists (same as Dataset 1)
            'RegioS',              # ✓ Exists (different name than Dataset 1!)
            'Perioden',            # ✓ Exists (same as Dataset 1)
            # Note: 'Onderwerpen' removed from expected list
            #       Reason: Not a dimension - measures are columns
            #       Same pattern as doorlooptijden (86260NED)
        ]
        
        all_found_2, missing_2, found_2 = analyze_dimensions(endpoints_2, expected_dims_2)
        
        # Test 2.3: Sample Data
        # Pattern: Same as test_sample_data() for Dataset 1
        # Purpose: Show actual column structure and data types
        success_data_2, columns_2 = test_sample_data(table_name_2, table_id_2)
        
        results['woningen_pijplijn'] = {
            'connection': success_2,
            'all_dims_found': all_found_2,
            'missing_dims': missing_2,
            'found_dims': found_2,
            'sample_data': success_data_2,
            'columns': columns_2
        }
    else:
        print("\n❌ Cannot continue tests for Dataset 2 - connection failed")
        results['woningen_pijplijn'] = {'connection': False}
    
    # ================================================================
    # FINAL SUMMARY
    # ================================================================
    print("\n" + "=" * 80)
    print(" 📊 COMPREHENSIVE TEST SUMMARY")
    print("=" * 80 + "\n")
    
    # Summary for Dataset 1
    if 'doorlooptijden' in results and results['doorlooptijden']['connection']:
        r1 = results['doorlooptijden']
        print("📋 Dataset 1: Doorlooptijden (86260NED)")
        print(f"   Connection:     ✅ Success")
        print(f"   Dimensions:     {'✅ All found' if r1['all_dims_found'] else '⚠️  Some missing'}")
        if r1['missing_dims']:
            print(f"      Missing: {r1['missing_dims']}")
        print(f"   Sample Data:    {'✅ Success' if r1['sample_data'] else '❌ Failed'}")
        print(f"   Total Columns:  {len(r1['columns'])}")
    else:
        print("📋 Dataset 1: Doorlooptijden (86260NED)")
        print("   ❌ Connection failed")
    
    print()
    
    # Summary for Dataset 2
    if 'woningen_pijplijn' in results and results['woningen_pijplijn']['connection']:
        r2 = results['woningen_pijplijn']
        print("📋 Dataset 2: Woningen in Pijplijn (82211NED)")
        print(f"   Connection:     ✅ Success")
        print(f"   Dimensions:     {'✅ All found' if r2['all_dims_found'] else '⚠️  Some missing'}")
        if r2['missing_dims']:
            print(f"      Missing: {r2['missing_dims']}")
        print(f"   Sample Data:    {'✅ Success' if r2['sample_data'] else '❌ Failed'}")
        print(f"   Total Columns:  {len(r2['columns'])}")
    else:
        print("📋 Dataset 2: Woningen in Pijplijn (82211NED)")
        print("   ❌ Connection failed or not tested")
    
    print("\n" + "=" * 80)
    print("\n💡 Next Steps:")
    print("   1. Review dimension mismatches above")
    print("   2. Note correct dimension names for each table")
    print("   3. Update config.py with actual CBS API names")
    print("   4. Build extraction code using verified names")
    print("\n" + "=" * 80 + "\n")