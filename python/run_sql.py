"""
Run SQL queries from a file against the housing analytics database.

Usage:
    python python/run_sql.py sql/03_sample_queries.sql
"""

import sqlite3
import sys
from pathlib import Path

def run_sql_file(db_path: str, sql_file_path: str):
    """Execute SQL queries from a file."""
    
    # Check if files exist
    if not Path(db_path).exists():
        print(f"❌ Database not found: {db_path}")
        print("💡 Run extraction and transformation first:")
        print("   python python/extract_cbs_housing.py")
        print("   python python/transform_housing_data.py")
        return
    
    if not Path(sql_file_path).exists():
        print(f"❌ SQL file not found: {sql_file_path}")
        return
    
    # Read SQL file
    print(f"📖 Reading SQL from: {sql_file_path}")
    with open(sql_file_path, 'r', encoding='utf-8') as f:
        sql_content = f.read()
    
    # Connect to database
    print(f"🔗 Connecting to database: {db_path}")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Split queries (basic split by semicolon)
    queries = [q.strip() for q in sql_content.split(';') if q.strip()]
    
    print(f"\n🚀 Executing {len(queries)} queries...\n")
    
    # Execute each query
    for i, query in enumerate(queries, 1):
        # Skip comments-only queries
        if query.strip().startswith('--') or not query.strip():
            continue
        
        # Extract query description from comments
        query_lines = query.strip().split('\n')
        description = None
        for line in query_lines:
            if line.strip().startswith('-- Query'):
                description = line.strip('- ').strip()
                break
        
        try:
            if description:
                print(f"[{i}/{len(queries)}] {description}")
            
            cursor.execute(query)
            
            # Fetch results if it's a SELECT query
            if query.strip().upper().startswith('SELECT'):
                results = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                
                # Print results (first 10 rows)
                if results:
                    print(f"   ✅ Returned {len(results)} rows")
                    print(f"   Columns: {', '.join(columns)}")
                    
                    # Print first few rows
                    if len(results) <= 5:
                        for row in results:
                            print(f"   {row}")
                    else:
                        print(f"   First 3 rows:")
                        for row in results[:3]:
                            print(f"   {row}")
                        print(f"   ... ({len(results) - 3} more rows)")
                else:
                    print(f"   ⚠️  No results returned")
            else:
                # For non-SELECT queries (INSERT, UPDATE, etc.)
                conn.commit()
                print(f"   ✅ Executed successfully")
            
            print()  # Blank line between queries
            
        except sqlite3.Error as e:
            print(f"   ❌ Error: {e}")
            print(f"   Query preview: {query[:100]}...")
            print()
    
    # Close connection
    conn.close()
    print("✅ All queries executed!")
    print(f"🔒 Database connection closed")

if __name__ == "__main__":
    # Default paths
    DB_PATH = "data/housing_analytics.db"
    
    # Get SQL file from command line argument
    if len(sys.argv) < 2:
        print("❌ Usage: python python/run_sql.py <sql_file>")
        print("\nExample:")
        print("  python python/run_sql.py sql/03_sample_queries.sql")
        sys.exit(1)
    
    sql_file = sys.argv[1]
    
    # Run SQL file
    run_sql_file(DB_PATH, sql_file)