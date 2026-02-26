-- Sample SQL Queries for Dutch Housing Analytics
-- Database: housing_analytics.db (SQLite)
-- Star Schema: 2 fact tables + 5 dimension tables

-- ==============================================================================
-- BASIC QUERIES (Getting Started)
-- ==============================================================================

-- Query 1: Count rows in each table
SELECT 'fact_doorlooptijden' as table_name, COUNT(*) as row_count 
FROM fact_doorlooptijden
UNION ALL
SELECT 'fact_woningen_pijplijn', COUNT(*) 
FROM fact_woningen_pijplijn
UNION ALL
SELECT 'dim_regiokenmerken', COUNT(*) 
FROM dim_regiokenmerken
UNION ALL
SELECT 'dim_regios', COUNT(*) 
FROM dim_regios
UNION ALL
SELECT 'dim_gebruiksfunctie', COUNT(*) 
FROM dim_gebruiksfunctie
UNION ALL
SELECT 'dim_woningtype', COUNT(*) 
FROM dim_woningtype
UNION ALL
SELECT 'dim_perioden', COUNT(*) 
FROM dim_perioden;

-- Query 2: Inspect dimension tables
SELECT * FROM dim_regiokenmerken LIMIT 10;
SELECT * FROM dim_regios LIMIT 20;
SELECT * FROM dim_woningtype;
SELECT * FROM dim_gebruiksfunctie;

-- Query 3: Check date range
SELECT 
    MIN(jaar) as start_jaar,
    MAX(jaar) as end_jaar,
    COUNT(DISTINCT jaar) as aantal_jaren
FROM fact_doorlooptijden;
