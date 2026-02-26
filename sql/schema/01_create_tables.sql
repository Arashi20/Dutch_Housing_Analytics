-- Star Schema for Dutch Housing Analytics
-- Database: SQLite

DROP TABLE IF EXISTS fact_doorlooptijden;
DROP TABLE IF EXISTS fact_woningen_pijplijn;
DROP TABLE IF EXISTS dim_regiokenmerken;
DROP TABLE IF EXISTS dim_regios;
DROP TABLE IF EXISTS dim_gebruiksfunctie;
DROP TABLE IF EXISTS dim_woningtype;
DROP TABLE IF EXISTS dim_perioden;

-- Dimension: Regiokenmerken (19 items)
CREATE TABLE dim_regiokenmerken (
    code TEXT PRIMARY KEY,
    naam TEXT NOT NULL,
    type TEXT
);

-- Dimension: Regios (415 gemeentes)
CREATE TABLE dim_regios (
    code TEXT PRIMARY KEY,
    naam TEXT NOT NULL,
    provincie TEXT
);

-- Dimension: Gebruiksfunctie (3 items)
CREATE TABLE dim_gebruiksfunctie (
    code TEXT PRIMARY KEY,
    naam TEXT NOT NULL
);

-- Dimension: Woningtype (3 items)
CREATE TABLE dim_woningtype (
    code TEXT PRIMARY KEY,
    naam TEXT NOT NULL
);

-- Dimension: Perioden (~187 rows)
CREATE TABLE dim_perioden (
    code TEXT PRIMARY KEY,
    naam TEXT NOT NULL,
    jaar INTEGER NOT NULL,
    kwartaal REAL,
    maand REAL
);

-- Fact: Doorlooptijden (9,234 rows)
CREATE TABLE fact_doorlooptijden (
    id INTEGER PRIMARY KEY,
    regiokenmerk_code TEXT NOT NULL,
    gebruiksfunctie_code TEXT NOT NULL,
    woningtype_code TEXT NOT NULL,
    periode_code TEXT NOT NULL,
    jaar INTEGER NOT NULL,
    kwartaal INTEGER NOT NULL,
    doorlooptijd_mediaan REAL,
    doorlooptijd_gemiddelde REAL,
    doorlooptijd_p10 REAL,
    doorlooptijd_p25 REAL,
    doorlooptijd_p75 REAL,
    doorlooptijd_p90 REAL,
    doorlooptijd_iqr REAL,
    doorlooptijd_p10_p90_range REAL,
    doorlooptijd_cv REAL,
    nieuwbouw_aantal INTEGER,
    hoge_variabiliteit INTEGER,
    FOREIGN KEY (regiokenmerk_code) REFERENCES dim_regiokenmerken(code),
    FOREIGN KEY (gebruiksfunctie_code) REFERENCES dim_gebruiksfunctie(code),
    FOREIGN KEY (woningtype_code) REFERENCES dim_woningtype(code),
    FOREIGN KEY (periode_code) REFERENCES dim_perioden(code)
);

-- Fact: Woningen Pijplijn (88,825 rows)
CREATE TABLE fact_woningen_pijplijn (
    id INTEGER PRIMARY KEY,
    regio_code TEXT NOT NULL,
    gebruiksfunctie_code TEXT NOT NULL,
    periode_code TEXT NOT NULL,
    jaar INTEGER NOT NULL,
    maand INTEGER NOT NULL,
    pijplijn_totaal INTEGER,
    pijplijn_bouw_gestart INTEGER,
    pijplijn_vergunning INTEGER,
    pijplijn_vast_2jaar INTEGER,
    pijplijn_bouw_gestart_2jaar INTEGER,
    pijplijn_vergunning_2jaar INTEGER,
    pijplijn_vast_5jaar INTEGER,
    pijplijn_bouw_gestart_5jaar INTEGER,
    pijplijn_vergunning_5jaar INTEGER,
    bottleneck_2jaar_pct REAL,
    bottleneck_5jaar_pct REAL,
    vergunning_bottleneck_pct REAL,
    bouw_bottleneck_pct REAL,
    vergunning_fase_pct REAL,
    bouw_fase_pct REAL,
    crisis_regio INTEGER,
    FOREIGN KEY (regio_code) REFERENCES dim_regios(code),
    FOREIGN KEY (gebruiksfunctie_code) REFERENCES dim_gebruiksfunctie(code),
    FOREIGN KEY (periode_code) REFERENCES dim_perioden(code)
);