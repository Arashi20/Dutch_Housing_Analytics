-- Performance indexes for fact tables

CREATE INDEX IF NOT EXISTS idx_doorloop_jaar ON fact_doorlooptijden(jaar);
CREATE INDEX IF NOT EXISTS idx_doorloop_regio ON fact_doorlooptijden(regiokenmerk_code);
CREATE INDEX IF NOT EXISTS idx_doorloop_jaar_regio ON fact_doorlooptijden(jaar, regiokenmerk_code);
CREATE INDEX IF NOT EXISTS idx_doorloop_woningtype ON fact_doorlooptijden(woningtype_code);

CREATE INDEX IF NOT EXISTS idx_pijplijn_jaar ON fact_woningen_pijplijn(jaar);
CREATE INDEX IF NOT EXISTS idx_pijplijn_regio ON fact_woningen_pijplijn(regio_code);
CREATE INDEX IF NOT EXISTS idx_pijplijn_jaar_regio ON fact_woningen_pijplijn(jaar, regio_code);
CREATE INDEX IF NOT EXISTS idx_pijplijn_jaar_maand ON fact_woningen_pijplijn(jaar, maand);
CREATE INDEX IF NOT EXISTS idx_pijplijn_bottleneck ON fact_woningen_pijplijn(crisis_regio, bottleneck_2jaar_pct);