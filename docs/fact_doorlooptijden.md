# fact_doorlooptijden

Doorlooptijden van nieuwbouw (vergunningverlening → oplevering), per kwartaal 2015-2025.

**Bron:** CBS 86260NED | **Rijen:** 9.234 | **Granulariteit:** Per kwartaal

---

## Kolommen

| # | Kolomnaam | Type | Omschrijving |
|---|-----------|------|--------------|
| 1 | `id` | INTEGER | Primary key |
| 2 | `regiokenmerk_code` | TEXT | FK → dim_regiokenmerken.code |
| 3 | `gebruiksfunctie_code` | TEXT | FK → dim_gebruiksfunctie.code |
| 4 | `woningtype_code` | TEXT | FK → dim_woningtype.code |
| 5 | `periode_code` | TEXT | FK → dim_perioden.code (bijv. "2025KW01") |
| 6 | `jaar` | INTEGER | Jaar (2015–2025) |
| 7 | `kwartaal` | INTEGER | Kwartaal (1–4) |
| 8 | `doorlooptijd_mediaan` | REAL | Mediaan doorlooptijd in maanden ⭐ |
| 9 | `doorlooptijd_gemiddelde` | REAL | Gemiddelde doorlooptijd in maanden |
| 10 | `doorlooptijd_p10` | REAL | 10e percentiel (snelle projecten) |
| 11 | `doorlooptijd_p25` | REAL | 25e percentiel |
| 12 | `doorlooptijd_p75` | REAL | 75e percentiel |
| 13 | `doorlooptijd_p90` | REAL | 90e percentiel (trage projecten) |
| 14 | `doorlooptijd_iqr` | REAL | Interkwartielafstand (P75-P25) |
| 15 | `doorlooptijd_p10_p90_range` | REAL | Spreiding P90-P10 |
| 16 | `doorlooptijd_cv` | REAL | Variatiecoëfficiënt |
| 17 | `nieuwbouw_aantal` | REAL | Aantal nieuwbouwwoningen |
| 18 | `hoge_variabiliteit` | INTEGER | 1 = hoge spreiding, 0 = normaal |

---

## Dimensie filterwaarden (voor Power BI)

### regiokenmerk_code → dim_regiokenmerken
| code | naam |
|------|------|
| `NL01` | Nederland ⭐ (gebruik voor nationale KPIs) |
| `PV20` t/m `PV31` | Provincies (Noord-Holland, Utrecht, etc.) |
| `1018850` t/m `1019055` | Stedelijkheidsgraden |

### gebruiksfunctie_code → dim_gebruiksfunctie
| code | naam |
|------|------|
| `A045364` | Woning totaal ⭐ |
| `T001419` | Woning en niet-woning totaal |

### woningtype_code → dim_woningtype
| code | naam |
|------|------|
| `ZW10290` | Eengezinswoning |
| `ZW10340` | Meergezinswoning |
| `T001419` | Totaal ⭐ |