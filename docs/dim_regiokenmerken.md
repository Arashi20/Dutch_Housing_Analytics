# dim_regiokenmerken

Regiokenmerken dimensietabel voor `fact_doorlooptijden`. Bevat Nederland totaal, 12 provincies, en 6 stedelijkheidsgraden.

**Bron:** CBS 86260NED | **Rijen:** 19

---

## Kolommen

| # | Kolomnaam | Type | Omschrijving |
|---|-----------|------|--------------|
| 1 | `code` | TEXT | Primary key (bijv. "NL01", "PV27") |
| 2 | `naam` | TEXT | Leesbare naam (bijv. "Nederland", "Noord-Holland (PV)") |
| 3 | `type` | TEXT | Altijd "regio" |

---

## Alle waarden

| code | naam |
|------|------|
| `NL01` | Nederland ⭐ |
| `PV20` | Groningen (PV) |
| `PV21` | Fryslân (PV) |
| `PV22` | Drenthe (PV) |
| `PV23` | Overijssel (PV) |
| `PV24` | Flevoland (PV) |
| `PV25` | Gelderland (PV) |
| `PV26` | Utrecht (PV) |
| `PV27` | Noord-Holland (PV) |
| `PV28` | Zuid-Holland (PV) |
| `PV29` | Zeeland (PV) |
| `PV30` | Noord-Brabant (PV) |
| `PV31` | Limburg (PV) |
| `1018850` | Zeer sterk stedelijk |
| `1018905` | Sterk stedelijk |
| `1018955` | Matig stedelijk |
| `1019005` | Weinig stedelijk |
| `1019052` | Niet stedelijk |
| `1019055` | Stedelijkheid onbekend |

---

## Power BI filtergebruik

| Use case | Filter op naam |
|----------|---------------|
| Nationale KPI's | `"Nederland"` |
| Provincievergelijking | `PV20` t/m `PV31` |
| Stedelijkheidsvergelijking | `1018850` t/m `1019055` |