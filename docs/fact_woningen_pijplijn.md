# fact_woningen_pijplijn

Maandelijkse pijplijn data voor woningbouw in Nederland, per regio en gebruiksfunctie. Toont hoeveel woningen in de bouwpijplijn zitten, uitgesplitst naar fase (vergunning vs. bouw) en hoe lang ze al vastzitten.

**Bron:** CBS 82211NED | **Rijen:** 88.825 | **Granulariteit:** Per maand

---

## Kolommen

| # | Kolomnaam | Type | Omschrijving |
|---|-----------|------|--------------|
| 1 | `id` | INTEGER | Primary key |
| 2 | `regio_code` | TEXT | FK → dim_regios.code |
| 3 | `gebruiksfunctie_code` | TEXT | FK → dim_gebruiksfunctie.code |
| 4 | `periode_code` | TEXT | FK → dim_perioden.code (bijv. "2025MM01") |
| 5 | `jaar` | INTEGER | Jaar (2015–2025) |
| 6 | `maand` | INTEGER | Maand (1–12) |
| 7 | `pijplijn_totaal` | REAL | Totaal woningen in pijplijn |
| 8 | `pijplijn_bouw_gestart` | REAL | Woningen waar bouw is gestart |
| 9 | `pijplijn_vergunning` | REAL | Woningen in vergunningsfase |
| 10 | `pijplijn_vast_2jaar` | REAL | ⚠️ Woningen >2 jaar vastgezet in pijplijn |
| 11 | `pijplijn_bouw_gestart_2jaar` | REAL | Bouw >2 jaar geleden gestart |
| 12 | `pijplijn_vergunning_2jaar` | REAL | Vergunning >2 jaar geleden verleend |
| 13 | `pijplijn_vast_5jaar` | REAL | 🚨 Woningen >5 jaar vastgezet |
| 14 | `pijplijn_bouw_gestart_5jaar` | REAL | Bouw >5 jaar geleden gestart |
| 15 | `pijplijn_vergunning_5jaar` | REAL | Vergunning >5 jaar geleden verleend |
| 16 | `bottleneck_2jaar_pct` | REAL | % pijplijn dat >2 jaar vastzit |
| 17 | `bottleneck_5jaar_pct` | REAL | % pijplijn dat >5 jaar vastzit |
| 18 | `vergunning_bottleneck_pct` | REAL | % vergunningen dat >2 jaar vastzit |
| 19 | `bouw_bottleneck_pct` | REAL | % bouwstarts dat >2 jaar vastzit |
| 20 | `vergunning_fase_pct` | REAL | % totaal dat in vergunningsfase zit |
| 21 | `bouw_fase_pct` | REAL | % totaal dat in bouwfase zit |
| 22 | `crisis_regio` | INTEGER | 1 = bottleneck >20%, 0 = normaal |

---

## Dimensie filterwaarden (voor Power BI)

### regio_code → dim_regios
| code | naam |
|------|------|
| `NL01` | Nederland ⭐ (gebruik voor nationale KPIs) |
| `LD01`–`LD04` | Landsdelen |
| `PV20`–`PV31` | Provincies |
| `GM####` | Gemeentes (415 stuks) |

### gebruiksfunctie_code → dim_gebruiksfunctie
| code | naam |
|------|------|
| `A045364` | Woning totaal ⭐ |

---

## Unieke jaren & maanden
- **Jaren:** 2015–2025
- **Maanden:** 1–12 (maandelijkse data)
- **Perioden formaat:** `2015MM01` t/m `2025MM12`