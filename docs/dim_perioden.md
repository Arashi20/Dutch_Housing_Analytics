# dim_perioden

Periode dimensietabel. Gedeeld door beide fact tabellen. Bevat kwartalen (voor fact_doorlooptijden) én maanden (voor fact_woningen_pijplijn), plus jaarlijkse totaalrijen.

**Bron:** CBS 86260NED + 82211NED | **Rijen:** ~280

---

## Kolommen

| # | Kolomnaam | Type | Omschrijving |
|---|-----------|------|--------------|
| 1 | `code` | TEXT | Primary key (bijv. "2015KW01", "2019MM07", "2015JJ00") |
| 2 | `naam` | TEXT | Leesbare naam (bijv. "2015 1e kwartaal", "2019 juli") |
| 3 | `jaar` | INTEGER | Jaar (2015–2025) |
| 4 | `kwartaal` | REAL | Kwartaal (1–4), of NULL bij maand/jaarrijen |
| 5 | `maand` | REAL | Maand (1–12), of NULL bij kwartaal/jaarrijen |

---

## Code formaten

| Formaat | Voorbeeld | Gebruikt door | kwartaal | maand |
|---------|-----------|---------------|----------|-------|
| `####KW##` | `2015KW01` | fact_doorlooptijden ⭐ | 1–4 | NULL |
| `####MM##` | `2019MM07` | fact_woningen_pijplijn ⭐ | NULL | 1–12 |
| `####JJ00` | `2015JJ00` | Jaarlijkse totaalrijen | NULL | NULL |

---

## Perioden bereik

| Type | Bereik | Aantal |
|------|--------|--------|
| Kwartalen (`KW`) | 2015 KW01 – 2025 KW04 | ~44 rijen |
| Maanden (`MM`) | 2015 MM01 – 2025 MM12 | ~132 rijen |
| Jaartotalen (`JJ`) | 2015 – 2025 | ~11 rijen |

---

## Power BI filtergebruik

| Use case | Filter op |
|----------|-----------|
| Kwartaal lijndiagram | `kwartaal IS NOT NULL` |
| Maandelijks lijndiagram | `maand IS NOT NULL` |
| Jaarlijkse KPIs | code eindigt op `JJ00` |