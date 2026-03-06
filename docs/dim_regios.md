# dim_regios

Regio dimensietabel voor `fact_woningen_pijplijn`. Bevat Nederland totaal, landsdelen, provincies en alle 415 Nederlandse gemeentes.

**Bron:** CBS 82211NED | **Rijen:** 475

---

## Kolommen

| # | Kolomnaam | Type | Omschrijving |
|---|-----------|------|--------------|
| 1 | `code` | TEXT | Primary key (bijv. "NL01", "GM0363") |
| 2 | `naam` | TEXT | Leesbare naam (bijv. "Nederland", "Amsterdam") |
| 3 | `provincie` | TEXT | ⚠️ Momenteel NULL voor alle rijen |

---

## Structuur (hiërarchie)

| Code patroon | Type | Aantal | Voorbeeld |
|---|---|---|---|
| `NL01` | Nederland totaal | 1 | Nederland ⭐ |
| `LD01`–`LD04` | Landsdelen | 4 | Noord-Nederland (LD) |
| `LD99` | Niet in te delen | 1 | — |
| `PV20`–`PV3x` | Provincies | 12 | Noord-Holland (PV) |
| `GM####` | Gemeentes | 415 | Amsterdam, Utrecht, … |

---

## Power BI filtergebruik

| Use case | Filter op naam |
|---|---|
| Nationale KPI's | `"Nederland"` |
| Landsdelen vergelijking | naam bevat `"(LD)"` |
| Provincievergelijking | naam bevat `"(PV)"` |
| Gemeente-niveau | naam zonder suffix |

---

## Bekende data issues

- `provincie` kolom is **volledig NULL** — gemeentes zijn niet gekoppeld aan provincies