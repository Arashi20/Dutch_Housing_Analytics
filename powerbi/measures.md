# DAX Measures en Documentatie

PBIX bestand: powerbi/Dutch_Housing_Dashboard.pbix  


---

## Pagina 1: Overzicht

Measure naam: Avg Doorlooptijd
- Beschrijving: Gemiddelde doorlooptijd berekenen.
- DAX:
```dax
Avg Doorlooptijd = 
CALCULATE(
    AVERAGE(fact_doorlooptijden[doorlooptijd_mediaan]),
    dim_regiokenmerken[naam] = "Nederland",
    dim_gebruiksfunctie[naam] = "Woning totaal",
    dim_woningtype[naam] = "Totaal"
)
```
- Input tabel:
  - fact_doorlooptijden[doorlooptijd_mediaan]

- Eventuele filters:
  - dim_regiokenmerken[naam] = "Nederland"
  - dim_gebruiksfunctie[naam] = "Woning totaal"
  - dim_woningtype[naam] = "Totaal"

Measure naam:


## Pagina 2: Trendanalyse