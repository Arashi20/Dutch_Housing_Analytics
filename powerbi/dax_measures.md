# DAX Measures en Documentatie

PBIX bestand: powerbi/Dutch_Housing_Dashboard.pbix  


---

## Pagina 1: Overzicht

#### Measure naam: Avg Doorlooptijd
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

#### Measure naam: YoY Change (%)

- Beschrijving: Gemiddelde verandering in doorlooptijden ten opzichte van het afgelopen jaar. Je moet eerst de gewenste jaar selecteren.
- DAX:
```dax

YoY Change (%) = 
VAR CurrentYearValue = [Avg Doorlooptijd]
VAR SelectedYear = SELECTEDVALUE(fact_doorlooptijden[jaar])
VAR PreviousYearValue = 
    CALCULATE(
        [Avg Doorlooptijd],
        FILTER(
            ALL(fact_doorlooptijden),
            fact_doorlooptijden[jaar] = SelectedYear - 1
        )
    )
RETURN
    IF(
        ISBLANK(PreviousYearValue) || ISBLANK(SelectedYear),
        BLANK(),
        DIVIDE(CurrentYearValue - PreviousYearValue, PreviousYearValue, 0) * 100
    )

```

- Input tabel:
  - fact_doorlooptijden[jaar]

- Eventuele filters:
  - fact_doorlooptijden[jaar] = SelectedYear - 1

#### Measure naam: Avg YoY Change (%)

- Beschrijving: Jaarlijkse gemiddelde verandering in doorlooptijden (2015-2025).
- DAX:
```dax
Avg YoY Change (%) = 
VAR YearlyChanges = 
    ADDCOLUMNS(
        SUMMARIZE(fact_doorlooptijden, fact_doorlooptijden[jaar]),
        "YearValue", [Avg Doorlooptijd],
        "PreviousYearValue", 
            CALCULATE(
                [Avg Doorlooptijd],
                FILTER(
                    ALL(fact_doorlooptijden),
                    fact_doorlooptijden[jaar] = EARLIER(fact_doorlooptijden[jaar]) - 1
                )
            )
    )
VAR YearlyChangesPct = 
    ADDCOLUMNS(
        YearlyChanges,
        "YoYChange", 
            DIVIDE([YearValue] - [PreviousYearValue], [PreviousYearValue], BLANK()) * 100
    )
RETURN
    AVERAGEX(
        FILTER(YearlyChangesPct, NOT(ISBLANK([YoYChange]))),
        [YoYChange]
    )

```

- Input tabel: 
  - fact_doorlooptijden[jaar]

- Eventuele filters:
  - fact_doorlooptijden[jaar] = EARLIER(fact_doorlooptijden[jaar]) - 1



## Pagina 2: Trendanalyse

#### Measure naam: P10 Doorlooptijd

- Beschrijving = Gemiddelde doorlooptijd 10e percentiel.
- DAX:

```dax
P10 Doorlooptijd = 
CALCULATE(
    AVERAGE(fact_doorlooptijden[doorlooptijd_p10]),
    dim_regiokenmerken[naam] = "Nederland",
    dim_gebruiksfunctie[naam] = "Woning totaal",
    dim_woningtype[naam] = "Totaal"
)

```

- Input tabel:
  - fact_doorlooptijden[doorlooptijd_p10]

Eventuele filters:
  - dim_regiokenmerken[naam] = "Nederland",
  - dim_gebruiksfunctie[naam] = "Woning totaal"
  - dim_woningtype[naam] = "Totaal"


#### Measure naam: P25 Doorlooptijd

- Beschrijving = Gemiddelde doorlooptijd 25e percentiel.
- DAX:

```dax
P25 Doorlooptijd = 
CALCULATE(
    AVERAGE(fact_doorlooptijden[doorlooptijd_p25]),
    dim_regiokenmerken[naam] = "Nederland",
    dim_gebruiksfunctie[naam] = "Woning totaal",
    dim_woningtype[naam] = "Totaal"
)

```

- Input tabel:
  - fact_doorlooptijden[doorlooptijd_p25]

Eventuele filters:
  - dim_regiokenmerken[naam] = "Nederland",
  - dim_gebruiksfunctie[naam] = "Woning totaal"
  - dim_woningtype[naam] = "Totaal"

#### Measure naam: P75 Doorlooptijd

- Beschrijving = Gemiddelde doorlooptijd 75e percentiel.
- DAX:

```dax
P75 Doorlooptijd = 
CALCULATE(
    AVERAGE(fact_doorlooptijden[doorlooptijd_p75]),
    dim_regiokenmerken[naam] = "Nederland",
    dim_gebruiksfunctie[naam] = "Woning totaal",
    dim_woningtype[naam] = "Totaal"
)

```

- Input tabel:
  - fact_doorlooptijden[doorlooptijd_p75]

Eventuele filters:
  - dim_regiokenmerken[naam] = "Nederland",
  - dim_gebruiksfunctie[naam] = "Woning totaal"
  - dim_woningtype[naam] = "Totaal"


#### Measure naam: P90 Doorlooptijd

- Beschrijving = Gemiddelde doorlooptijd 90e percentiel. 
- DAX:

```dax

P90 Doorlooptijd = 
CALCULATE(
    AVERAGE(fact_doorlooptijden[doorlooptijd_p90]),
    dim_regiokenmerken[naam] = "Nederland",
    dim_gebruiksfunctie[naam] = "Woning totaal",
    dim_woningtype[naam] = "Totaal"
)

```

- Input tabel:
  - fact_doorlooptijden[doorlooptijd_p90]

Eventuele filters:
  - dim_regiokenmerken[naam] = "Nederland",
  - dim_gebruiksfunctie[naam] = "Woning totaal"
  - dim_woningtype[naam] = "Totaal"


#### Measure naam: Doorlooptijd 2015

- Beschrijving: Gemiddelde doorlooptijd 2015.
- DAX:


```dax

Doorlooptijd 2015 = 
CALCULATE(
    AVERAGE(fact_doorlooptijden[doorlooptijd_mediaan]),
    dim_regiokenmerken[naam] = "Nederland",
    dim_gebruiksfunctie[naam] = "Woning totaal",
    dim_woningtype[naam] = "Totaal",
    fact_doorlooptijden[jaar] = 2015
)

```

- Input tabel:
  - fact_doorlooptijden[doorlooptijd_mediaan]

- Eventuele filters:
  - dim_regiokenmerken[naam] = "Nederland",
  - dim_gebruiksfunctie[naam] = "Woning totaal"
  - dim_woningtype[naam] = "Totaal"
  - fact_doorlooptijden[jaar] = 2015


#### Measure naam: Doorlooptijd 2025


- Beschrijving: Gemiddelde doorlooptijd 2025.
- DAX:


```dax

Doorlooptijd 2025 = 
CALCULATE(
    AVERAGE(fact_doorlooptijden[doorlooptijd_mediaan]),
    dim_regiokenmerken[naam] = "Nederland",
    dim_gebruiksfunctie[naam] = "Woning totaal",
    dim_woningtype[naam] = "Totaal",
    fact_doorlooptijden[jaar] = 2025
)

```

- Input tabel:
  - fact_doorlooptijden[doorlooptijd_mediaan]

- Eventuele filters:
  - dim_regiokenmerken[naam] = "Nederland",
  - dim_gebruiksfunctie[naam] = "Woning totaal"
  - dim_woningtype[naam] = "Totaal"
  - fact_doorlooptijden[jaar] = 2025


#### Measure naam: Stijging 2015 naar 2025


- DAX:
- Beschrijving: Verschil tussen gemiddelde doorlooptijden 2015 en 2025

```dax
Stijging 2015 naar 2025 = 
[Doorlooptijd 2025] - [Doorlooptijd 2015]

```

- Input tabel:
  - fact_doorlooptijden[doorlooptijd_mediaan] (vorige 2 DAX measures)

- Eventuele filters:
  - dim_regiokenmerken[naam] = "Nederland",
  - dim_gebruiksfunctie[naam] = "Woning totaal"
  - dim_woningtype[naam] = "Totaal"
  - fact_doorlooptijden[jaar] = 2015
  - fact_doorlooptijden[jaar] = 2025


  ## Pagina 3: Regionale Verschillen



  ## Pagina 4: Bottleneckanalyse


  ## Pagina 5: Woningtypes


  ## Pagina 6: Seizoenspatronen