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

- Beschrijving: Verschil tussen gemiddelde doorlooptijden 2015 en 2025
- DAX: 

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

#### Measure naam: Gem Doorlooptijd Provincie

- Beschrijving: Gemiddelde doorlooptijd per provincie (2015-2025).
- DAX:
```dax
Gem Doorlooptijd Provincie = 
CALCULATE(
    AVERAGE(fact_doorlooptijden[doorlooptijd_mediaan]),
    dim_gebruiksfunctie[naam] = "Woning totaal",
    dim_woningtype[naam] = "Totaal"
)

```
- Input tabel: fact_doorlooptijden[doorlooptijd_mediaan]
- Eventuele filters:
  - dim_gebruiksfunctie[naam] = "Woning totaal"
  - dim_woningtype[naam] = "Totaal"

#### Measure naam: Langzaamste Provincie

- Beschrijving: De provincie met de hoogste doorlooptijd.
- DAX:
```dax

Langzaamste Provincie = 
CALCULATE(
    AVERAGE(fact_doorlooptijden[doorlooptijd_mediaan]),
    dim_regiokenmerken[naam] = "Noord-Holland (PV)",
    dim_gebruiksfunctie[naam] = "Woning totaal",
    dim_woningtype[naam] = "Totaal"
)

```
- Input tabel: fact_doorlooptijden[doorlooptijd_mediaan]
- Eventuele filters:
  - dim_regiokenmerken[naam] = "Noord-Holland (PV)"
  - dim_gebruiksfunctie[naam] = "Woning totaal"
  - dim_woningtype[naam] = "Totaal"

#### Measure naam: Snelste Provincie

- Beschrijving: De provincie met de laagste doorlooptijd. 
- DAX:
```dax
Snelste Provincie = 
CALCULATE(
    AVERAGE(fact_doorlooptijden[doorlooptijd_mediaan]),
    dim_regiokenmerken[naam] = "Overijssel (PV)",
    dim_gebruiksfunctie[naam] = "Woning totaal",
    dim_woningtype[naam] = "Totaal"
)

```
- Input tabel: fact_doorlooptijden[doorlooptijd_mediaan]
- Eventuele filters: 
  - dim_regiokenmerken[naam] = "Overijssel (PV)"
  - dim_gebruiksfunctie[naam] = "Woning totaal"
  - dim_woningtype[naam] = "Totaal"

#### Measure naam: Verschil Snelste Langzaamste

- Beschrijving: Het verschil in doorlooptijd tussen snelste en langzaamste provincie.
- DAX:
```dax
Verschil Snelste Langzaamste = 
[Langzaamste Provincie] - [Snelste Provincie]
```
- Input: vorige 2 DAX measures
- Geen filters


## Pagina 4: Bottleneckanalyse

#### Measure naam: Gem Vergunning Bottleneck Pct
- Beschrijving: Gemiddelde (ongewogen) van het percentage vergunnings‑bottlenecks over de rijen in results/3_bottleneck_top10_crisis.csv. De kolom Vergunning_Bottleneck_Pct_Avg is het percentage projecten in de vergunningsfase (in procentpunten).
- DAX:
```dax
Gem Vergunning Bottleneck Pct = 
AVERAGE('3_bottleneck_top10_crisis'[Vergunning_Bottleneck_Pct_Avg])

```

- Input tabel: 3_bottleneck_top10_crisis[Vergunning_Bottleneck_Pct_Avg]
- Geen filters

#### Measure naam: Ergste Gemeente Bottleneck

- Beschrijving: De hoogste (maximale) waarde van Bottleneck_2Jaar_Pct_Avg binnen de tabel results/3_bottleneck_top10_crisis (respectievelijk binnen de actuele filtercontext). Geeft het grootste percentage projecten dat >2 jaar vastzit (in procentpunten) binnen de getoonde set, typisch gebruikt om de absolute topwaarde in een top10 te tonen.
DAX (exact):
- DAX: 

```dax
Ergste Gemeente Bottleneck = 
MAXX(
    '3_bottleneck_top10_crisis',
    '3_bottleneck_top10_crisis'[Bottleneck_2Jaar_Pct_Avg]
)
```

- Input tabel: 3_bottleneck_top10_crisis[Bottleneck_2Jaar_Pct_Avg]
- eventuele filters: MAXX

#### Measure naam: Gem Nationaal Bottleneck 2Jaar

- Beschrijving: Ongewogen gemiddelde van Bottleneck_2Jaar_Pct_Avg over alle rijen in results/3_bottleneck_summary.csv (typisch alle gemeenten/regio's). Geeft het gemiddelde van gemeentelijke percentages (in procentpunten) binnen de huidige filtercontext.
DAX (exact):
- DAX:
```dax
Gem Nationaal Bottleneck 2Jaar = 
AVERAGE('3_bottleneck_summary'[Bottleneck_2Jaar_Pct_Avg])
```
- Input tabel: 3_bottleneck_summary[Bottleneck_2Jaar_Pct_Avg]
- Eventuele filters: AVERAGE


#### Measure naam: Vergunningsfase % (Meest Recent)

- Beschrijving: Hoeveel van de woningen in de pijplijn van 2025 die in de Vergunningsfase zitten.
- DAX:
```dax
Vergunning Fase % (Meest Recent) = 
CALCULATE(
    AVERAGE(fact_woningen_pijplijn[vergunning_fase_pct]),
    fact_woningen_pijplijn[jaar] = MAX(fact_woningen_pijplijn[jaar]),
    dim_regios[code] = "NL01",
    dim_gebruiksfunctie[naam] = "Woning totaal"
)
```

- Input Tabel: fact_woningen_pijplijn[vergunning_fase_pct]
- Eventuele filters:
  - fact_woningen_pijplijn[jaar] = MAX(fact_woningen_pijplijn[jaar])
  - dim_regios[code] = "NL01"
  - dim_gebruiksfunctie[naam] = "Woning totaal"

#### Measure naam: Bouwfase % (Meest Recent)

- Beschrijving: Hoeveel van de woningen in de pijplijn van 2025 die in de Bouwfase zitten.
- DAX:
```dax
Bouw Fase % (Meest Recent) = 
CALCULATE(
    AVERAGE(fact_woningen_pijplijn[bouw_fase_pct]),
    fact_woningen_pijplijn[jaar] = MAX(fact_woningen_pijplijn[jaar]),
    dim_regios[code] = "NL01",
    dim_gebruiksfunctie[naam] = "Woning totaal"
)

```

- Input tabel: fact_woningen_pijplijn[bouw_fase_pct]
- eventuele filters:
  - fact_woningen_pijplijn[jaar] = MAX(fact_woningen_pijplijn[jaar])
  - dim_regios[code] = "NL01"
  - dim_gebruiksfunctie[naam] = "Woning totaal"

## Pagina 5: Woningtypes

#### Doorlooptijd Eengezins

- Beschrijving: Gemiddelde doorlooptijd voor eengezinswoningen (2015-2025)
- DAX:

```dax
Doorlooptijd Eengezins = 
CALCULATE(
    AVERAGE(fact_doorlooptijden[doorlooptijd_mediaan]),
    dim_woningtype[naam] = "Eengezinswoning"
    dim_gebruiksfunctie[naam] = "Woning totaal"
    dim_regiokenmerken[naam] = "Nederland"
)

```
- input tabel: fact_doorlooptijden[doorlooptijd_mediaan]
- Eventuele filters: 
  - dim_woningtype[naam] = "Eengezinswoning"
  - dim_gebruiksfunctie[naam] = "Woning totaal"
  - dim_regiokenmerken[naam] = "Nederland"

#### Measure naam: Doorlooptijd Meergezins

- Beschrijving: Gemiddelde doorlooptijd voor meergezinswoningen (2015-2025)
- DAX:
```dax
Doorlooptijd Meergezins = 
CALCULATE(
    AVERAGE(fact_doorlooptijden[doorlooptijd_mediaan])
    dim_woningtype[naam] = "Meergezinswoning"
    dim_gebruiksfunctie[naam] = "Woning totaal"
    dim_regiokenmerken[naam] = "Nederland
)

```
- input tabel: fact_doorlooptijden[doorlooptijd_mediaan]
- Eventuele filters:
  - dim_woningtype[naam] = "Meergezinswoning"
  - dim_gebruiksfunctie[naam] = "Woning totaal"
  - dim_regiokenmerken[naam] = "Nederland"


## Pagina 6: Seizoenspatronen

