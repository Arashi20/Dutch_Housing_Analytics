# De Nederlandse Woningcrisis onder de Loep

Nederland wordt al tientallen jaren geteisterd door een (structureel) woningtekort, en dit lijkt alleen erger te worden in komende jaren.

Dit woningtekort raakt alle Nederlanders, maar beperkt toch vooral de mogelijkheden van de starters en middeninkomens op de woningmarkt (Boelhouwer & van der Heijden, 2022). 

Zelf kamp ik ook ernstig met dit probleem. Met een WO-diploma op zak kun je in vele sectoren passend werk vinden, alleen kun je met de juniorsalaris nog lang geen huis kopen. 


Aangezien de huidige woningcrisis in Nederland (anno 2026) niet nieuw is -- eerder een slepend probleem waar veel ogen en haken aan vastzitten -- wil ik zelf in de cijfers van het CBS duiken (omtrent woningbouw) om de knelpunten te begrijpen, visualisaties te maken van de data, en een algemee beeld te krijgen van de Nederlandse woningmarkt over de jaren heen. 

## Onderzoekskader

### Hoofdvraag

**"Wat zijn de belangrijkste knelpunten in het Nederlandse woningbouwproces die bijdragen aan het structurele woningtekort, en hoe verschillen deze knelpunten tussen regio's en stedelijkheidsniveaus?"**

Deze vraag is relevant voor beleidsmakers omdat het:
- Identificeert **waar** in het proces vertragingen ontstaan (vergunning vs. bouw)
- Toont **welke regio's** het meest getroffen zijn (targetted interventions)
- Kwantificeert de **omvang** van het probleem (evidence-based policy)
- Biedt **vergelijkbare metrics** tussen gemeentes (benchmarking)

---

### Deelvragen

#### 1. Doorlooptijd Analyse
**"Hoe lang duurt het gemiddeld om een woning te bouwen (van vergunning tot oplevering) en hoe is dit veranderd sinds 2015?"**

#### 2. Regionale Verschillen
**"Zijn er significante verschillen in doorlooptijden en pijplijn-bottlenecks tussen provincies en stedelijkheidsgraden?"**

#### 3. Pijplijn Bottlenecks
**"Waar in het bouwproces ontstaan de grootste vertragingen en welk percentage projecten loopt vast in de pijplijn?"**

#### 4. Woningtype Verschillen
**"Verschilt de doorlooptijd significant tussen eengezinswoningen en meergezinswoningen?"**

#### 5. Temporele Patronen
**"Zijn er seizoenseffecten of economische cycli waarneembaar in doorlooptijden en pijplijn-volumes?"**

### Methodologie

(Placeholder voor nu).

## Datasets

### Doorlooptijden Nieuwbouw 

**ID:** 86260NED  
**URL:** https://opendata.cbs.nl/ODataApi/odata/86260NED

Deze tabel bevat doorlooptijden van nieuwbouw van woningen en niet-woningen, van vergunningverlening tot oplevering.

**Dimensies (filter-assen):**
- **Regiokenmerken** (19 items): Zeer stedelijk, Nederland, Utrecht (PV), etc.
- **Gebruiksfunctie** (3 items): Woning totaal, Niet-woning totaal, etc.
- **Woningtype** (3 items): Eengezinswoning, Meergezinswoning, Totaal
- **Perioden** (55 items): Kwartalen vanaf 2015 Q1 (format: `2015KW01`)
  - Granulariteit: Per kwartaal

**Measures (kolommen in de data):**
- `NieuwbouwTotaal_1`: Totaal aantal nieuwbouw
- `k_10KwantielDoorlooptijdMaanden_2`: 10e percentiel doorlooptijd (snelle projecten)
- `k_25KwantielDoorlooptijdMaanden_3`: 25e percentiel
- `MediaanDoorlooptijdMaanden_4`: Mediaan doorlooptijd (meest representatief)
- `k_75KwantielDoorlooptijdMaanden_5`: 75e percentiel
- `k_90KwantielDoorlooptijdMaanden_6`: 90e percentiel (trage projecten)
- `GemiddeldeDoorlooptijdMaanden_7`: Gemiddelde doorlooptijd

**Data structuur:** OLAP cube (multi-dimensionaal)  
**Totaal aantal cellen:** 65.835  
**Geverifieerd:** 2026-02-18 via CBS OData API test

### Woningen en niet-woningen in de pijplijn

**ID:** 82211NED  
**URL:** https://opendata.cbs.nl/ODataApi/odata/82211NED

Deze tabel bevat gegevens over woningen en niet-woningen in de pijplijn in Nederland. Voor elke maand is inzichtelijk gemaakt hoeveel verblijfsobjecten in de pijplijn zitten om gebouwd te worden: deze hebben ofwel een verleende bouwvergunning, of er is een melding gemaakt van bouw gestart.

**Dimensies (filter-assen):**
- **Gebruiksfunctie** (3 items): Woning totaal, Niet-woning totaal, etc.
- **RegioS** (475 items): Alle provincies + gemeentes
  - Bevat ALLE Nederlandse gemeentes (zeer gedetailleerd!)
- **Perioden** (187 items): Maandelijkse data vanaf 2015 (format: `2015MM01`)
  - Granulariteit: Per maand (!)
  - *Let op:* Meer gedetailleerd dan Dataset 1 (maandelijks vs. kwartaal)

**Measures (kolommen in de data):**
- `VerblijfsobjectenInDePijplijnTotaal_1`: Totaal aantal objecten in pijplijn
- `BouwGestartPijplijn_2`: Aantal waar bouw is gestart
- `Vergunningspijplijn_3`: Aantal in vergunningsfase (bottleneck!)
- `TotaalInDePijplijn2Jaar_4`: **KRITIEK** - Projecten langer dan 2 jaar vast
- `BouwGestartPijplijn2Jaar_5`: Bouw >2 jaar geleden gestart (vertraging)
- `Vergunningspijplijn2Jaar_6`: Vergunning >2 jaar geleden (bureaucratie!)
- `TotaalInDePijplijn5Jaar_7`: **ZEER KRITIEK** - Projecten langer dan 5 jaar vast

**Data structuur:** OLAP cube (multi-dimensionaal)  
**Totaal aantal cellen:** 2.398.275 (groot!)  
**Geverifieerd:** 2026-02-18 via CBS OData API test

### Bevolking per gemeente

(Additionele dataset die ik nog moet vinden). 


## Tech stack (Voorlopig)
- SQL
- Power BI
- Python



## Verwijzingen

Boelhouwer, P. J., & van der Heijden, H. M. H. (2022). De woningcrisis in Nederland vanuit een bestuurlijk
perspectief: achtergronden en oplossingen. Bestuurskunde, 31(1), 19-33.
https://doi.org/10.5553/Bk/092733872022031001002