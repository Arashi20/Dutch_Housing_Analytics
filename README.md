# De Nederlandse Woningcrisis onder de Loep

Nederland wordt al tientallen jaren geteisterd door een (structureel) woningtekort, en dit lijkt alleen erger te worden in komende jaren.

Dit woningtekort raakt alle Nederlanders, maar beperkt toch vooral de mogelijkheden van de starters en middeninkomens op de woningmarkt (Boelhouwer & van der Heijden, 2022). 

Zelf kamp ik ook ernstig met dit probleem. Met een WO-diploma op zak kun je in vele sectoren passend werk vinden, alleen kun je met de juniorsalaris nog lang geen huis kopen. 


Aangezien de huidige woningcrisis in Nederland (anno 2026) niet nieuw is -- eerder een slepend probleem waar veel ogen en haken aan vastzitten -- wil ik zelf in de cijfers van het CBS duiken (omtrent woningbouw) om de knelpunten te begrijpen, visualisaties te maken van de data, en een algemee beeld te krijgen van de Nederlandse woningmarkt over de jaren heen. 


## Datasets

### Doorlooptijden Nieuwbouw 

ID:86260NED 
https://opendata.cbs.nl/ODataApi/odata/86260NED

Deze tabel bevat doorlooptijden van nieuwbouw van woningen en niet-woningen, van vergunningverlening tot oplevering. 

Dimensies:
- "Onderwerpen": 10% Kwantiel doorlooptijd (maanden), Mediaan doorlooptijd (maanden), etc. (7 totaal).
- "Regiokenmerken": Zeer stedelijk, Nederland, Utrecht (PV), etc. (19 totaal).
- "Gebruiksfunctie": Woning totaal, Niet-woning totaal, etc. (3 totaal).
- "Woningtype": Eengezinswoning, Meergezinswoning, Totaal. (3 totaal).
- "Perioden": Jaren, Kwartalen, 2015 1e kwartaal, etc. (55 totaal).

Totaal aantal cellen: 65.835


## Woningen en niet-woningen in de pijplijn

ID:82211NED
https://opendata.cbs.nl/ODataApi/odata/82211NED

Deze tabel bevat gegevens over woningen en niet-woningen in de pijplijn in Nederland. Voor elke maand is inzichtelijk gemaakt hoeveel verblijfsobjecten in de pijplijn zitten om gebouwd te worden: deze hebben ofwel een verleende bouwvergunning, of er is een melding gemaakt van bouw gestart. 

Dimensies:
- "Onderwerpen": Verblijfsobjecten in de pijplijn, Bouw gestart pijplijn, Verblijfsobjecten >5 jaar in de pijplijn, etc. (9 totaal).
- "Gebruiksfunctie": Woning totaal, Niet-woning totaal, etc. (3 totaal). 
- "Regio's": Provincies, Gemeentes, Groningen (PV), etc. (475 totaal).
- "Perioden": Jaren, Maanden, Kwartalen, 2015 mei, etc. (187 totaal).

Totaal aantal cellen: 2.398.275











## Verwijzingen

Boelhouwer, P. J., & van der Heijden, H. M. H. (2022). De woningcrisis in Nederland vanuit een bestuurlijk
perspectief: achtergronden en oplossingen. Bestuurskunde, 31(1), 19-33.
https://doi.org/10.5553/Bk/092733872022031001002