# Overzicht fact_doorlooptijden

Deze tabel bevat doorlooptijden van nieuwbouw van woningen en niet-woningen, van vergunningverlening tot oplevering. Voor elk verblijfsobject dat tussen 2015-heden in de voorraad is gekomen door nieuwbouw is de tijd berekend tussen de datum van de eerste vergunningverlening, en de datum van de voltooiing van de nieuwbouw. De tabel bevat gemiddelde, mediaan, en percentielen van de doorlooptijd, en zijn opgedeeld naar provincie, stedelijkheid, woningtype (een- of meergezins), en woning of niet-woning (CBS, 2026). 

## Kolomnamen + datatypes (SQLite)
1. ID - Integer
2. regiokenmerk_code - Text
3. gebruiksfunctie_code - Text
4. woningtype_code - Text
5. periode_code - Text
6. jaar - Integer
7. kwartaal - Integer
8. doorlooptijd_mediaan - Real
9. doorlooptijd_gemiddelde - Real
10. doorlooptijd_p10 - Real
11. doorlooptijd_p25 - Real
12. doorlooptijd_p75 - Real
13. doorlooptijd_p90 - Real
14. doorlooptijd_iqr - Real
15. doorlooptijd_p10_p90_range - Real
16. doorlooptijd_cv - Real
17. nieuwbouw_aantal - Real
18. hoge_variabiliteit - Integer

Voorbeeld entries (ID: 325 & 1550):
- 325,NL01   ,A045364,ZW10340,2025KW01,28.8,30.1,13.6,20.9,39.3,44.9,8443.0,Nederland,Woning totaal,Meergezinswoning,2025 1e kwartaal,2025,1,18.4,31.299999999999997,1,0.6388888888888888
- 1550,PV22   ,T001419,ZW10290,2017KW01,20.8,27.8,8.5,12.5,47.4,55.8,346.0,Drenthe (PV),Woning en niet-woning totaal,Eengezinswoning,2017 1e kwartaal,2017,1,34.9,47.3,1,1.6778846153846152 

Unieke jaren: 10 (2015-2025)
Unieke kwartalen: 44 (2015-2025)