# case-study-azsde
This codebase is a PoC for the case study assigned on 2023-12-01. Goal is to design and implement simple, yet functional pipeline within the context of GCP that will:

'''
Zadání: Z níže uvedených zdrojů dat, vytvořte datovou pipelinu, která jednou za hodinu stáhne data
do vhodně navržené cloudové architektury včetně transformačních procesů. Ze zdroje 1 stáhne data,
následně až tento proces skončí, tak ze zdroje 2 a 3 (paralelně) a nakonec ze zdroje 4.
Zdroje dat:
1. [zdroj 1] .alza soubor:
• Stáhněte soubor z cloudového úložiště, do kterého se přihlaste pomocí tohoto sas_url:
https://stalzadfcase.blob.core.windows.net/sales-data?sp=r&st=2023-11-
30T10:39:13Z&se=2023-12-14T11:00:13Z&spr=https&sv=2022-11-
02&sr=c&sig=HWRihJvJCStY45hh5XwjAHOgBJvjZNhQDVo0p2PCgqk%3D.
• Výstup: Uložte data tak, aby byla použitelná pro jejich další analýzu.
2. [zdroj 2] Devizový trh ČNB:
• Stažení dat z devizového trhu ČNB v rozsahu od 20.12.2022 do 3.2.2023.
• Výstup: Uložte data tak, aby byla použitelná pro jejich další analýzu.
o Provedení transformací nad daty je na Vás.

3. [zdroj 3] API s historickými údaji o počasí:
• Napojte se na volně dostupné API s historickými údaji o počasí.
o Např. https://www.visualcrossing.com/weather/weather-data-services/
• Stáhněte data pro město Austin v Texasu, USA, proveďte Vámi navrhované transformace.
• Výstup: Vhodně uložte historická data o počasí s ohledem na jejich další analytickou
využitelnost.
4. [zdroj 4] BigQuery dataset:

• Napojte se na data z veřejně dostupného BigQuery Dataset_ID „bigquery-public-
data.austin_bikeshare“. Definice zadání:

a) Sestavte dotaz, který nám vrátí pro daný den, výchozí stanici, jaké kolo bylo
využito (bike_id) a dobu strávenou na kole v hodinách. Zajímají nás jen elektrická
kola. Stanice výchozí a cílová musí být totožná a zároveň aktivní.
▪ Výstup: Tato data zpracujte a uložte tak, aby byla připravena pro další
analýzy na základě níže definovaného požadavku "b)".

b) Výše připravená data použijte a spojte se zdrojem [1], [2] a [3], aby finální výstup
byl následující:
▪ Předchozí data obohaťte a pro každý den dále zobrazte, jaké bylo počasí
(průměrná teplota ve stupních Celsia) v analyzovaném městě, název kola
(výrobce) pro daný „trip“ a cenu kola zobrazte v Kč na základě kurzů z
ČNB.

Požadavky:
• Cloudová platforma: Zvolte vhodnou cloudovou platformu pro implementaci ETL/ELT
procesů (AWS, Azure, GCP), s preferencí pro Google Cloud Platform (GCP).
• Pipelines/ETL procesy: Vytvořte efektivní a škálovatelné ETL procesy pro stahování,
transformaci a ukládání dat z každého z čtyř zdrojů.
• Orchestrační pipelina: Navrhněte pipelinu, která bude spouštěčem jednotlivých událostí a
bude časově nastavená.
• Monitoring dat: Popište, jak budou data monitorována během ETL/ELT procesů, a jak budou
řešeny případné chyby v průběhu zpracování dat. [bod navíc, jen se zamyslete a případně pár
větami popište v přiložené výstupní dokumentaci]
• Architektura: Navrhněte cloudovou architekturu pro ukládání dat, s důrazem na
škálovatelnost, výkon a správné fungování ETL/ELT procesů.
• Dokumentace: K case study očekáváme doprovod v podobě detailní dokumentace, která
popisuje váš přístup, rozhodnutí při návrhu a implementaci, a také případné kompromisy.
Uveďte relevantní technické detaily a vysvětlení, jak jste překonali případné obtíže.
Souhrn k části č. 1: Cílem této první části case study je demonstrace vaší schopnosti efektivně
zpracovávat data z různých zdrojů pomocí ETL/ELT procesů, s důrazem na bezpečnost, monitorování
a navrhovanou cloudovou architekturu. Pro tuto část je také očekáván doprovod v podobě
dokumentace.
'''