<H1>FileFilter</H1>

<div align="center">
<img src="https://github.com/javitorres/filefilter/assets/4235424/6ad70c65-e35f-4ecf-a25d-37a2591e5d75" height="300">
</div>

<p align="center">
    <img src="https://img.shields.io/badge/Version-0.1.0-red" alt="Latest Release">
    <img src="https://img.shields.io/badge/DuckDB-0.9.1-yellow" alt="DuckDB">

</p>


# FileFilter
FileFilter is a ETL tool to load, transform and save data files. Transformations are defined in a YAML file.

The YAML configuration files includes settings for the delimiter used in the output file, the number of filter threads, the number of sample lines to process, and how often to reload the configuration. It also specifies three filters with their configurations, which will be applied to the data in sequence: a REST filter to make an HTTP request and add the response to the data, a Python filter to execute some Python code, and an SQL filter to execute a SQL query.

Filter types:

* **rest**: takes a row of data and an actionConfig dictionary. It then makes an HTTP request using parameters extracted from the actionConfig and the data row. The HTTP response is added to the row as a new column if the status is 200.

* **python**: executes Python code specified in the actionConfig.code field with the current row's data. The result of this execution (if any) is expected to modify the row's data.

* **sql**: executes SQL code provided in the actionConfig.sql field using duckdb on the dataframe df. The result of this query replaces the current dataframe.

* **pandas**: This filter executes Python code specified in the actionConfig.code section with the current df. The result of this query replaces the current dataframe.


# Configuration

Install dependencies listed in requirements.txt:

```
pip install -r requirements.txt
```

# Usage
usage: python3 filefilter.py FILE_IN FILTERS.yml FILE_OUT 

You can find examples in the examples folder.

Example: Consider file examples/fullExample/fullExample.txt:

```
id
1
2
3
4
5
```

Run:
```
python3 filefilter.py examples/fullExample/fullExample.txt examples/fullExample/fullExample.yml /tmp/fullExampleOutput.csv
```

And you'll get an output similar to:
```
,id,lat,lon,response,display_name,locality,state,lat_n,lat_s,lon_w,lon_e
0,1,40.37622704647547,-3.641814011779527,"{""place_id"": 108862066, ""licence"": ""Data \u00a9 OpenStreetMap contributors, ODbL 1.0. https://osm.org/copyright"", ""powered_by"": ""Map Maker: https://maps.co"", ""osm_type"": ""way"", ""osm_id"": 27608142, ""lat"": ""40.376376648779114"", ""lon"": ""-3.6418518373185678"", ""display_name"": ""Calle de Luis I, Casco Hist\u00f3rico de Vallecas, Villa de Vallecas, Madrid, \u00c1rea metropolitana de Madrid y Corredor del Henares, Community of Madrid, 28001, Spain"", ""address"": {""road"": ""Calle de Luis I"", ""quarter"": ""Casco Hist\u00f3rico de Vallecas"", ""suburb"": ""Villa de Vallecas"", ""city"": ""Madrid"", ""county"": ""\u00c1rea metropolitana de Madrid y Corredor del Henares"", ""state"": ""Community of Madrid"", ""postcode"": ""28001"", ""country"": ""Spain"", ""country_code"": ""es""}, ""boundingbox"": [""40.3733161"", ""40.3769421"", ""-3.6503754"", ""-3.6395964""]}","Calle de Luis I, Casco Histórico de Vallecas, Villa de Vallecas, Madrid, Área metropolitana de Madrid y Corredor del Henares, Community of Madrid, 28001, Spain",,Community of Madrid,40.37522704647547,40.37722704647547,-3.6428140117795267,-3.640814011779527
1,2,40.40391615649808,-3.6751618769655727,"{""place_id"": 165088347, ""licence"": ""Data \u00a9 OpenStreetMap contributors, ODbL 1.0. https://osm.org/copyright"", ""powered_by"": ""Map Maker: https://maps.co"", ""osm_type"": ""way"", ""osm_id"": 246712722, ""lat"": ""40.404062499999995"", ""lon"": ""-3.675274502164089"", ""display_name"": ""32, Calle de Valderribas, Pac\u00edfico, Retiro, Madrid, \u00c1rea metropolitana de Madrid y Corredor del Henares, Community of Madrid, 28007, Spain"", ""address"": {""house_number"": ""32"", ""road"": ""Calle de Valderribas"", ""quarter"": ""Pac\u00edfico"", ""suburb"": ""Retiro"", ""city"": ""Madrid"", ""county"": ""\u00c1rea metropolitana de Madrid y Corredor del Henares"", ""state"": ""Community of Madrid"", ""postcode"": ""28007"", ""country"": ""Spain"", ""country_code"": ""es""}, ""boundingbox"": [""40.4038997"", ""40.4042167"", ""-3.6754522"", ""-3.6750803""]}","32, Calle de Valderribas, Pacífico, Retiro, Madrid, Área metropolitana de Madrid y Corredor del Henares, Community of Madrid, 28007, Spain",,Community of Madrid,40.40291615649808,40.404916156498075,-3.6761618769655726,-3.674161876965573
2,3,40.340078105977554,-3.7349370437846385,"{""place_id"": 131171046, ""licence"": ""Data \u00a9 OpenStreetMap contributors, ODbL 1.0. https://osm.org/copyright"", ""powered_by"": ""Map Maker: https://maps.co"", ""osm_type"": ""way"", ""osm_id"": 125235653, ""lat"": ""40.339399150000006"", ""lon"": ""-3.7348636868751672"", ""display_name"": ""Centro Comercial Parquesur, Carretera de Villaverde, El Carrascal, Legan\u00e9s, \u00c1rea metropolitana de Madrid y Corredor del Henares, Community of Madrid, 28919, Spain"", ""address"": {""shop"": ""Centro Comercial Parquesur"", ""road"": ""Carretera de Villaverde"", ""suburb"": ""El Carrascal"", ""city"": ""Legan\u00e9s"", ""county"": ""\u00c1rea metropolitana de Madrid y Corredor del Henares"", ""state"": ""Community of Madrid"", ""postcode"": ""28919"", ""country"": ""Spain"", ""country_code"": ""es""}, ""boundingbox"": [""40.3376139"", ""40.3411805"", ""-3.7367676"", ""-3.7299257""]}","Centro Comercial Parquesur, Carretera de Villaverde, El Carrascal, Leganés, Área metropolitana de Madrid y Corredor del Henares, Community of Madrid, 28919, Spain",,Community of Madrid,40.339078105977556,40.34107810597755,-3.7359370437846384,-3.7339370437846386
3,4,40.51602067808552,-3.869456200657983,"{""place_id"": 12810594, ""licence"": ""Data \u00a9 OpenStreetMap contributors, ODbL 1.0. https://osm.org/copyright"", ""powered_by"": ""Map Maker: https://maps.co"", ""osm_type"": ""node"", ""osm_id"": 1243399419, ""lat"": ""40.5159164"", ""lon"": ""-3.8672713"", ""display_name"": ""La Fuente del Guarda, Las Rozas de Madrid, \u00c1rea metropolitana de Madrid y Corredor del Henares, Community of Madrid, 28231, Spain"", ""address"": {""locality"": ""La Fuente del Guarda"", ""town"": ""Las Rozas de Madrid"", ""county"": ""\u00c1rea metropolitana de Madrid y Corredor del Henares"", ""state"": ""Community of Madrid"", ""postcode"": ""28231"", ""country"": ""Spain"", ""country_code"": ""es""}, ""boundingbox"": [""40.5059164"", ""40.5259164"", ""-3.8772713"", ""-3.8572713""]}","La Fuente del Guarda, Las Rozas de Madrid, Área metropolitana de Madrid y Corredor del Henares, Community of Madrid, 28231, Spain",La Fuente del Guarda,Community of Madrid,40.51502067808552,40.517020678085515,-3.870456200657983,-3.8684562006579832
4,5,40.46369342878541,-3.7125918548980774,"{""place_id"": 171666269, ""licence"": ""Data \u00a9 OpenStreetMap contributors, ODbL 1.0. https://osm.org/copyright"", ""powered_by"": ""Map Maker: https://maps.co"", ""osm_type"": ""way"", ""osm_id"": 287910222, ""lat"": ""40.46376035"", ""lon"": ""-3.7126207776619244"", ""display_name"": ""18, Calle de Armenteros, Colonia de la Polic\u00eda Nacional, Valdezarza, Madrid, \u00c1rea metropolitana de Madrid y Corredor del Henares, Community of Madrid, 28039, Spain"", ""address"": {""house_number"": ""18"", ""road"": ""Calle de Armenteros"", ""neighbourhood"": ""Colonia de la Polic\u00eda Nacional"", ""quarter"": ""Valdezarza"", ""city"": ""Madrid"", ""county"": ""\u00c1rea metropolitana de Madrid y Corredor del Henares"", ""state"": ""Community of Madrid"", ""postcode"": ""28039"", ""country"": ""Spain"", ""country_code"": ""es""}, ""boundingbox"": [""40.4637147"", ""40.463805"", ""-3.7127252"", ""-3.712516""]}","18, Calle de Armenteros, Colonia de la Policía Nacional, Valdezarza, Madrid, Área metropolitana de Madrid y Corredor del Henares, Community of Madrid, 28039, Spain",,Community of Madrid,40.462693428785414,40.46469342878541,-3.7135918548980773,-3.7115918548980775
```

Showing with a CSV viewer like VisiData:

![Captura desde 2023-11-05 17-49-57](https://github.com/javitorres/filefilter/assets/4235424/a5e311a3-614b-4bf0-bc47-3c39b821eed8)

TODO
Run python code as UDF functions
    https://duckdb.org/docs/api/python/function.html
    https://duckdb.org/docs/sql/functions/overview.html
    https://duckdb.org/2023/07/07/python-udf.html