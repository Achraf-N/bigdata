# TP2 — Apache Pig (Solutions)

## Objectif du TP

Ce TP utilise Pig Latin pour charger, filtrer, grouper et agréger des données semi-structurées. Les exercices couvrent plusieurs jeux de données afin de montrer comment Pig simplifie les traitements qui seraient plus longs à écrire directement en MapReduce.

---

## Exercice 1 : UFO Sightings (`ufo_sightings.tsv`)

Colonnes : `sighted_at, reported_at, location_str, shape, duration_str, description, lng, lat, city, county, state, country`

### Chargement
```pig
sightings = LOAD 'ufo_sightings.tsv' AS (
    sighted_at:chararray, reported_at:chararray, location_str:chararray,
    shape:chararray,      duration_str:chararray, description:chararray,
    lng:float,            lat:float,              city:chararray,
    county:chararray,     state:chararray,        country:chararray
);
```

### Nombre d'observations par mois
```pig
month_count = FOREACH sightings GENERATE SUBSTRING(sighted_at, 5, 7) AS month;
ufos_by_month = FOREACH (GROUP month_count BY month) GENERATE
    group AS month, COUNT_STAR(month_count) AS total;
DUMP ufos_by_month;
STORE ufos_by_month INTO './ufos_by_month.out';
```

### Nombre d'observations par année
```pig
year_count = FOREACH sightings GENERATE SUBSTRING(sighted_at, 0, 4) AS year;
ufos_by_year = FOREACH (GROUP year_count BY year) GENERATE
    group AS year, COUNT_STAR(year_count) AS total;
ufos_by_year_sorted = ORDER ufos_by_year BY year ASC;
DUMP ufos_by_year_sorted;
```

### Nombre d'observations par état (state)
```pig
by_state = FOREACH (GROUP sightings BY state) GENERATE
    group AS state, COUNT_STAR(sightings) AS total;
by_state_sorted = ORDER by_state BY total DESC;
DUMP by_state_sorted;
```

### Observations aux États-Unis uniquement
```pig
us_sightings = FILTER sightings BY country == 'United States of America';
DUMP us_sightings;
```

### Top 10 des villes avec le plus d'observations
```pig
by_city = FOREACH (GROUP sightings BY city) GENERATE
    group AS city, COUNT_STAR(sightings) AS total;
top10 = LIMIT (ORDER by_city BY total DESC) 10;
DUMP top10;
```

---

## Exercice 2 : Pollution de l'air (`AirPollution.txt`)

Colonnes : `Ville, Mois, Annee, CO, O3, Temperature`

### Chargement
```pig
pollution = LOAD 'AirPollution.txt'
    USING PigStorage(',')
    AS (ville:chararray, mois:int, annee:int, co:float, o3:float, temperature:float);

-- Ignorer l'en-tête
pollution_data = FILTER pollution BY ville != 'Ville';
```

### Moyenne de CO par ville
```pig
avg_co = FOREACH (GROUP pollution_data BY ville) GENERATE
    group AS ville, AVG(pollution_data.co) AS avg_co;
DUMP avg_co;
```

### Moyenne de température par année
```pig
avg_temp_year = FOREACH (GROUP pollution_data BY annee) GENERATE
    group AS annee, AVG(pollution_data.temperature) AS avg_temp;
avg_temp_sorted = ORDER avg_temp_year BY annee ASC;
DUMP avg_temp_sorted;
```

### Ville avec le taux de CO le plus élevé
```pig
max_co = FOREACH (GROUP pollution_data BY ville) GENERATE
    group AS ville, MAX(pollution_data.co) AS max_co;
top_co = LIMIT (ORDER max_co BY max_co DESC) 1;
DUMP top_co;
```

### Données pour une ville spécifique (ex : Fez)
```pig
fez = FILTER pollution_data BY ville == 'Fez';
DUMP fez;
```

---

## Exercice 3 : Arbres de Paris (`arbres.csv`)

Colonnes : `GEOPOINT;ARRONDISSEMENT;GENRE;ESPECE;FAMILLE;ANNEE PLANTATION;HAUTEUR;CIRCONFERENCE;ADRESSE;NOM COMMUN;VARIETE;OBJECTID;NOM_EV`

### Chargement
```pig
arbres = LOAD 'arbres.csv'
    USING PigStorage(';')
    AS (geopoint:chararray, arrondissement:int, genre:chararray, espece:chararray,
        famille:chararray, annee:int, hauteur:float, circonference:float,
        adresse:chararray, nom_commun:chararray, variete:chararray,
        objectid:int, nom_ev:chararray);

-- Ignorer l'en-tête
arbres_data = FILTER arbres BY genre != 'GENRE';
```

### Nombre d'arbres par arrondissement
```pig
by_arr = FOREACH (GROUP arbres_data BY arrondissement) GENERATE
    group AS arrondissement, COUNT_STAR(arbres_data) AS nb_arbres;
by_arr_sorted = ORDER by_arr BY arrondissement ASC;
DUMP by_arr_sorted;
```

### Hauteur moyenne par genre
```pig
avg_height = FOREACH (GROUP arbres_data BY genre) GENERATE
    group AS genre, AVG(arbres_data.hauteur) AS avg_hauteur;
DUMP avg_height;
```

### Arbres plantés par décennie
```pig
decades = FOREACH arbres_data GENERATE
    (int)((annee / 10) * 10) AS decennie;
by_decade = FOREACH (GROUP decades BY decennie) GENERATE
    group AS decennie, COUNT_STAR(decades) AS nb_arbres;
by_decade_sorted = ORDER by_decade BY decennie ASC;
DUMP by_decade_sorted;
```

---

## Exercice 4 : Films (`movies_data.csv`)

Colonnes : `id, titre, annee, evaluation, duree`

### Chargement
```pig
movies = LOAD 'movies_data.csv'
    USING PigStorage(',')
    AS (id:int, titre:chararray, annee:int, evaluation:float, duree:int);
```

### Films les mieux notés (top 10)
```pig
top10 = LIMIT (ORDER movies BY evaluation DESC) 10;
DUMP top10;
```

### Évaluation moyenne par année
```pig
avg_eval = FOREACH (GROUP movies BY annee) GENERATE
    group AS annee, AVG(movies.evaluation) AS avg_eval;
avg_eval_sorted = ORDER avg_eval BY annee ASC;
DUMP avg_eval_sorted;
```

### Nombre de films par décennie
```pig
decades = FOREACH movies GENERATE (int)((annee / 10) * 10) AS decennie;
by_decade = FOREACH (GROUP decades BY decennie) GENERATE
    group AS decennie, COUNT_STAR(decades) AS nb_films;
DUMP (ORDER by_decade BY decennie ASC);
```

## Bilan

Pig permet d'exprimer des traitements analytiques sous forme de pipeline : `LOAD`, `FILTER`, `GROUP`, `FOREACH`, `ORDER`, puis `DUMP` ou `STORE`. Il est particulièrement utile pour explorer rapidement des fichiers volumineux avant de passer à des traitements plus spécialisés.
