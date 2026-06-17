# TP3 — Apache Hive (Solutions)

## Objectif du TP

Ce TP présente Hive comme une couche SQL au-dessus de Hadoop. Il montre comment créer des tables, charger des fichiers depuis HDFS, effectuer des agrégations et réaliser des jointures sur des données volumineuses.

---

## Partie 1 : Table `purchases`

Données : `purchases.txt` — format `date\ttime\tstore\titem\tcost\tpayment`

### Création et chargement
```sql
CREATE TABLE purchases (
    datePurchase DATE,
    timePurchase VARCHAR(5),
    store        STRING,
    item         STRING,
    cost         DOUBLE,
    payment      STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

LOAD DATA INPATH '/user/cloudera/purchase/purchases.txt' INTO TABLE purchases;
```

### Requêtes

```sql
-- Tout afficher
SELECT * FROM purchases;

-- Ventes totales par magasin
SELECT store, SUM(cost) AS total_store
FROM purchases
GROUP BY store;

-- Ventes totales par catégorie d'article
SELECT item, SUM(cost) AS total_item
FROM purchases
GROUP BY item;

-- Ventes pour Toys et Consumer Electronics uniquement
SELECT item, SUM(cost) AS total_item
FROM purchases
WHERE item IN ('Toys', 'Consumer Electronics')
GROUP BY item;

-- Ventes totales et moyenne globale
SELECT
    SUM(cost)  AS total_ventes,
    AVG(cost)  AS moyenne_ventes
FROM purchases;

-- Somme des ventes par jour de la semaine
SELECT
    date_format(datePurchase, 'EEEE') AS jour_semaine,
    SUM(cost)                          AS total_ventes
FROM purchases
GROUP BY date_format(datePurchase, 'EEEE');

-- Moyenne des ventes par jour de la semaine
SELECT
    date_format(datePurchase, 'EEEE') AS jour_semaine,
    AVG(cost)                          AS moyenne_ventes
FROM purchases
GROUP BY date_format(datePurchase, 'EEEE');
```

---

## Partie 2 : Table `drivers`

Données : `drivers.csv` — format CSV, colonnes : `driverId,name,ssn,location,certified,wage-plan`

### Chargement via table temporaire (le CSV n'a pas de délimiteur simple)
```sql
-- Table temporaire pour lire le CSV brut
CREATE TABLE temp_driver (col_value STRING);
LOAD DATA INPATH '/user/cloudera/tp3/drivers.csv' OVERWRITE INTO TABLE temp_driver;

SELECT * FROM temp_driver;

-- Table cible structurée
CREATE TABLE drivers (
    driverId  INT,
    name      STRING,
    ssn       BIGINT,
    location  STRING,
    certified STRING,
    wageplan  STRING
);

-- Extraction par regexp
INSERT OVERWRITE TABLE drivers
SELECT
    regexp_extract(col_value, '^(?:([^,]*),?){1}', 1) AS driverId,
    regexp_extract(col_value, '^(?:([^,]*),?){2}', 1) AS name,
    regexp_extract(col_value, '^(?:([^,]*),?){3}', 1) AS ssn,
    regexp_extract(col_value, '^(?:([^,]*),?){4}', 1) AS location,
    regexp_extract(col_value, '^(?:([^,]*),?){5}', 1) AS certified,
    regexp_extract(col_value, '^(?:([^,]*),?){6}', 1) AS wageplan
FROM temp_driver;

SELECT * FROM drivers;
```

---

## Partie 3 : Table `timesheet`

Données : `timesheet.csv` — colonnes : `driverId,week,hours-logged,miles-logged`

```sql
CREATE TABLE timesheet (
    driverId     INT,
    week         INT,
    hours_logged INT,
    miles_logged INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

LOAD DATA INPATH '/user/cloudera/tp3/timesheet.csv' INTO TABLE timesheet;

SELECT * FROM timesheet;

-- Total heures et miles par conducteur
SELECT
    driverId,
    SUM(hours_logged) AS total_hours,
    SUM(miles_logged) AS total_miles
FROM timesheet
GROUP BY driverId;
```

---

## Partie 4 : Jointure drivers + timesheet

```sql
SELECT d.driverId, d.name, t.total_hours, t.total_miles
FROM drivers d
JOIN (
    SELECT
        driverId,
        SUM(hours_logged) AS total_hours,
        SUM(miles_logged) AS total_miles
    FROM timesheet
    GROUP BY driverId
) t ON d.driverId = t.driverId;
```

---

## Partie 5 : Base retail (customers / products / orders / categories)

```sql
SHOW TABLES;

-- Clients dont le prénom commence par 'A'
SELECT *
FROM customers
WHERE customer_fname LIKE 'A%';

-- Produits avec leur catégorie
SELECT p.product_name, c.category_name
FROM products p
JOIN categories c ON p.product_category_id = c.category_id;

-- Clients et leurs commandes
SELECT c.customer_id, c.customer_fname, o.order_id, o.order_date
FROM customers c
JOIN orders o ON c.customer_id = o.order_customer_id;
```

## Bilan

Hive facilite l'analyse de données stockées dans Hadoop grâce à une syntaxe proche du SQL. Les requêtes d'agrégation et de jointure deviennent plus lisibles que des programmes MapReduce, ce qui rend Hive adapté aux rapports, aux statistiques et aux analyses récurrentes.
