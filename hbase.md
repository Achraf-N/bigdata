# TP4 — HBase (Solutions)

## Objectif du TP

Ce TP introduit HBase comme base NoSQL orientée colonnes. Il montre comment créer des tables avec familles de colonnes, insérer des lignes, interroger des cellules précises et relier HBase avec Hive, MapReduce et Sqoop.

---

## Partie 1 : Shell HBase — Table `commande`

Structure :

| RowKey | client:nom | client:ville | vente:produit | vente:prix |
|--------|-----------|-------------|--------------|-----------|
| 101 | ALAOUI Mohammed | Fes | Laptop | 10000 |
| 102 | HACHIMI Karima | Meknes | Haut parleurs | 140 |
| 103 | NOUACH Adil | Rabat | Power Bank | 1600 |
| 104 | IBRAHIMI Saloua | Casablanca | Casque Bluetooth | 780 |

### Création de la table et des familles de colonnes
```
hbase(main)> create 'commande', 'client', 'vente'
hbase(main)> list
```

### Insertion des données
```
hbase(main)> put 'commande', '101', 'client:nom',   'Alaoui Mohammed'
hbase(main)> put 'commande', '101', 'client:ville', 'Fes'
hbase(main)> put 'commande', '101', 'vente:produit','Laptop'
hbase(main)> put 'commande', '101', 'vente:prix',   '10000.00'

hbase(main)> put 'commande', '102', 'client:nom',   'Hachimi Karima'
hbase(main)> put 'commande', '102', 'client:ville', 'Meknes'
hbase(main)> put 'commande', '102', 'vente:produit','Haut parleurs'
hbase(main)> put 'commande', '102', 'vente:prix',   '140.00'

hbase(main)> put 'commande', '103', 'client:nom',   'Nouach Adil'
hbase(main)> put 'commande', '103', 'client:ville', 'Rabat'
hbase(main)> put 'commande', '103', 'vente:produit','Power Bank'
hbase(main)> put 'commande', '103', 'vente:prix',   '1600.00'

hbase(main)> put 'commande', '104', 'client:nom',   'Ibrahimi Saloua'
hbase(main)> put 'commande', '104', 'client:ville', 'Casablanca'
hbase(main)> put 'commande', '104', 'vente:produit','Casque Bluetooth'
hbase(main)> put 'commande', '104', 'vente:prix',   '780.00'
```

### Affichage
```
-- Toute la table
hbase(main)> scan 'commande'

-- Colonne produit de la ligne 102
hbase(main)> get 'commande', '102', {COLUMN => 'vente:produit'}

-- Toutes les colonnes de la ligne 101
hbase(main)> get 'commande', '101'
```

---

## Partie 1 (suite) : HiveQL sur la table HBase

```sql
CREATE EXTERNAL TABLE hbase_table_commande (
    id      INT,
    nom     STRING,
    ville   STRING,
    produit STRING,
    prix    FLOAT
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
    "hbase.columns.mapping" = ":key,client:nom,client:ville,vente:produit,vente:prix"
)
TBLPROPERTIES ("hbase.table.name" = "commande");

-- Afficher toutes les données
SELECT * FROM hbase_table_commande;

-- Clients dont le nom commence par 'A'
SELECT * FROM hbase_table_commande WHERE nom LIKE 'A%';
```

---

## Partie 2 : Chargement de `movies_data.csv`

Colonnes : `id, titre, annee, evaluation, duree`

```bash
# Charger dans HDFS
hdfs dfs -mkdir -p input
hdfs dfs -put movies_data.csv input

# Créer la table HBase
hbase shell
hbase(main)> create 'Films', 'cf'
exit

# Importer via MapReduce
hbase org.apache.hadoop.hbase.mapreduce.ImportTsv \
  -Dimporttsv.separator=',' \
  -Dimporttsv.columns=HBASE_ROW_KEY,cf:titre,cf:annee,cf:evaluation,cf:duree \
  Films input
```

### Vérification
```
hbase(main)> get 'Films', '1163', {COLUMN => 'cf:titre'}
hbase(main)> scan 'Films', {LIMIT => 5}
```

---

## Partie 3 : Chargement de `arbres.csv` via `csvToHbase.py`

Familles de colonnes :
- `genre` : GENRE(3), ESPECE(4), FAMILLE(5), NOM COMMUN(10), VARIETE(11)
- `infos` : ANNEE PLANTATION(6), HAUTEUR(7), CIRCONFERENCE(8)
- `adresse` : GEOPOINT(1), ARRONDISSEMENT(2), ADRESSE(9), NOM_EV(13)

### Chargement
```bash
hadoop fs -cat /user/cloudera/arbres.csv | python ./csvToHbase.py | hbase shell
```

### Requêtes HBase Shell
```
-- Genre de l'arbre arbre-82
hbase(main)> get 'arbres', 'arbre-82', {COLUMN => 'genre:genre'}

-- Famille infos de l'arbre arbre-10
hbase(main)> get 'arbres', 'arbre-10', 'infos'

-- Année de plantation des arbres du Parc Montsouris (scan avec filtre)
hbase(main)> scan 'arbres', {
  FILTER => "SingleColumnValueFilter('adresse', 'nom_ev', =, 'binary:Parc Montsouris')",
  COLUMNS => ['infos:annee_plantation']
}

-- Hauteur des arbres dont le genre est Quercus
hbase(main)> scan 'arbres', {
  FILTER => "SingleColumnValueFilter('genre', 'genre', =, 'binary:Quercus')",
  COLUMNS => ['infos:hauteur']
}

-- Noms communs des arbres du 13e arrondissement
hbase(main)> scan 'arbres', {
  FILTER => "SingleColumnValueFilter('adresse', 'arrondissement', =, 'binary:13')",
  COLUMNS => ['genre:nom_commun']
}
```

---

## Partie 4 : Import MySQL → HBase via Sqoop

```bash
sqoop import \
  --connect jdbc:mysql://localhost/retail_db \
  --username root \
  --password cloudera \
  --table categories \
  --hbase-table categories_hbase \
  --column-family cf \
  --hbase-row-key category_id \
  --hbase-create-table
```

## Bilan

HBase est adapté aux accès rapides par clé de ligne et aux données clairsemées organisées en familles de colonnes. Le TP met aussi en évidence son intégration avec l'écosystème Hadoop : Hive pour l'interrogation, MapReduce pour l'import massif et Sqoop pour l'alimentation depuis une base relationnelle.
