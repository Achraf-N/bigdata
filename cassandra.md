# TP5 — Cassandra CQL (Solutions)

## Objectif du TP

Ce TP présente Cassandra et le langage CQL. Il met l'accent sur la création de keyspaces, la modélisation orientée requêtes, l'utilisation des collections et les limites importantes de Cassandra par rapport aux bases relationnelles classiques.

---

## Installation (rappel)
```bash
wget https://download.nust.na/pub2/apache/cassandra/2.1.22/apache-cassandra-2.1.22-bin.tar.gz
tar xvf apache-cassandra-2.1.22-bin.tar.gz
cd /home/cloudera/apache-cassandra-2.1.22/bin
./cassandra        # démarrer le serveur
./cqlsh            # démarrer le client
```

---

## Partie 1 : Requêtes simples — KeySpace `university`

### Création du KeySpace et des tables
```cql
CREATE KEYSPACE IF NOT EXISTS university
WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 3 };

USE university;

CREATE TABLE Cours (
    idCours    INT,
    Intitule   VARCHAR,
    Responsable INT,
    Niveau     VARCHAR,
    nbHeuresMax INT,
    Coeff      INT,
    PRIMARY KEY (idCours)
);

CREATE INDEX fk_Enseignement_Enseignant_idx ON Cours (Responsable);

CREATE TABLE Enseignant (
    idEnseignant INT,
    Nom          VARCHAR,
    Prenom       VARCHAR,
    status       VARCHAR,
    PRIMARY KEY (idEnseignant)
);
```

### Insertion des données
```cql
INSERT INTO Cours (idCours, Intitule, Responsable, Niveau, nbHeuresMax, Coeff)
    VALUES (1, 'Introduction aux Bases de Donnees', 1, 'M1', 30, 3);
INSERT INTO Cours (idCours, Intitule, Responsable, Niveau, nbHeuresMax, Coeff)
    VALUES (2, 'C++', 4, 'M1', 30, 2);
INSERT INTO Cours (idCours, Intitule, Responsable, Niveau, nbHeuresMax, Coeff)
    VALUES (3, 'Initiation Python', 5, 'M1', 30, 2);
INSERT INTO Cours (idCours, Intitule, Responsable, Niveau, nbHeuresMax, Coeff)
    VALUES (4, 'Bases de Donnees Avancees', 1, 'M2', 30, 5);
INSERT INTO Cours (idCours, Intitule, Responsable, Niveau, nbHeuresMax, Coeff)
    VALUES (5, 'Architecture des Systemes', 6, 'M2', 8, 1);
INSERT INTO Cours (idCours, Intitule, Responsable, Niveau, nbHeuresMax, Coeff)
    VALUES (6, 'Big Data', 7, 'M2', 20, 3);
INSERT INTO Cours (idCours, Intitule, Responsable, Niveau, nbHeuresMax, Coeff)
    VALUES (7, 'Bd NOSQL', 8, 'M2', 10, 1);

INSERT INTO Enseignant (idEnseignant, Nom, Prenom, status) VALUES (1, 'ALAOUI',   'Mohammed', 'Vacataire');
INSERT INTO Enseignant (idEnseignant, Nom, Prenom, status) VALUES (2, 'HACHIMI',  'Fatima',   'Permanent');
INSERT INTO Enseignant (idEnseignant, Nom, Prenom, status) VALUES (3, 'NOUACH',   'Adil',     'Vacataire');
INSERT INTO Enseignant (idEnseignant, Nom, Prenom, status) VALUES (4, 'SLAMI',    'Houda',    'Permanent');
INSERT INTO Enseignant (idEnseignant, Nom, Prenom, status) VALUES (5, 'BARAKAT',  'Ikram',    'Permanent');
INSERT INTO Enseignant (idEnseignant, Nom, Prenom, status) VALUES (6, 'TRABLSI',  'Mohamed',  'Permanent');
INSERT INTO Enseignant (idEnseignant, Nom, Prenom, status) VALUES (7, 'FATIM',    'Aicha',    'Vacataire');
INSERT INTO Enseignant (idEnseignant, Nom, Prenom, status) VALUES (8, 'ZNATY',    'Imad',     'Vacataire');
INSERT INTO Enseignant (idEnseignant, Nom, Prenom, status) VALUES (9, 'KASSIM',   'Ahmed',    'Vacataire');
```

### Requêtes
```cql
-- Liste tous les cours
SELECT * FROM Cours;

-- Liste des intitulés de cours
SELECT Intitule FROM Cours;

-- Nom de l'enseignant n°4
SELECT Nom, Prenom FROM Enseignant WHERE idEnseignant = 4;

-- Intitulé des cours du responsable n°1 (index requis)
SELECT Intitule FROM Cours WHERE Responsable = 1;

-- Cours dont nbHeuresMax = 30
SELECT Intitule FROM Cours WHERE nbHeuresMax = 30 ALLOW FILTERING;

-- Cours dont nbHeuresMax <= 30
-- Cassandra n'autorise pas < sur des colonnes non-primaires sans ALLOW FILTERING
-- et les inégalités ne sont supportées que sur la clé de clustering.
-- Solution : utiliser ALLOW FILTERING (attention aux performances en prod)
SELECT Intitule FROM Cours WHERE nbHeuresMax <= 30 ALLOW FILTERING;

-- Cours dont le responsable est 1 ET le niveau est M1
SELECT Intitule FROM Cours WHERE Responsable = 1 AND Niveau = 'M1' ALLOW FILTERING;

-- Cours dont l'identifiant est inférieur à 5 (via token)
SELECT Intitule FROM Cours WHERE token(idCours) < token(5);

-- Nombre de cours dont l'id < 5
SELECT COUNT(*) FROM Cours WHERE token(idCours) < token(5);
```

---

## Partie 2 : Collections

### LIST
```cql
CREATE TABLE maTableList (id INT PRIMARY KEY, list_col list<int>);
INSERT INTO maTableList (id, list_col) VALUES (1, [1, 2, 1]);
SELECT * FROM maTableList;

-- Ajouter un élément en fin de liste
UPDATE maTableList SET list_col = list_col + [3] WHERE id = 1;
-- Ajouter en début de liste
UPDATE maTableList SET list_col = [0] + list_col WHERE id = 1;
```

### SET
```cql
CREATE TABLE maTableSet (id INT PRIMARY KEY, set_col set<text>);
INSERT INTO maTableSet (id, set_col) VALUES (1, {'head', 'tail', 'head'});
-- Les doublons sont automatiquement éliminés → {'head', 'tail'}
SELECT * FROM maTableSet;

-- Ajouter un élément
UPDATE maTableSet SET set_col = set_col + {'side'} WHERE id = 1;
-- Supprimer un élément
UPDATE maTableSet SET set_col = set_col - {'tail'} WHERE id = 1;
```

### MAP
```cql
CREATE TABLE maTableMap (id INT PRIMARY KEY, map_col map<int, text>);
INSERT INTO maTableMap (id, map_col) VALUES (1, {1: 'one', 2: 'two', 1: 'dupe'});
-- La clé dupliquée est écrasée → {1: 'dupe', 2: 'two'}
SELECT * FROM maTableMap;

-- Ajouter/modifier une entrée
UPDATE maTableMap SET map_col[3] = 'three' WHERE id = 1;
SELECT * FROM maTableMap;
```

---

## Partie 3 : Exercice — Gestion sportive (équipes, athlètes, contrôles antidopage)

### Modélisation suggérée

```cql
CREATE KEYSPACE IF NOT EXISTS sport
WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 };

USE sport;

-- Table équipe avec athlètes comme SET (ou LIST)
CREATE TABLE equipe (
    idEquipe   INT,
    CodePays   TEXT,
    Sport      TEXT,
    athletes   set<text>,      -- {Nom Prénom, ...}
    PRIMARY KEY (idEquipe)
);

-- Table athlète (pour les requêtes détaillées)
CREATE TABLE athlete (
    idAthlète  INT,
    idEquipe   INT,
    Nom        TEXT,
    Prenom     TEXT,
    PRIMARY KEY (idEquipe, idAthlète)
);

-- Table contrôle antidopage
CREATE TABLE controle (
    idEquipe   INT,
    idAthlete  INT,
    numControle INT,
    dateControle DATE,
    resultat   TEXT,       -- 'Positif' ou 'Négatif'
    PRIMARY KEY ((idEquipe, idAthlete), dateControle, numControle)
) WITH CLUSTERING ORDER BY (dateControle DESC);
```

### Saisir une équipe (avec SET)
```cql
INSERT INTO equipe (idEquipe, CodePays, Sport, athletes)
    VALUES (1, 'MA', 'Football', {'Ali Hassani', 'Youssef Amrani'});

SELECT * FROM equipe WHERE idEquipe = 1;
```

> **Peut-on afficher le premier joueur uniquement ?**
> Non, pas directement. Un SET n'est pas ordonné et CQL ne supporte pas l'indexation par position sur les collections. Il faut modéliser les athlètes dans une table séparée avec une clé de clustering pour cibler un athlète précis.

### Requêtes après modélisation en tables séparées

```cql
-- 1. Liste des équipes avec leurs athlètes
-- Cassandra ne supporte pas les JOIN. On effectue donc deux lectures
-- puis on assemble le résultat côté application, ou bien on crée une table dénormalisée.
SELECT * FROM equipe;           -- toutes les équipes
SELECT * FROM athlete;          -- tous les athlètes

-- 2. Numéro et date des contrôles par équipe
SELECT idEquipe, numControle, dateControle FROM controle;

-- 3. Contrôles de l'athlète n°4 de l'équipe n°3
SELECT * FROM controle WHERE idEquipe = 3 AND idAthlete = 4;

-- 4. Nombre d'athlètes dans l'équipe n°6
SELECT COUNT(*) FROM athlete WHERE idEquipe = 6;

-- 5. Nombre de contrôles positifs par pays
-- (nécessite table dénormalisée avec CodePays en clé de partition)
SELECT COUNT(*) FROM controle
WHERE resultat = 'Positif' ALLOW FILTERING;

-- 6. Date du dernier contrôle pour chaque athlète
-- (ORDER BY dateControle DESC dans le clustering → première ligne = dernier contrôle)
SELECT idEquipe, idAthlete, dateControle
FROM controle PER PARTITION LIMIT 1;

-- 7. Équipes dont aucun athlète n'a été contrôlé positif
-- → Impossible en une seule requête CQL, traitement côté applicatif requis.

-- 8. Supprimer tous les contrôles de l'athlète n°4 de l'équipe n°3
DELETE FROM controle WHERE idEquipe = 3 AND idAthlete = 4;

-- 9. Modifier à « Négatif » les contrôles de l'athlète n°6 de l'équipe n°4 avant le 04/11/2005
-- Étape 1 : repérer les lignes concernées.
SELECT dateControle, numControle
FROM controle
WHERE idEquipe = 4 AND idAthlete = 6 AND dateControle < '2005-11-04';

-- Étape 2 : mettre à jour chaque ligne trouvée avec la clé complète.
-- Exemple pour une ligne retournée par le SELECT :
UPDATE controle SET resultat = 'Négatif'
WHERE idEquipe = 4 AND idAthlete = 6
  AND dateControle = '2005-10-20'
  AND numControle = 1;
```

## Bilan

Cassandra impose de penser les tables à partir des requêtes à exécuter. Les index secondaires et `ALLOW FILTERING` peuvent aider en TP, mais une solution propre repose souvent sur la dénormalisation et sur des clés de partition/clustering bien choisies.
