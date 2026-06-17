# Travaux pratiques Big Data

Ce dossier regroupe les solutions de cinq TP autour de l'écosystème Big Data : Hadoop MapReduce, Apache Pig, Apache Hive, HBase et Cassandra.

## Contenu

| Fichier | Sujet | Résumé |
|---|---|---|
| `mapreduce.md` | TP1 - Hadoop MapReduce | Scripts mapper/reducer pour calculer des agrégations sur des ventes. |
| `pig.md` | TP2 - Apache Pig | Pipelines Pig Latin pour analyser UFO, pollution, arbres et films. |
| `hive.md` | TP3 - Apache Hive | Tables Hive, chargement HDFS, agrégations et jointures SQL. |
| `hbase.md` | TP4 - HBase | Manipulation HBase Shell, table externe Hive, ImportTsv et Sqoop. |
| `cassandra.md` | TP5 - Cassandra CQL | Keyspaces, tables CQL, collections et modélisation orientée requêtes. |

## Organisation conseillée

1. Lire le fichier Markdown du TP concerné.
2. Préparer les fichiers de données dans HDFS ou dans le dossier local selon l'outil utilisé.
3. Exécuter les commandes dans l'ordre indiqué.
4. Comparer les résultats avec l'objectif du TP et les remarques de bilan.

## Rapport

Le fichier `rapport_tp_big_data.docx` contient une synthèse rédigée de chaque TP avec les objectifs, les notions utilisées, les étapes principales et les conclusions.

## Remarque

Les commandes sont adaptées à un environnement pédagogique de type Cloudera/Hadoop. Certains chemins, versions ou mots de passe peuvent devoir être changés selon la machine utilisée.
