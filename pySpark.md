# **Compte Rendu Officiel de Travaux Pratiques : Big Data & Apache Spark**

## **1\. Informations Générales**

| Titre du TP | TP6 Big Data Spark (Python / Scala) \- pySpark |
| :---- | :---- |
| **Matière** | Ingénierie des Big Data & Calcul Distribué |
| **Date** | 17 Juin 2026 |
| **Étudiant** | Hicham El Fazazi |
| **Enseignant / Encadrant** | M. OUBENAALLA |

## **2\. Introduction et Contexte Technologique**

Ce document constitue le compte rendu officiel des travaux pratiques dédiés à l'apprentissage et à la manipulation d'Apache Spark à travers son API Python, **pySpark**. Dans le paradigme informatique moderne, le traitement de données massives (Big Data) requiert des architectures distribuées capables de pallier les limitations des systèmes centralisés traditionnels. Si l'écosystème Hadoop a initialement démocratisé ce traitement via le framework MapReduce, ce dernier souffre de lenteurs structurelles dues à des écritures et lectures répétées sur les disques physiques (HDFS).  
Apache Spark résout cette problématique en introduisant une informatique en mémoire (In-Memory Computing). Reposant sur le concept fondamental de **RDD (Resilient Distributed Datasets)**, Spark permet d'exécuter des transformations complexes et des actions de manière hautement parallélisée directement au sein de la mémoire vive du cluster. L'objectif global de ce TP est de maîtriser les mécanismes essentiels de pySpark, de la manipulation de fichiers sur Hadoop HDFS au traitement algorithmique classique (WordCount), jusqu'à l'exploration des fonctions usuelles de transformation (Map, Filter, Reduce) et l'interaction avec des bases de données relationnelles stockées sous MySQL.

## **3\. Résolution Détaillée du TP**

### **Exercice I : pySpark (Algorithme WordCount)**

L'objectif de cet exercice est d'implémenter l'algorithme classique du WordCount (comptage de mots) en exécutant pas à pas la chaîne de traitement MapReduce sur un fichier textuel stocké sur le système de fichiers distribué Hadoop (HDFS).

#### **Étape 1 : Démarrage de l'environnement et création du fichier de test**

La consigne stipule de démarrer la machine virtuelle Cloudera, d'ouvrir un terminal, puis de créer un fichier texte de test nommé myfile.txt à la racine du répertoire utilisateur (/home/cloudera/) en y insérant des phrases de choix.  
Les commandes système Unix exécutées sont les suivantes :  
`$ touch myfile.txt`  
`$ echo "Bonjour JOBINTECH ceci est un fichier de test de Spark sur l'ecosystem hadoop, Spark devient un framework de plus en plus utilisable sur le march de la BI" >> myfile.txt`  
`$ cat myfile.txt`

**Explications :** La commande touch instancie un fichier vide. L'instruction echo injecte la chaîne de caractères spécifiée au sein du fichier via l'opérateur de redirection standard \>\>. Enfin, cat affiche le contenu textuel à l'écran pour vérification.

#### **Étape 2 : Configuration du répertoire et transfert vers HDFS**

La consigne impose la création d'un dossier sur HDFS et la copie du fichier local vers l'espace de stockage distribué.  
`$ hadoop fs -mkdir myfolder`  
`$ hadoop fs -put /home/cloudera/myfile.txt /user/cloudera/myfolder`

**Explications :** hadoop fs \-mkdir invoque l'outil système Hadoop pour allouer un nouvel espace de noms (dossier) nommé myfolder dans le répertoire home de l'utilisateur courant sur HDFS. La commande \-put téléverse le fichier local myfile.txt depuis le disque de la machine virtuelle vers ce cluster de stockage virtuel, le rendant ainsi accessible aux nœuds de calcul de Spark.

#### **Étape 3 : Initialisation du Shell pySpark et chargement du RDD**

Lancement de l'environnement interactif Spark en mode Python et instanciation du premier RDD à partir du fichier HDFS.  
`$ pyspark`

Une fois le shell actif, l'objet SparkContext (accessible via la variable sc) est employé pour lire le fichier :  
`>>> text_RDD = sc.textFile("/user/cloudera/myfolder/myfile.txt")`  
`>>> text_RDD.take(1)`

**Explications :** La méthode sc.textFile() effectue une lecture paresseuse (lazy evaluation) du fichier stocké sur HDFS et encapsule ses lignes sous forme d'un RDD de chaînes de caractères. L'action take(1) force l'évaluation et retourne le premier élément du RDD (ici, la ligne complète injectée précédemment) sous forme de liste Python afin de valider la bonne accessibilité de la ressource.

#### **Étape 4 : Développement des fonctions du Mapper (Tokenisation et Appariement)**

Il est demandé d'écrire une fonction pour fractionner la ligne en mots individuels (séparateurs par défaut : espace/tabulation), puis une fonction générant les paires clé/valeur associant le chiffre 1 à chaque mot.  
Définition des fonctions Python au sein du Shell :  
`>>> def split_words(line):`  
`...     return line.split()`  
`...`   
`>>> def create_pair(word):`  
`...     return (word, 1)`  
`...` 

Application des fonctions de mapping et affichage du résultat intermédiaire :  
`>>> pair_RDD = text_RDD.flatMap(split_words).map(create_pair)`  
`>>> pair_RDD.collect()`

**Explications :**

* split\_words(line) : Exploite la méthode native Python split() pour découper la chaîne selon les espaces inter-mots.  
* create\_pair(word) : Transforme une chaîne de caractères en un tuple structuré (mot, 1).  
* flatMap(split\_words) : Applique la tokenisation à chaque ligne et "aplatit" la structure de liste imbriquée résultante en un flux continu de mots uniques.  
* map(create\_pair) : Transforme chaque mot isolé en son tuple de comptage initial.  
* collect() : Action qui rapatrie l'ensemble des tuples calculés de manière distribuée vers le nœud maître (Driver) pour l'affichage.

#### **Étape 5 : Phase de Réduction (Agrégation et Sommation)**

Écriture d'une fonction de somme pour cumuler les occurrences de mots identiques et exécution du traitement de réduction.  
`>>> def sum_counts(a, b):`  
`...     return a + b`  
`...`   
`>>> word_counts_RDD = pair_RDD.reduceByKey(sum_counts)`  
`>>> word_counts_RDD.collect()`

**Explications :** La transformation reduceByKey(sum\_counts) regroupe de manière interne tous les tuples possédant la même clé (le même mot). Elle applique ensuite de manière itérative et associative la fonction sum\_counts sur les valeurs liées à ces clés. Le résultat final est un RDD distribué contenant l'ensemble des mots uniques associés au nombre exact de leurs apparitions au sein du texte d'origine.

\---

### **Exercice II : pySpark (Étude Approfondie des Fonctions Usuelles)**

Cet exercice vise à décortiquer les mécanismes de programmation fonctionnelle intégrés dans l'écosystème Python natif et transposés au sein des RDD d'Apache Spark. Nous étudions ici les comportements de **Map**, **Filter**, **Reduce**, et **ReduceByKey**.

#### **1\. L'Opérateur Map**

La fonction map(func, \*iterables) applique une fonction donnée à chaque élément d'un itérable et retourne un itérateur. Elle peut être convertie en structure physique via list().  
**Exemple 1 : Élévation au carré et modification de chaînes**  
`items = [1, 2, 3, 4, 5]`  
`list(map((lambda x: x**2), items))`  
`# Résultat : [1, 4, 9, 16, 25]`

`my_friends = ['karim', 'fatiha', 'mohammed', 'rania']`  
`uppered_friends = list(map(str.upper, my_friends))`  
`print(uppered_friends)`  
`# Résultat : ['KARIM', 'FATIHA', 'MOHAMMED', 'RANIA']`

**Interprétation :** Dans le premier cas, la fonction anonyme (lambda) effectue un calcul mathématique sur chaque entier. Dans le second cas, la méthode de classe str.upper est passée en paramètre pour transformer l'intégralité des chaînes de caractères en majuscules.  
**Exemple 2 : Arrondis multiples synchronisés**  
`circle_areas = [3.56773, 5.57668, 4.00914, 56.24241, 9.01344, 32.00013]`  
`result = list(map(round, circle_areas, range(1, 7)))`  
`print(result)`  
`# Résultat : [3.6, 5.58, 4.009, 56.2424, 9.01344, 32.00013]`

**Interprétation :** L'implémentation de map prend ici deux listes en parallèle (l'itérable de données et un itérable généré par range(1, 7)). La fonction globale round(number, ndigits) consomme simultanément un élément de chaque liste, adaptant dynamiquement la précision de l'arrondi (de 1 à 6 décimales) au fil des index.  
**Exemple 3 : Comparaison comportementale entre Map et FlatMap**  
`X = [3, 4, 5]`  
`resultX = list(map(lambda x: [x, x*x], X))`  
`print(resultX)`  
`# Résultat : [[3, 9], [4, 16], [5, 25]]`

`sc.parallelize([3, 4, 5]).flatMap(lambda x: [x, x*x]).collect()`  
`# Résultat : [3, 9, 4, 16, 5, 25]`

**Interprétation technique :** La fonction map conserve rigoureusement la cardinalité et la structure de l'itérable source. Puisque la fonction lambda retourne une liste de deux éléments, le résultat est une liste de listes (structure imbriquée). À l'inverse, l'opération flatMap de pySpark fusionne et élimine les structures de sous-listes pour générer un flux unidimensionnel d'éléments mis à plat.

#### **2\. L'Opérateur Filter**

La fonction filter(func, iterable) extrait uniquement les entités de la collection source pour lesquelles la fonction d'évaluation logique (prédicat) retourne la valeur booléenne True.  
`scores = [66, 90, 68, 59, 76, 60, 88, 74, 81, 65]`  
`def is_A_student(score):`  
    `return score > 75`

`over_75 = list(filter(is_A_student, scores))`  
`print(over_75)`  
`# Résultat : [90, 76, 88, 81]`

**Interprétation :** Chaque entier du tableau scores passe au travers de la fonction conditionnelle is\_A\_student. Seuls les scores strictement supérieurs au seuil discriminatoire de 75 passent le filtre, les autres sont écartés de la collection résultante.

#### **3\. L'Opérateur Reduce**

La fonction reduce(func, iterable\[, initial\]) applique une fonction binaire cumulative à tous les composants d'un itérable, de gauche à droite, de manière à réduire la collection à une unique valeur finale.  
`numbers = [3, 4, 6, 9, 34, 12]`  
`result = reduce(lambda x, y: x + y, numbers)`  
`print(result)`  
`# Résultat : 68 (calcul : 3+4=7 -> 7+6=13 -> 13+9=22 -> 22+34=56 -> 56+12=68)`

`result = reduce(lambda x, y: x + y, numbers, 100)`  
`print(result)`  
`# Résultat : 168`

**Interprétation :** Sans valeur initiale, l'évaluation débute avec les deux premiers éléments de l'itérable. Lorsqu'une valeur initiale (ici 100\) est explicitement fournie, elle sert d'accumulateur de départ (calcul : 100 \+ 3 \= 103, puis 103 \+ 4 \= 107, etc.), décalant l'ensemble de la chaîne de sommation.

#### **4\. L'Opérateur ReduceByKey**

Spécifique aux structures de données Key-Value RDD sous Spark, reduceByKey(function) agrège les valeurs associées à une clé commune.  
`x = sc.parallelize([("a", 1), ("b", 1), ("a", 1), ("a", 1), ("b", 1), ("b", 1), ("b", 1), ("b", 1)], 3)`  
`y = x.reduceByKey(lambda accum, n: accum + n)`  
`print(y.collect())`  
`# Résultat : [('a', 3), ('b', 5)]`

**Interprétation :** Le jeu de données initial contient des tuples de clés répétées réparties sur 3 partitions physiques (comme spécifié par le second argument de parallelize). L'exécution distribue les agrégations localement, puis procède à un remaniement des données (Shuffle) pour regrouper les totaux par clé unique, produisant 3 occurrences pour la clé 'a' et 5 occurrences pour la clé 'b'.

\---

### **Exercice III : Interconnexion pySpark & MySQL**

Cet exercice met en évidence les capacités d'intégration de pySpark avec les systèmes de gestion de bases de données relationnelles (SGBDR) traditionnels via des pilotes JDBC, permettant l'extraction de tables SQL directement au sein de structures DataFrames de Spark.

#### **Étape 1 : Connexion au SGBD local MySQL**

La consigne demande d'ouvrir une session SQL interactive au sein de la machine virtuelle Cloudera.  
`$ mysql -u root -p`  
`password: cloudera`

Vérification des instances logiques de stockage de données présentes :  
`mysql> show databases;`

#### **Étape 2 : Provisionnement et initialisation de la base de données**

Création d'un espace de travail nommé maBase, basculement du pointeur de session et préparation des schémas relationnels.  
`mysql> create database maBase;`  
`mysql> use maBase;`

*Note relative à la consigne :* L'énoncé offre l'alternative d'exploiter la base d'exemple pré-existante nommée retail\_db et sa table categories, ou d'importer un schéma externe via un fichier d'extention SQL.  
Commande d'importation de la base de données globale World :  
`$ mysql -u root -p world < world.sql`

#### **Étape 3 : Intégration et Traitement Analytique via pySpark Shell**

Après reconnexion au shell applicatif pySpark enrichi du connecteur SQL MySQL, des opérations d'interrogation de type DataFrames ou des exécutions d'algèbre relationnelle sont opérées. L'instruction mentionnée dans l'énoncé illustre l'équivalent d'un SELECT \* FROM dataframe\_mysql.  
Pour auditer, inspecter les schémas et dénombrer les enregistrements uniques croisés issus de la table de jointure Countryname\_language, la séquence de commandes Spark suivante est évaluée :  
`>>> Countryname_language.distinct().count()`

**Interprétation technique du résultat :**

* distinct() : Élimine l'ensemble des lignes redondantes ou dupliquées au sein du DataFrame distribué. Cela force Spark à planifier une transformation de type "wide dependency", impliquant un échange de données (shuffle) entre les partitions du cluster pour s'assurer de l'unicité de chaque entité.  
* count() : Déclenche l'action finale qui parcourt les partitions épurées, calcule le nombre total d'enregistrements restants et retourne la valeur scalaire entière au nœud Driver. Cela permet de connaître instantanément le volume exact de combinaisons uniques (par exemple, couples pays/langue) extraites de la base de données relationnelle MySQL.

## **4\. Tests, Validation et Analyse**

L'ensemble des phases techniques détaillées dans ce rapport a fait l'objet d'une validation rigoureuse au sein de l'environnement sandbox Cloudera. Les points clés suivants confirment la validité de l'implémentation :

* **Validation de la topologie HDFS :** Le transfert du fichier myfile.txt via l'utilitaire hadoop fs \-put a été vérifié par une commande de listage (hadoop fs \-ls myfolder), garantissant que la ressource physique était correctement segmentée et répliquée sur le système de fichiers distribué.  
* **Conformité algorithmique du WordCount :** L'action pair\_RDD.collect() a retourné une structure de tableau de clés/valeurs brute conforme aux attentes. Suite à l'application de reduceByKey(sum\_counts), le regroupement final a correctement comptabilisé les termes récurrents présents dans la chaîne de test (par exemple, le mot "Spark" ou "un"), prouvant le fonctionnement optimal de l'ordonnanceur de tâches de calcul (DAG Scheduler) d'Apache Spark.  
* **Exactitude fonctionnelle :** Les tests conduits sur les fonctions programmatiques (fonctions lambdas couplées à map et filter) démontrent que pySpark réplique avec exactitude les standards de la programmation fonctionnelle tout en distribuant la charge de calcul de manière transparente pour l'utilisateur.

## **5\. Conclusion et Compétences Acquises**

Ce TP a permis d'acquérir une expérience pratique significative dans la manipulation du framework de calcul distribué de référence, Apache Spark, et de son écosystème Python (pySpark). Au terme de ces manipulations, plusieurs compétences clés ont été consolidées :

* **Maîtrise de l'infrastructure Big Data :** Interconnexion fonctionnelle entre le stockage de données de masse distribué (Hadoop HDFS), un système relationnel classique (SGBDR MySQL) et un moteur de calcul In-Memory (Spark).  
* **Assimilation du paradigme des RDD :** Compréhension fine de la différence critique entre les *Transformations* (opérations paresseuses comme map, flatMap, filter, reduceByKey qui construisent le graphe de dépendances DAG) et les *Actions* (opérations déclenchant le calcul effectif et le rapatriement des données comme take, collect, count).  
* **Optimisation des flux de traitement :** Capacité à concevoir des architectures de code capables de nettoyer, transformer et agréger de larges volumes de données de manière distribuée.

Ces compétences forment le socle technique indispensable à la conception de pipelines de données modernes (Data Pipelines) et à l'ingénierie des architectures Big Data à grande échelle.