# TP1 — Hadoop MapReduce (Solutions regroupées)

Données : `purchases.txt` — format `date\ttime\tstore\titem\tcost\tpayment`

## Objectif du TP

Ce TP présente le principe de traitement distribué avec Hadoop Streaming. L'idée est de transformer un fichier de ventes en paires clé-valeur avec un mapper, puis d'agréger ces valeurs avec un reducer pour obtenir des indicateurs comme le total, la moyenne ou la vente maximale.

---

## Scripts

### mapper.py — clé = magasin
```python
#!/usr/bin/python
import sys

for line in sys.stdin:
    data = line.strip().split("\t")
    if len(data) == 6:
        date, time, store, item, cost, payment = data
        print(f"{store}\t{cost}")
```

### reducer.py — somme par clé
```python
#!/usr/bin/python
import sys

salesTotal = 0
oldKey = None

for line in sys.stdin:
    data_mapped = line.strip().split("\t")
    if len(data_mapped) != 2:
        continue
    thisKey, thisSale = data_mapped
    if oldKey and oldKey != thisKey:
        print(f"{oldKey}\t{salesTotal}")
        oldKey = thisKey
        salesTotal = 0
    oldKey = thisKey
    salesTotal += float(thisSale)

if oldKey is not None:
    print(f"{oldKey}\t{salesTotal}")
```

### tp1/mapper_by_category.py — clé = catégorie d'article
```python
#!/usr/bin/python
import sys

for line in sys.stdin:
    data = line.strip().split("\t")
    if len(data) == 6:
        date, time, store, item, cost, payment = data
        print(f"{item}\t{cost}")
```

### tp1/mapper_sum_by_day.py — clé = jour de la semaine
```python
#!/usr/bin/python
import sys
from datetime import datetime

for line in sys.stdin:
    data = line.strip().split("\t")
    if len(data) == 6:
        date, time, store, item, cost, payment = data
        day = datetime.strptime(date, "%Y-%m-%d").strftime("%A")
        print(f"{day}\t{cost}")
```

### tp1/min_sell_by_store.py — max (ou min) de vente par magasin (reducer)
```python
#!/usr/bin/python
import sys

max_cost = None
old_key = None

for line in sys.stdin:
    parts = line.rstrip("\n").split("\t")
    if len(parts) != 2:
        continue
    this_key, this_cost_raw = parts[0].strip(), parts[1].strip()
    if not this_key:
        continue
    try:
        this_cost = float(this_cost_raw)
    except ValueError:
        continue
    if old_key is not None and this_key != old_key:
        if max_cost is not None:
            print(f"{old_key}\t{max_cost}")
        max_cost = None
    old_key = this_key
    max_cost = this_cost if max_cost is None else max(max_cost, this_cost)

if old_key is not None and max_cost is not None:
    print(f"{old_key}\t{max_cost}")
```

### tp1/avg.py — moyenne par clé (reducer)
```python
#!/usr/bin/python
import sys

salesTotal = 0
count = 0
oldKey = None

for line in sys.stdin:
    data = line.strip().split("\t")
    if len(data) != 2:
        continue
    thisKey, thisSale = data
    try:
        val = float(thisSale)
    except ValueError:
        continue
    if oldKey and oldKey != thisKey:
        print(f"{oldKey}\t{salesTotal / count:.2f}")
        salesTotal = 0
        count = 0
    oldKey = thisKey
    salesTotal += val
    count += 1

if oldKey is not None and count > 0:
    print(f"{oldKey}\t{salesTotal / count:.2f}")
```

---

## Commandes

```bash
# Ventes totales par magasin
cat purchases.txt | ./mapper.py | sort | ./reducer.py

# Ventes totales par catégorie
cat purchases.txt | ./tp1/mapper_by_category.py | sort | ./reducer.py

# Ventes totales pour une catégorie spécifique (ex : Toys)
cat purchases.txt | ./tp1/mapper_by_category.py | sort | ./reducer.py | grep "Toys"

# Vente maximum par magasin
cat purchases.txt | ./mapper.py | sort | python3 ./tp1/min_sell_by_store.py

# Vente maximum pour un magasin spécifique (ex : Reno)
cat purchases.txt | ./mapper.py | sort | ./tp1/min_sell_by_store.py | grep "Reno"

# Somme des ventes par jour de la semaine
cat purchases.txt | ./tp1/mapper_sum_by_day.py | sort | ./reducer.py

# Moyenne des ventes par jour de la semaine
cat purchases.txt | ./tp1/mapper_sum_by_day.py | sort | ./tp1/avg.py
```

---

## Hadoop Streaming (sur cluster)

```bash
# Exemple : ventes totales par magasin
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -input /user/cloudera/purchases.txt \
  -output /user/cloudera/output_store \
  -mapper mapper.py \
  -reducer reducer.py \
  -file mapper.py \
  -file reducer.py
```

## Bilan

Le TP montre comment décomposer un problème analytique simple en deux étapes : émission des données utiles par le mapper, puis calcul final par le reducer. Les mêmes scripts peuvent être testés localement avec `cat`, `sort` et des pipes, puis exécutés sur Hadoop avec le même comportement logique.
