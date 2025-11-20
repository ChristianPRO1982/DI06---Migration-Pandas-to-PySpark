🎯 Étape 1 – config.py : pourquoi, comment, rôle dans un pipeline ?
🔵 Pourquoi un fichier config.py ?

Dans un vrai pipeline, on manipule beaucoup de chemins, paramètres, options, noms de fichiers, etc.

Sans fichier de config :

tu dupliques les chemins partout

si tu changes l’arborescence → tu dois tout modifier

ton orchestrateur devient un gros “spaghetti”

Avec config.py :

tous les chemins sont dans un seul endroit

on sépare logique & configuration

modularité → on peut réutiliser les fonctions sans tout casser

clarté → chaque module lit dans config

C’est un vrai réflexe de data engineer ⭐

🔵 Où se place config.py ?

Tu l’as mis dans :

notebooks/pipeline/config.py

🔵 Contenu conceptuel du config.py

config.py doit définir :

1️⃣ Les chemins principaux du projet (relatifs à notebooks/)

chemin vers le répertoire input (tous les JSON)

chemin vers le répertoire done

chemin vers le répertoire error

chemin vers les statiques (customers + refunds)

chemin vers les outputs CSV

2️⃣ Les patterns des fichiers

préfixe des fichiers commandes : "orders_"

extension : .json

3️⃣ Les conventions de date

format attendu dans les fichiers : %Y-%m-%d

format pour le CSV en sortie : %Y%m%d

4️⃣ Optionnel : paramètres de pipeline

activer logs détaillés ?

nombre de partitions Spark ?

encoding CSV ?

Pour l’instant tu n’en as pas besoin, mais la place est là si un jour tu veux.

---

Étape 2 = poser proprement la création de la SparkSession dans spark_session.py.

1️⃣ Rôle de spark_session.py dans ton pipeline

En Spark, tout passe par la SparkSession :

c’est elle qui lit les fichiers (CSV, JSON, Parquet…),

qui applique les transformations,

qui lance les jobs visibles dans le Spark UI,

et qui gère la config (nombre de partitions, logs, etc.).

Bon réflexe data ingé :
👉 une seule fonction qui crée cette session, dans un module dédié
👉 tous les autres modules l’utilisent (orchestrator, io_readers, tests dans notebook)

Ça évite :

d’avoir des SparkSession.builder... copiés-collés partout,

d’avoir des configs différentes suivant les scripts,

d’oublier un paramètre important à un endroit.

2️⃣ Local vs cluster dans ton contexte

Dans ton docker-compose, tu as :

un conteneur spark qui joue le rôle de Spark Master (UI sur 8080)

un conteneur jupyter avec pyspark-notebook où tu codes.

Mais tu n’as pas de Spark Worker déclaré dans le docker-compose.yml.

Donc deux options théoriques :

Local mode (ce que tu fais aujourd’hui)

SparkSession.builder.getOrCreate() sans .master(...)

Spark tourne “en local” dans le conteneur Jupyter.

Tu auras une Spark UI sur le port 4040 de ce conteneur (si tu le mappes un jour).

Cluster mode (spark://spark:7077)`

il faudrait ajouter au moins un Worker dans ton docker-compose.

et configurer .master("spark://spark:7077").

Comme tu ne veux pas partir en usine à gaz, on reste en local mode, ce qui est parfait pour :

apprendre les transformations Spark,

avoir un code simple,

et plus tard tu pourras brancher sur un cluster en changeant juste une ligne ici.

3️⃣ Ce qu’on veut exactement dans spark_session.py

Objectif :

1 module : notebooks/pipeline/spark_session.py

1 fonction publique : create_spark_session(app_name: str = "FreshKartDailyPipeline")

centraliser la création de la session

ajouter 1–2 petits réglages utiles (ex : progression dans la console)

Tu utiliseras ensuite cette fonction :

dans l’orchestrateur,

dans tes notebooks (à la place de celle de freshkart_io à terme).

4️⃣ Code

Remarques :

pas de .master(...) → on reste en local mode pour l’instant, simple et fiable ;

si plus tard tu veux tester le master standalone, tu pourras juste ajouter :

.master("spark://spark:7077")

---

étape 3 😎
Objectif : centraliser toute la lecture des données dans io_readers.py.

🧠 Rappel pédagogique : rôle de io_readers.py

Dans ton pipeline :

spark_session.py → crée la SparkSession

config.py → sait où sont les fichiers

io_readers.py → sait comment les lire avec Spark

Pourquoi c’est utile :

tu sépares la configuration (chemins) de la logique de lecture ;

tous les autres modules (transformations, orchestrator, tests) appellent les mêmes fonctions pour lire les données ;

si tu changes un jour le format (CSV → Parquet, autre chemin…), tu modifies un seul fichier.

Dans ton cas, io_readers.py va :

lire les fichiers statiques :

customers.csv

refunds.csv

lire un fichier JSON de commandes pour une date donnée :

orders_YYYY-MM-DD.json dans data/input

On prépare aussi dès maintenant la gestion des erreurs de type “fichier manquant”, pour que l’orchestrateur puisse décider de mettre le fichier en error/.

---

🚀 Étape suivante : transformations.py

Objectif pédagogique de cette étape :

comprendre comment Spark traite les DataFrames comme des tables distribuées

apprendre à appliquer des règles métier de manière fonctionnelle

manipuler les colonnes, filtrer, exploser, joindre, nettoyer

et surtout découvrir comment Spark génère des plans de calcul (visible dans Spark UI)

🎯 Dans cette étape, on va coder 4 transformations :
1️⃣ Filtrer les commandes payées
payment_status = 'paid'

2️⃣ Joindre les clients et exclure is_active = false

→ On garde seulement les commandes de clients actifs

3️⃣ Exploser les items

orders_df contient :

items: array<struct<qty, sku, unit_price>>


On doit passer de :

{
  order_id: 123,
  items: [
    {"qty": 1, "unit_price": 10},
    {"qty": 2, "unit_price": 5}
  ]
}


À :

(order_id, qty=1, unit_price=10)
(order_id, qty=2, unit_price=5)

4️⃣ Filtrer prix négatifs + renvoyer un DF des lignes rejetées

Les règles métier disent :

si unit_price < 0 → rejeter la ligne

garder trace des rejets