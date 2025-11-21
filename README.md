# DI06---Migration-Pandas-to-PySpark
Migration Pandas → PySpark &amp; industrialisation
> Ceci est un projet d'école pour apprendre à utiliser PySpark

## pour une bonne installation

### pre-commit

Ce dépôt utilise [pre-commit](https://pre-commit.com/) pour automatiser le formatage, le linting et l'exécution de la suite de tests avant chaque commit. Pour l'installer et activer le hook :

```bash
pip install pre-commit
pre-commit install
```

Une fois installé, les étapes suivantes sont exécutées automatiquement lors d'un `git commit` :

- vérifications génériques (`check-yaml`, espaces fin de ligne, fin de fichier) ;
- formatage avec **Black** ;
- organisation des imports avec **isort** (profil Black) ;
- linting avec **flake8** + **flake8-bugbear** ;
- exécution de la suite de tests via **pytest**.


### redonner la main au dossier créé par docker
```
sudo chown -R $USER:$USER notebooks/
sudo chmod -R 755 notebooks/
```

### pour bien démarer N8N
```
mkdir -p n8n_data
mkdir -p n8n_files
sudo chown -R $USER:$USER n8n_data n8n_files
```

### SCHEDULING
**ajout du fichier main.py à la racine de "notebooks"
```
# Entry point for running the FreshKart pipeline with spark-submit.

from pipeline.orchestrator import run


if __name__ == "__main__":
    run()
```

**crontab**
```
0 6 * * * cd /chemin/vers/DI06---Migration-Pandas-to-PySpark && docker compose exec -w /home/jovyan/work jupyter /usr/local/spark/bin/spark-submit main.py >> logs/freshkart_pipeline.log 2>&1
```

Ne pas oublier de créer le dossier `logs` à la racine.


### structure utilisée

```
.
├── cours.md
├── docker-compose.yml
├── LICENSE
├── notebooks
│   ├── data
│   │   ├── done
│   │   │   ├── orders_2025-03-01.json
│   │   │   ├── orders_2025-03-02.json
│   │   │   ├── orders_2025-03-03.json
│   │   │   ├── orders_2025-03-04.json
│   │   │   ├── orders_2025-03-05.json
│   │   │   ├── orders_2025-03-06.json
│   │   │   ├── orders_2025-03-07.json
│   │   │   ├── orders_2025-03-08.json
│   │   │   ├── orders_2025-03-09.json
│   │   │   ├── orders_2025-03-10.json
│   │   │   ├── orders_2025-03-11.json
│   │   │   ├── orders_2025-03-12.json
│   │   │   ├── orders_2025-03-13.json
│   │   │   ├── orders_2025-03-14.json
│   │   │   ├── orders_2025-03-15.json
│   │   │   ├── orders_2025-03-16.json
│   │   │   ├── orders_2025-03-17.json
│   │   │   ├── orders_2025-03-18.json
│   │   │   ├── orders_2025-03-19.json
│   │   │   ├── orders_2025-03-20.json
│   │   │   ├── orders_2025-03-21.json
│   │   │   ├── orders_2025-03-22.json
│   │   │   ├── orders_2025-03-23.json
│   │   │   ├── orders_2025-03-24.json
│   │   │   ├── orders_2025-03-25.json
│   │   │   ├── orders_2025-03-26.json
│   │   │   ├── orders_2025-03-27.json
│   │   │   ├── orders_2025-03-28.json
│   │   │   ├── orders_2025-03-29.json
│   │   │   ├── orders_2025-03-30.json
│   │   │   └── orders_2025-03-31.json
│   │   ├── error
│   │   │   ├── orders_2025-02-29.json
│   │   │   └── orders_2025-04-01.json
│   │   ├── input
│   │   └── statics
│   │       ├── customers.csv
│   │       └── refunds.csv
│   ├── output
│   │   └── daily_summary
│   │       ├── daily_summary_20250301.csv
│   │       ├── daily_summary_20250302.csv
│   │       ├── daily_summary_20250303.csv
│   │       ├── daily_summary_20250304.csv
│   │       ├── daily_summary_20250305.csv
│   │       ├── daily_summary_20250306.csv
│   │       ├── daily_summary_20250307.csv
│   │       ├── daily_summary_20250308.csv
│   │       ├── daily_summary_20250309.csv
│   │       ├── daily_summary_20250310.csv
│   │       ├── daily_summary_20250311.csv
│   │       ├── daily_summary_20250312.csv
│   │       ├── daily_summary_20250313.csv
│   │       ├── daily_summary_20250314.csv
│   │       ├── daily_summary_20250315.csv
│   │       ├── daily_summary_20250316.csv
│   │       ├── daily_summary_20250317.csv
│   │       ├── daily_summary_20250318.csv
│   │       ├── daily_summary_20250319.csv
│   │       ├── daily_summary_20250320.csv
│   │       ├── daily_summary_20250321.csv
│   │       ├── daily_summary_20250322.csv
│   │       ├── daily_summary_20250323.csv
│   │       ├── daily_summary_20250324.csv
│   │       ├── daily_summary_20250325.csv
│   │       ├── daily_summary_20250326.csv
│   │       ├── daily_summary_20250327.csv
│   │       ├── daily_summary_20250328.csv
│   │       ├── daily_summary_20250329.csv
│   │       ├── daily_summary_20250330.csv
│   │       └── daily_summary_20250331.csv
│   ├── pipeline
│   │   ├── aggregations.py
│   │   ├── config.py
│   │   ├── file_management.py
│   │   ├── freshkart_io.py
│   │   ├── __init__.py
│   │   ├── io_readers.py
│   │   ├── orchestrator.py
│   │   ├── __pycache__
│   │   │   ├── aggregations.cpython-311.pyc
│   │   │   ├── config.cpython-311.pyc
│   │   │   ├── file_management.cpython-311.pyc
│   │   │   ├── freshkart_io.cpython-311.pyc
│   │   │   ├── __init__.cpython-311.pyc
│   │   │   ├── io_readers.cpython-311.pyc
│   │   │   ├── orchestrator.cpython-311.pyc
│   │   │   ├── spark_session.cpython-311.pyc
│   │   │   ├── transformations.cpython-311.pyc
│   │   │   └── writers.cpython-311.pyc
│   │   ├── spark_session.py
│   │   ├── transformations.py
│   │   └── writers.py
│   ├── test1.ipynb
│   ├── test2.ipynb
│   └── test final.ipynb
└── README.md
```