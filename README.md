## Aéroport

Ce projet est un pipeline de données aéroportuaires utilisant Apache Spark et Apache Airflow pour le traitement et l'orchestration des données. Il vise à fournir une solution évolutive et efficace pour l'analyse et la gestion des données liées aux aéroports.

## Installation

Tout d'abord, vous devez installer Spark et Airflow. Vous pouvez vous référer aux liens suivants pour l'installation :

- [Guide d'installation de Spark](https://spark.apache.org/docs/latest/)
- [Guide d'installation d'Airflow](https://airflow.apache.org/docs/apache-airflow/stable/start.html)

Remarque : Par défaut, si vous ne spécifiez pas la variable d'environnement AIRFLOW_HOME, Airflow utilisera le dossier $HOME/airflow comme emplacement par défaut pour sa configuration, ses DAG et ses métadonnées.

Si vous spécifiez AIRFLOW_HOME, Airflow utilisera le répertoire que vous avez spécifié. Cela vous permet de personnaliser l'emplacement où Airflow stocke ses fichiers de configuration, ses DAG et ses métadonnées selon vos besoins.

```bash
export AIRFLOW_HOME=/"chemin vers votre espace de travail"/
```

Ensuite, vous devez installer les bibliothèques Python datatime et subprocess. Vous pouvez les installer avec pip :

```bash
pip install datetime
pip install subprocess
```

## Utilisation

Après avoir installé toutes les dépendances, vous pouvez exécuter le script dag.py pour démarrer votre tâche de traitement des données :

```bash

python3 ./airflow/dags/dag.py
```

## Explication des scripts

    recuperer.py : Ce script est utilisé pour récupérer des données.
    analyse.py : Ce script est utilisé pour l'analyse des données.
    nettoyage.py : Ce script est utilisé pour nettoyer les données.
    formation.py : Ce script est utilisé pour le prétraitement des données.
    evaluation.py : Ce script est utilisé pour évaluer le modèle.

## Contribution

Toute forme de contribution est la bienvenue, y compris mais sans s'y limiter, les rapports de problèmes, les demandes de fonctionnalités et les soumissions de code.

## Licence

Ce projet est sous licence MIT, voir le fichier LICENSE pour plus de détails.
