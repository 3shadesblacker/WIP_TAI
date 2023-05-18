# Squelette de projet Traitement de l'Intelligence Artificielle

Le but de ce projet est de mettre en oeuvre une solution IA associé à l'IoT et le Big Data.
Le squelette est adapté à la classe I1 GR4

Les élèves utilisent tous Kafka pour envoyer les données du capteur vers un cluster Spark.

Il y a les programmes suivants :
1. Un Capteur
2. Un programme pour tourner Kafka
3. Un programme pour nettoyer les données de Kafka via Spark
4. Un programme pour entraîner un modèle de machine learning
5. Un programme pour prédire les résultats à partir du modèle entraîné

Rendu :
- Le code source
- Un rapport
- Eventuellement un support de présentation au format PDF

Pour installer Kafka : Deux choix:
- Soit avec Conduktor (Le plus simple)
- Soit avec Docker-compose (Plus compliqué).

pour installer un dataLake ( ici minIO type S3 ):

 - lancer la commande docker-compose up
 - attendre
 - attendre
 - attendre encore un peu
 - quand c'est fini, ouvrir le docker desktop et ouvrez l'url sur le conteneur minIO
 pour se connecter c'est minio et minio123
 vous avez une bucket de deja préte qui s'appel wherhaouse.
 - ouvrir le conteneur spark-notebook mais au niveau des logs, cliquer sur le lien qui ressemble a:
 http://127.0.0.1:8888/lab?token=XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
 
 - vous pouvez copier le code python sur les cellules jupyter
 
