import datetime
import numpy as np
import pandas as pd
from minio import Minio

"""
Mini-Projet : Traitement de l'Intelligence Artificielle
Contexte : Allier les concepts entre l'IA, le Big Data et IoT

Squelette pour simuler un capteur qui est temporairement stocké sous la forme de Pandas
"""

"""
    Dans ce fichier capteur.py, vous devez compléter les méthodes pour générer les données brutes vers Pandas 
    et par la suite, les envoyer vers Kafka. 
    ---
    Le premier devra prendre le contenue du topic donnees_capteurs pour le stocker dans un HDFS. (Fichier Kafka-topic.py)
    Le deuxième devra prendre le contenue du HDFS pour nettoyer les données que vous avez besoin avec SPARK, puis le stocker en HDFS.
    Le troisième programme est un entraînement d'un modèle du machine learning (Vous utiliserez TensorFlow, avec une Régression Linéaire) (Fichier train.py)
    Un Quatrième programme qui va prédire une valeur en fonction de votre projet. (Fichier predict.py)
"""


def generate_dataframe(col):
    """
    Cette méthode permet de générer un DataFrame Pandas pour alimenter vos data
    """
    df = pd.DataFrame(columns=col)
    add_data(df)
    return df


def add_data(df: pd.DataFrame):
    """
    Cette méthode permet d'ajouter de la donnée vers votre DataFrame Pandas
    # on va creer une boucle avec 10k itérations, vous devez créer et ajouter des données dans Pandas à valeur aléatoire
    # Chaque itération comportera 1 ligne à ajouter. ex : timestamp = datetime.timedelta(seconds=1)
    # on va creer une liste avec les elements créees
    # ajouter une ligne au DataFrame
    # retourner le dataframe
    """
    percentage = 0
    for i in range(10000):
        timestamp = datetime.datetime.now() + datetime.timedelta(np.random.randint(low=1, high=120))
        height = np.random.normal(loc=20, scale=5)
        width = np.random.normal(loc=50, scale=10)
        door_number = np.random.randint(low=1, high=7)
        entrance_exit = bool(np.random.randint(0, 1))
        parking_spot = np.random.randint(low=0, high=500)
        df.loc[i] = [timestamp, height, width, door_number, entrance_exit, parking_spot]
        if i and i % 1000 == 0:
            percent = 100 - (10000 // i)
            if percent != percentage:
                print(f"Data generation at {percent}%")
                percentage = percent
            if i == 10000:
                print("Data generation at 100%")
                break
    return df


def write_data_minio(df: pd.DataFrame):
    """
    Cette méthode permet d'écrire le DataFrame vers Minio.
    (Obligatoire)
    ## on va tester si la bucket existe , dans le cas contraire on la crer
    ## on pousse le dataframe sur minio
    #decommentez le code du dessous
    """
    client = Minio(
        "localhost:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )
    if not client.bucket_exists("receiver"):
        client.make_bucket("receiver")

    timestamp = datetime.datetime.now().strftime('%d-%m-%y')
    df.to_csv(f"receiver_{timestamp}.csv", encoding='utf-8', index=False)
    client.fput_object("receiver", f"receiver_{timestamp}.csv", f"receiver_{timestamp}.csv")


if __name__ == "__main__":
    """""
    creer une liste column qui contient le header de votre dataframe 
    decommentez le code du dessous
    """""
    columns = ['timestamp', 'height', 'width', 'door_number', 'entrance_exit', 'parking_spot']
    df = generate_dataframe(columns)
    write_data_minio(df)
    print("Success")