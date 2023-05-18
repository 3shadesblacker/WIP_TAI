import pandas as pd
from kafka import KafkaProducer
import datetime
import numpy as np

def add_datatokafka():
    # on va creer une boucle infinie, vous devez créer et ajouter des données dans Pandas à valeur aléatoire.
    # Chaque itération comportera 1 ligne à ajouter. ex : timestamp = datetime.timedelta(seconds=1)
    #  on va creer une liste avec les elements créees
    # on va crée le produceur kafka qui envoie la liste sur le topic qu'on a crée sur conduktor
    df = pd.DataFrame()
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

if __name__ == "__main__":
    add_datatokafka()
