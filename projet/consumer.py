from kafka import KafkaConsumer
import json
from minio import Minio
from minio.error import S3Error

def main():
    # Initialiser le client MinIO

    minioClient = Minio('localhost:9000',
                    access_key='minio',
                    secret_key='minio123',
                    secure=False)

    # Vérifier si la bucket existe, sinon le créer


    # Initialiser le consommateur Kafka

    consumer = KafkaConsumer('capteur',
                            bootstrap_servers=['127.0.0.1:9092'],
                            value_deserializer=lambda m: json.loads(m.decode('utf-8')))

    # Boucle infinie pour lire les données Kafka
    # Enregistrer les données dans la bucket MinIO

        # Définir le nom de l'objet


        # Encodage des données en JSON


        # Enregistrement des données dans le bucket MinIO



if __name__ == "__main__":
  main()