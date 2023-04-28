from kafka import KafkaConsumer
import json
from minio import Minio
from minio.error import S3Error


def main():
    # Initialiser le client MinIO
    client = Minio('localhost:9000',
                   access_key='minio',
                   secret_key='minio123',
                   secure=False)

    # Vérifier si le seau existe, sinon le créer
    if not client.bucket_exists("buffer"):
        client.make_bucket("buffer")

    # Initialiser le consommateur Kafka
    consumer = KafkaConsumer('receiver',
                             bootstrap_servers=['127.0.0.1:9092'],
                             value_deserializer=lambda m: json.loads(m.decode('utf-8')))

    # Boucle infinie pour lire les données Kafka
    for message in consumer:
        data = message.value
        # Enregistrer les données dans le seau MinIO
        try:
            # Définir le nom de l'objet
            object_name = f"{data['timestamp']}.json"
            # Encodage des données en JSON
            data_json = json.dumps(data).encode('utf-8')
            # Enregistrement des données dans le bucket MinIO
            client.put_object(
                "buffer",
                object_name,
                data_json,
                len(data_json),
                content_type='application/json'
            )
        except S3Error as e:
            print("Error:", e)


if __name__ == "__main__":
    main()
