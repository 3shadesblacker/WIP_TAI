from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
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
    print("Opening buffer...")
    if not client.bucket_exists("buffer"):
        client.make_bucket("buffer")

    # Initialiser le consommateur Kafka
    print("Creating consumer...")
    consumer = KafkaConsumer('receiver',
                             bootstrap_servers=['localhost:9092'],
                             value_deserializer=lambda m: json.loads(m))

    # Boucle infinie pour lire les données Kafka
    print("Started reading data...")
    for message in consumer:
        print('Reading line...')
        line = message.value
        # Enregistrer les données dans le bucket MinIO
        try:
            # Définir le nom de l'objet
            object_name = f"{line['timestamp']}.json"
            # Encodage des données en JSON
            data_json = json.dumps(line).encode('utf-8')
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
        print("Success")


if __name__ == "__main__":
    main()
