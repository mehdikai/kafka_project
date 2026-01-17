from kafka import KafkaProducer
import json
import csv
import time

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("📤 Lecture du fichier sales.csv et envoi des messages...\n")

# Lire le CSV
with open('sales.csv', 'r') as file:
    csv_reader = csv.DictReader(file)
    count = 0
    
    for row in csv_reader:
        # Créer le message depuis la ligne CSV
        message = {
            'eventTime': row['eventTime'],
            'store': row['store'],
            'product': row['product'],
            'qty': row['qty'],
            'unitPrice': row['unitPrice']
        }
        
        # Envoyer vers Kafka
        producer.send('tp8-input', message)
        print(f"✅ Envoyé: {message}")
        count += 1
        time.sleep(0.2)

producer.flush()
producer.close()
print(f"\n✨ {count} messages envoyés depuis sales.csv!")