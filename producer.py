from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Messages valides et invalides
messages = [
    {"id": 1, "type": "VALID", "data": "Message valide 1"},
    {"id": 2, "type": "VALID", "data": "Message valide 2"},
    {"id": 3, "type": "INVALID", "data": "Message invalide 1"},
    {"id": 4, "type": "VALID", "data": "Message valide 3"},
    {"id": 5, "type": "INVALID", "data": "Message invalide 2"},
    {"id": 6, "type": "INVALID", "data": "Message invalide 3"},
    {"id": 7, "type": "VALID", "data": "Message valide 4"},
]

print("📤 Envoi des messages...")
for msg in messages:
    producer.send('tp8-input', msg)
    print(f"✅ Envoyé: {msg}")
    time.sleep(0.5)

producer.flush()
producer.close()
print("\n✨ Tous les messages ont été envoyés!")