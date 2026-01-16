from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'tp8-dlq',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='dlq-consumer-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("🚨 DLQ Consumer démarré - Lecture des messages en erreur...\n")

try:
    for message in consumer:
        msg_value = message.value
        print(f"🔴 MESSAGE EN ERREUR:")
        print(f"   - ID: {msg_value.get('id')}")
        print(f"   - Data: {msg_value.get('data')}")
        print(f"   - Raison: {msg_value.get('error_reason')}")
        print(f"   - Topic original: {msg_value.get('original_topic')}")
        print(f"   - Offset original: {msg_value.get('original_offset')}")
        print("-" * 50 + "\n")
        
except KeyboardInterrupt:
    print("\n⚠️  Arrêt du DLQ consumer...")
finally:
    consumer.close()
    print("✨ DLQ Consumer fermé")