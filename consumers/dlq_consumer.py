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

print("🚨 DLQ Consumer démarré - Lecture des ventes en erreur...\n")

try:
    for message in consumer:
        msg_value = message.value
        print(f"🔴 VENTE EN ERREUR:")
        print(f"   - EventTime: {msg_value.get('eventTime')}")
        print(f"   - Store: {msg_value.get('store')}")
        print(f"   - Produit: {msg_value.get('product')}")
        print(f"   - Quantité: {msg_value.get('qty')}")
        print(f"   - Prix: {msg_value.get('unitPrice')}")
        print(f"   - Raison: {msg_value.get('error_reason')}")
        print(f"   - Topic original: {msg_value.get('original_topic')}")
        print(f"   - Offset: {msg_value.get('original_offset')}")
        print(f"   - Timestamp erreur: {msg_value.get('error_timestamp')}")
        print("-" * 70 + "\n")
        
except KeyboardInterrupt:
    print("\n⚠️  Arrêt du DLQ consumer...")
finally:
    consumer.close()
    print("✨ DLQ Consumer fermé")