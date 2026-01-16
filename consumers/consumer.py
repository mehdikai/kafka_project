from kafka import KafkaConsumer, KafkaProducer
import json

# Configuration Consumer
consumer = KafkaConsumer(
    'tp8-input',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='tp8-consumer-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Configuration Producer pour DLQ
dlq_producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("🔍 Consumer démarré, en attente de messages...\n")

try:
    for message in consumer:
        msg_value = message.value
        
        # Logique de validation
        if msg_value.get('type') == 'VALID':
            # Message valide - Traitement normal
            print(f"✅ MESSAGE VALIDE traité: {msg_value}")
        else:
            # Message invalide - Envoi vers DLQ
            print(f"❌ MESSAGE INVALIDE détecté: {msg_value}")
            
            # Enrichir avec métadonnées d'erreur
            dlq_message = {
                **msg_value,
                'error_reason': 'Type invalide',
                'original_topic': message.topic,
                'original_partition': message.partition,
                'original_offset': message.offset
            }
            
            # Envoyer vers DLQ
            dlq_producer.send('tp8-dlq', dlq_message)
            print(f"📮 Envoyé vers DLQ: {dlq_message}\n")
            
except KeyboardInterrupt:
    print("\n⚠️  Arrêt du consumer...")
finally:
    consumer.close()
    dlq_producer.close()
    print("✨ Consumer fermé proprement")