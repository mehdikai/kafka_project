from kafka import KafkaConsumer, KafkaProducer
import json

# Consumer DLQ
dlq_consumer = KafkaConsumer(
    'tp8-dlq',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    group_id='dlq-retry-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Producer pour renvoyer vers topic principal
retry_producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("🔄 Stratégie de reprise DLQ démarrée\n")
print("Correction automatique et renvoi")

try:
    for message in dlq_consumer:
        msg_value = message.value
        
        print(f"\n🔍 Message en erreur trouvé:")
        print(f"   ID: {msg_value.get('id')}")
        print(f"   Data: {msg_value.get('data')}")
        print(f"   Erreur: {msg_value.get('error_reason')}")
        
        # Simulation: Correction automatique
        # Dans un cas réel, vous pourriez avoir une logique de correction
        print("\n🛠️  Tentative de correction...")
        
        # Corriger le message
        corrected_message = {
            'id': msg_value.get('id'),
            'type': 'VALID',  # Correction du type
            'data': msg_value.get('data') + ' [CORRIGÉ]',
            'retry_count': msg_value.get('retry_count', 0) + 1
        }
        
        # Limiter les tentatives
        if corrected_message['retry_count'] < 3:
            # Renvoyer vers topic principal
            retry_producer.send('tp8-input', corrected_message)
            print(f"✅ Message corrigé et renvoyé: {corrected_message}")
            
            # Commit offset après traitement réussi
            dlq_consumer.commit()
        else:
            print(f"⚠️  Trop de tentatives, message abandonné")
            dlq_consumer.commit()
        
        print("-" * 60)
        
except KeyboardInterrupt:
    print("\n⚠️  Arrêt de la stratégie de reprise...")
finally:
    dlq_consumer.close()
    retry_producer.close()
    print("✨ Processus de reprise fermé")