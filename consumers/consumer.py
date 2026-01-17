from kafka import KafkaConsumer, KafkaProducer
import json
from datetime import datetime

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

def validate_sale(sale):
    """Valide une vente et retourne (is_valid, error_reason)"""
    errors = []
    
    # 1. Vérifier les valeurs vides
    required_fields = ['eventTime', 'store', 'product', 'qty', 'unitPrice']
    for field in required_fields:
        if not sale.get(field) or sale.get(field) == '':
            errors.append(f"Champ vide: {field}")
    
    # 2. Vérifier la date future
    try:
        event_time = datetime.fromisoformat(sale.get('eventTime', '').replace('Z', '+00:00'))
        if event_time > datetime.now().astimezone():
            errors.append(f"Date future détectée: {sale.get('eventTime')}")
    except:
        errors.append(f"Format de date invalide: {sale.get('eventTime')}")
    
    # 3. Vérifier les valeurs numériques négatives
    try:
        qty = float(sale.get('qty', 0))
        if qty <= 0:
            errors.append(f"Quantité invalide: {qty}")
    except:
        errors.append(f"Quantité non numérique: {sale.get('qty')}")
    
    try:
        price = float(sale.get('unitPrice', 0))
        if price <= 0:
            errors.append(f"Prix invalide: {price}")
    except:
        errors.append(f"Prix non numérique: {sale.get('unitPrice')}")
    
    # 4. Vérifier format store et product
    store = sale.get('store', '')
    if store and not store.startswith('S'):
        errors.append(f"Format store invalide: {store}")
    
    product = sale.get('product', '')
    if product and not product.startswith('p'):
        errors.append(f"Format produit invalide: {product}")
    
    if errors:
        return False, " | ".join(errors)
    return True, None

print("🔍 Consumer démarré, en attente de messages...\n")

try:
    for message in consumer:
        msg_value = message.value
        
        # Validation
        is_valid, error_reason = validate_sale(msg_value)
        
        if is_valid:
            # Message valide - Traitement normal
            total = float(msg_value.get('qty', 0)) * float(msg_value.get('unitPrice', 0))
            print(f"✅ VENTE VALIDE - Store: {msg_value.get('store')}, "
                  f"Produit: {msg_value.get('product')}, "
                  f"Total: {total}€")
        else:
            # Message invalide - Envoi vers DLQ
            print(f"❌ VENTE INVALIDE détectée")
            print(f"   Erreur: {error_reason}")
            
            # Enrichir avec métadonnées d'erreur
            dlq_message = {
                **msg_value,
                'error_reason': error_reason,
                'original_topic': message.topic,
                'original_partition': message.partition,
                'original_offset': message.offset,
                'error_timestamp': datetime.now().isoformat()
            }
            
            # Envoyer vers DLQ
            dlq_producer.send('tp8-dlq', dlq_message)
            print(f"📮 Envoyé vers DLQ\n")
            
except KeyboardInterrupt:
    print("\n⚠️  Arrêt du consumer...")
finally:
    consumer.close()
    dlq_producer.close()
    print("✨ Consumer fermé proprement")