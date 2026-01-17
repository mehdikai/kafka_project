from kafka import KafkaConsumer, KafkaProducer
import json
from datetime import datetime

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

def auto_correct_sale(sale):
    """Tente de corriger automatiquement une vente invalide"""
    corrected = sale.copy()
    corrections = []
    
    # 1. Corriger les valeurs vides avec des valeurs par défaut
    if not corrected.get('store'):
        corrected['store'] = 'S0'
        corrections.append("Store corrigé vers S0")
    
    if not corrected.get('product'):
        corrected['product'] = 'p0'
        corrections.append("Produit corrigé vers p0")
    
    # 2. Corriger les valeurs numériques invalides
    try:
        qty = float(corrected.get('qty', 0))
        if qty <= 0:
            corrected['qty'] = '1'
            corrections.append(f"Quantité corrigée: {qty} -> 1")
    except:
        corrected['qty'] = '1'
        corrections.append("Quantité non numérique corrigée vers 1")
    
    try:
        price = float(corrected.get('unitPrice', 0))
        if price <= 0:
            corrected['unitPrice'] = '10.0'
            corrections.append(f"Prix corrigé: {price} -> 10.0")
    except:
        corrected['unitPrice'] = '10.0'
        corrections.append("Prix non numérique corrigé vers 10.0")
    
    # 3. Corriger les formats invalides
    store = corrected.get('store', '')
    if store and not store.startswith('S'):
        corrected['store'] = 'S0'
        corrections.append(f"Format store corrigé: {store} -> S0")
    
    product = corrected.get('product', '')
    if product and not product.startswith('p'):
        corrected['product'] = 'p0'
        corrections.append(f"Format produit corrigé: {product} -> p0")
    
    # 4. Corriger les dates futures ou invalides
    if not corrected.get('eventTime'):
        corrected['eventTime'] = datetime.now().isoformat() + 'Z'
        corrections.append("Date vide corrigée vers maintenant")
    else:
        try:
            event_time = datetime.fromisoformat(corrected.get('eventTime', '').replace('Z', '+00:00'))
            if event_time > datetime.now().astimezone():
                corrected['eventTime'] = datetime.now().isoformat() + 'Z'
                corrections.append("Date future corrigée vers maintenant")
        except:
            corrected['eventTime'] = datetime.now().isoformat() + 'Z'
            corrections.append("Date invalide corrigée vers maintenant")
    
    return corrected, corrections

print("🔄 Stratégie de reprise DLQ démarrée\n")
print("Correction automatique des ventes en erreur\n")

try:
    for message in dlq_consumer:
        msg_value = message.value
        
        print(f"\n🔍 Vente en erreur trouvée:")
        print(f"   Store: {msg_value.get('store')}")
        print(f"   Produit: {msg_value.get('product')}")
        print(f"   Quantité: {msg_value.get('qty')}")
        print(f"   Prix: {msg_value.get('unitPrice')}")
        print(f"   Erreur(s): {msg_value.get('error_reason')}")
        
        # Tentative de correction
        print("\n🛠️  Tentative de correction automatique...")
        
        retry_count = msg_value.get('retry_count', 0)
        
        # Limiter les tentatives
        if retry_count < 3:
            # Corriger la vente
            corrected_sale, corrections = auto_correct_sale(msg_value)
            corrected_sale['retry_count'] = retry_count + 1
            
            # Supprimer les métadonnées d'erreur avant renvoi
            corrected_sale.pop('error_reason', None)
            corrected_sale.pop('original_topic', None)
            corrected_sale.pop('original_partition', None)
            corrected_sale.pop('original_offset', None)
            corrected_sale.pop('error_timestamp', None)
            
            print(f"✅ Corrections appliquées:")
            for correction in corrections:
                print(f"   - {correction}")
            
            # Renvoyer vers topic principal
            retry_producer.send('tp8-input', corrected_sale)
            print(f"\n📤 Vente corrigée et renvoyée vers tp8-input")
            print(f"   Tentative: {corrected_sale['retry_count']}/3")
            
            # Commit offset après traitement réussi
            dlq_consumer.commit()
        else:
            print(f"⚠️  Trop de tentatives ({retry_count}), vente abandonnée")
            print(f"   Cette vente nécessite une intervention manuelle")
            dlq_consumer.commit()
        
        print("-" * 70)
        
except KeyboardInterrupt:
    print("\n⚠️  Arrêt de la stratégie de reprise...")
finally:
    dlq_consumer.close()
    retry_producer.close()
    print("✨ Processus de reprise fermé")