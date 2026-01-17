Dead Letter Queue (DLQ) avec Kafka

Implémentation du pattern Dead Letter Queue pour gérer les ventes invalides dans Kafka avec validation multi-critères.

## 📋 Objectifs

- Mettre en place un pattern de Dead Letter Queue
- Valider les données de ventes (CSV) avec plusieurs critères
- Rediriger les ventes invalides vers un topic de DLQ
- Implémenter une stratégie de correction automatique
- Limiter les tentatives de reprise (max 3)ur

## 🏗️ Architecture

```
sales.csv
    ↓
Producer (lit CSV)
    ↓
Topic Principal 
    ↓
Consumer (Validation)
  /      \
 ✅       ❌
Valid   Invalid
  ↓        ↓
Traité   DLQ (tp8-dlq)
           ↓
      DLQ Consumer
           ↓
    Retry Strategy (auto-correction)
           ↓
    Renvoi vers tp8-input (max 3x)
```

## 🚀 Démarrage

### Prérequis

- Docker & Docker Compose
- Python 3.8+
- pip

### Installation

1. **Démarrer Kafka:**
```bash
docker-compose up -d
```

2. **Créer les topics:**
```bash
# Topic principal
docker exec -it kafka_project-kafka-1 kafka-topics --create \
  --topic tp8-input \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1

# Topic DLQ
docker exec -it kafka_project-kafka-1 kafka-topics --create \
  --topic tp8-dlq \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

3. **Installer les dépendances Python:**
```bash
pip install requirements.txt
```
## ✅ Critères de Validation

Le consumer valide chaque vente selon ces critères:

1. **Champs vides** - Tous les champs requis doivent être remplis (eventTime, store, product, qty, unitPrice)
2. **Date future** - eventTime ne doit pas être dans le futur
3. **Valeurs négatives/nulles** - qty et unitPrice doivent être > 0
4. **Format invalide** - store doit commencer par 'S', product par 'p'
5. **Types numériques** - qty et unitPrice doivent être des nombres valides

## 🎯 Utilisation

### Lancer les consumers (dans des terminaux séparés)

**Terminal 1 - DLQ Consumer:**
```bash
cd consumers
python dlq_consumer.py
```

**Terminal 2 - Consumer Principal:**
```bash
cd consumers
python consumer.py
```

**Terminal 3 - Producer:**
```bash
python producer.py
```

## 📁 Structure du Projet

```
.
├── docker-compose.yml      # Configuration Kafka/Zookeeper
├── consumers
   ├── consumer.py            # Consumer principal avec logique DLQ
   └── dlq_consumer.py        # Consumer du topic DLQ
├── producer.py            # Producteur de messages valides/invalides
├── dlq_retry.py           # Stratégie de reprise des messages
├── sales.csv
└── README.md
```

## 🔄 Stratégie de Reprise (dlq_retry.py)

Le système de correction automatique applique les corrections suivantes:

### Corrections Automatiques:
- **Champs vides** → Valeurs par défaut (S0, p0)
- **Quantité invalide** → Corrigée vers 1
- **Prix invalide** → Corrigé vers 10.0
- **Format invalide** → Corrigé (S0 pour store, p0 pour product)
- **Date future/invalide** → Corrigée vers la date actuelle

### Limite de Retry:
- Maximum **3 tentatives** par vente
- Après 3 échecs → Intervention manuelle nécessaire
- Chaque tentative est trackée avec `retry_count`

Pour lancer la reprise:
```bash
python dlq_retry.py
```

## 📊 Format des Messages

**Vente Valide:**
```json
{
  "eventTime": "2026-01-10T12:00:15Z",
  "store": "S1",
  "product": "p4",
  "qty": "1",
  "unitPrice": "30.0"
}
```

**Vente Invalide → DLQ:**
```json
{
  "eventTime": "2026-01-28T12:00:51Z",
  "store": "S1",
  "product": "p4",
  "qty": "7",
  "unitPrice": "30.0",
  "error_reason": "Date future détectée: 2026-01-28T12:00:51Z",
  "original_topic": "tp8-input",
  "original_partition": 0,
  "original_offset": 3,
  "error_timestamp": "2026-01-17T10:56:03.123456"
}
```

## 🛠️ Commandes Utiles

**Lister les topics:**
```bash
docker exec -it kafka_project-kafka-1 kafka-topics --list \
  --bootstrap-server localhost:9092
```

**Lire un topic:**
```bash
docker exec -it kafka_project-kafka-1 kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic tp8-dlq \
  --from-beginning
```

**Arrêter Kafka:**
```bash
docker-compose down
```

## 📝 Notes

- Les messages de type `VALID` sont traités normalement
- Les messages de type `INVALID` sont redirigés vers la DLQ
- Chaque message en DLQ contient des métadonnées sur son origine
- La stratégie de retry limite les tentatives à 3 pour éviter les boucles infinies

---

⭐ **If you found this project helpful, please give it a star!** ⭐

**Built with ❤️ by EL Mehdi**
