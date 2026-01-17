# TP8 - Dead Letter Queue (DLQ) avec Kafka

Implémentation du pattern Dead Letter Queue pour gérer les messages en erreur dans Kafka.

## 📋 Objectifs

- Mettre en place un pattern de Dead Letter Queue
- Distinguer les messages valides des messages en erreur
- Rediriger les messages invalides vers un topic de DLQ
- Implémenter une stratégie de reprise des messages en erreur

## 🏗️ Architecture

```
Topic Principal (tp8-input)
         ↓
    Consumer
    /      \
   ✅       ❌
Valide   Invalide
   ↓         ↓
Traité    DLQ (tp8-dlq)
            ↓
       DLQ Consumer
            ↓
      Retry Strategy
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
pip install kafka-python
```

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
└── README.md
```

## 🔄 Stratégie de Reprise

Le fichier `dlq_retry.py` implémente 3 stratégies:

1. **Retry automatique** - Correction et renvoi (max 3 tentatives)
2. **Analyse manuelle** - Inspection et correction manuelle
3. **Archivage** - Conservation pour audit

Pour lancer la reprise:
```bash
python dlq_retry.py
```

## 📊 Format des Messages

**Message Valide:**
```json
{
  "id": 1,
  "type": "VALID",
  "data": "Message valide 1"
}
```

**Message Invalide (DLQ):**
```json
{
  "id": 3,
  "type": "INVALID",
  "data": "Message invalide 1",
  "error_reason": "Type invalide",
  "original_topic": "tp8-input",
  "original_partition": 0,
  "original_offset": 2
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
