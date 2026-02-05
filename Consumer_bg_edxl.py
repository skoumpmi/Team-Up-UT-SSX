from kafka import KafkaConsumer
import json
import uuid
from datetime import datetime, timezone

# Σύνδεση στον Consumer
consumer = KafkaConsumer(
    "teamup.bgd.001",
    bootstrap_servers="10.8.0.1:9094",
    security_protocol="SSL",
    ssl_cafile="/home/skoumpmi/kafka_2.13-4.0.0/kafka-ssl-setup/ca.crt",
    auto_offset_reset="earliest",
    group_id="tls-test",
    # Αποκωδικοποίηση του εξωτερικού JSON
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    consumer_timeout_ms=60000
)

def transform_to_cap(message_payload):
    """
    Με βάση την εικόνα σου, το message_payload έχει τη μορφή:
    {
      "offset": ...,
      "value": { "id": "...", "latitude": ..., "Beta": ... }
    }
    """
    # Εδώ είναι το "κλειδί": Παίρνουμε το εσωτερικό 'value'
    inner_data = message_payload.get("value", {})
    
    # Μετατροπή Timestamp (χρησιμοποιούμε αυτό που είναι μέσα στο JSON αν υπάρχει, 
    # αλλιώς του Kafka)
    ts_raw = message_payload.get("timestamp", 0)
    dt_object = datetime.fromtimestamp(ts_raw / 1000.0, tz=timezone.utc)
    sent_time = dt_object.strftime('%Y-%m-%dT%H:%M:%SZ')

    # Εξαγωγή στοιχείων από το inner_data
    lat = inner_data.get("latitude")
    lon = inner_data.get("longitude")
    beta = inner_data.get("Beta")
    gamma = inner_data.get("Gamma")
    dev_id = inner_data.get("id", "Unknown")

    cap_message = {
        "alert": {
            "identifier": f"TEAMUP-CEA-{uuid.uuid4().hex[:8].upper()}",
            "sender": "BETA-GAMMA Probe",
            "sent": sent_time,
            "status": "Actual",
            "msgType": "Alert",
            "scope": "Restricted",
            "restriction": "For authorized recipients only",
            "code": "urn:oasis:names:tc:emergency:cap:1.2:profile:CAP-AU:1.0",
            "info": [
                {
                    "category": ["CBRNE"],
                    "event": "Radiological",
                    "urgency": "Immediate",
                    "severity": "Severe",
                    "certainty": "Likely",
                    "description": "Radiological Detected",
                    "area": [
                        {
                            "areaDesc": f"Point location near {lat},{lon}",
                            "circle": [f"{lat},{lon} 0.0"]
                        }
                    ],
                    "parameter": [
                        { "valueName": "message_id", "value": str(uuid.uuid4()) },
                        { "valueName": "id", "value": dev_id },
                        { "valueName": "s-1 Beta", "value": beta },
                        { "valueName": "s-1 gamma", "value": gamma },
                        { "valueName": "Beta Alarm", "value": "Y" if (beta and beta > 0) else "N" },
                        { "valueName": "gamma alarm", "value": "Y" if (gamma and gamma > 0) else "N" }
                    ]
                }
            ]
        }
    }
    return cap_message

print("Processing messages based on UI structure...\n")

for message in consumer:
    # Το message.value είναι όλο το JSON που βλέπουμε στην εικόνα σου
    cap_result = transform_to_cap(message.value)
    
    # Εκτύπωση του τελικού EDXL-CAP
    print(json.dumps(cap_result, indent=2))
    print("-" * 40)
