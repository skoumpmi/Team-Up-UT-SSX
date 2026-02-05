from kafka import KafkaConsumer
import json
import uuid
from datetime import datetime, timezone

# Ρυθμίσεις Consumer
consumer = KafkaConsumer(
    "teamup.rpd.001",
    bootstrap_servers="10.8.0.1:9094",
    security_protocol="SSL",
    ssl_cafile="/home/skoumpmi/kafka_2.13-4.0.0/kafka-ssl-setup/ca.crt",
    auto_offset_reset="earliest",
    group_id="tls-test",
    # 1ο Deserialization: Μετατρέπει το byte array του Kafka στο εξωτερικό JSON wrapper
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    consumer_timeout_ms=60000
)

def transform_rpd_to_cap(kafka_msg_value):
    """
    Μετατρέπει το μήνυμα του Rapid Explosive Sensor σε EDXL-CAP.
    Διαχειρίζεται Stringified JSON μέσα στο πεδίο 'value'.
    """
    
    # 1. Εξαγωγή του String από το wrapper
    # Το kafka_msg_value είναι: { "offset": 20, "value": "{\"duration\":...}" }
    raw_value_string = kafka_msg_value.get("value")
    
    # 2. 2ο Deserialization: Μετατροπή του string σε Dictionary
    try:
        if isinstance(raw_value_string, str):
            sensor_data = json.loads(raw_value_string)
        else:
            # Αν για κάποιο λόγο έρθει ήδη ως dict
            sensor_data = raw_value_string if raw_value_string else {}
    except json.JSONDecodeError:
        print("Error: Could not parse inner JSON string.")
        return None, False

    # 3. Λογική Ανίχνευσης (Explosive: 1 = Detected, 0 = Not Detected)
    is_explosive = sensor_data.get("explosive") == 1
    
    if is_explosive:
        event_desc = "Explosive Detected"
        severity = "Severe"
        urgency = "Immediate"
        certainty = "Likely"
        headline = "DANGER: Explosive Material Identified"
    else:
        event_desc = "Explosive Test Negative"
        severity = "Minor"
        urgency = "Past"
        certainty = "Observed"
        headline = "Routine Scan - Negative"

    # 4. Διαχείριση Χρόνου
    ts_raw = kafka_msg_value.get("timestamp", 0)
    try:
        dt_object = datetime.fromtimestamp(ts_raw / 1000.0, tz=timezone.utc)
        sent_time = dt_object.strftime('%Y-%m-%dT%H:%M:%SZ')
    except:
        sent_time = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

    # 5. Κατασκευή EDXL-CAP
    cap_message = {
        "alert": {
            "identifier": str(uuid.uuid4()), # Μοναδικό ID
            "sender": "H-BRS",
            "sent": sent_time,
            "status": "Actual",
            "msgType": "Alert",
            "scope": "Restricted",
            "info": [
                {
                    "category": ["CBRNE"],
                    "event": "explosion",
                    "urgency": urgency,
                    "severity": severity,
                    "certainty": certainty,
                    "description": event_desc,
                    "headline": headline,
                    "parameter": [
                        { "valueName": "duration_heater", "value": sensor_data.get("duration_heater") },
                        { "valueName": "rate", "value": sensor_data.get("rate") },
                        { "valueName": "power", "value": sensor_data.get("power") },
                        { "valueName": "classification", "value": "primary" if is_explosive else "none" },
                        { "valueName": "explosive", "value": sensor_data.get("explosive") },
                        { "valueName": "sampleID", "value": sensor_data.get("id") }
                    ],
                    "area": [
                        {
                            "areaDesc": None # Δεν παρέχεται τοποθεσία στο μήνυμα
                        }
                    ],
                    "resource": [
                        {
                            "resourceDesc": None,
                            "mimeType": None
                        }
                    ]
                }
            ]
        }
    }
    
    return cap_message, is_explosive

# --- Main Execution ---
print("Listening for Rapid Explosive Sensor messages...\n")

try:
    for message in consumer:
        # Περνάμε το value του μηνύματος (που περιέχει το wrapper)
        cap_output, alert_triggered = transform_rpd_to_cap(message.value)
        
        if cap_output:
            # Εκτύπωση JSON
            print(json.dumps(cap_output, indent=2))
            
            # Οπτική Ειδοποίηση
            if alert_triggered:
                print("\n💣 BOMB ALERT: EXPLOSIVES DETECTED! 💣")
            else:
                print("\n✅ Status: Negative (No Explosives)")
                
            print("-" * 50)

except Exception as e:
    print(f"System Error: {e}")
finally:
    consumer.close()
