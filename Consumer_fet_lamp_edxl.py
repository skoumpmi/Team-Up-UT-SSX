from kafka import KafkaConsumer
import json
import uuid
from datetime import datetime, timezone

# Ρυθμίσεις Consumer
consumer = KafkaConsumer(
    "teamup.fet-lamp.dimbio",
    bootstrap_servers="10.8.0.1:9094",
    security_protocol="SSL",
    ssl_cafile="/home/skoumpmi/kafka_2.13-4.0.0/kafka-ssl-setup/ca.crt",
    auto_offset_reset="earliest",
    group_id="tls-test",
    # Το value_deserializer επιστρέφει το πλήρες JSON (με offset, timestamp, value: {...})
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    consumer_timeout_ms=60000
)

def transform_fet_lamp_to_cap(kafka_msg_value):
    """
    Δέχεται το value του Kafka message (που είναι dictionary)
    και το μετατρέπει σε EDXL-CAP.
    """
    # 1. Εξαγωγή των πραγματικών δεδομένων του αισθητήρα
    # Η δομή είναι: { "offset": ..., "value": { "device_name": ... } }
    sensor_data = kafka_msg_value.get("value", {})
    
    # 2. Έλεγχος αποτελέσματος (Positive/Negative)
    result = sensor_data.get("result", "unknown").lower()
    target_agent = sensor_data.get("target", "Unknown Agent")
    
    is_positive = (result == "positive")
    
    # 3. Καθορισμός Κρισιμότητας (Severity/Urgency)
    if is_positive:
        event_desc = "Biological Detected"
        severity = "Severe"
        urgency = "Immediate"
        certainty = "Likely"
        headline = f"Biological Threat Detected: {target_agent}"
    else:
        event_desc = "Biological Test Negative"
        severity = "Minor"
        urgency = "Past"
        certainty = "Observed"
        headline = "Routine Biological Scan - Negative"

    # 4. Διαχείριση Χρόνου (Timestamp από το JSON)
    # Παίρνουμε το timestamp από το JSON (όχι του Kafka object) όπως ζητήθηκε
    ts_raw = kafka_msg_value.get("timestamp", 0)
    try:
        dt_object = datetime.fromtimestamp(ts_raw / 1000.0, tz=timezone.utc)
        sent_time = dt_object.strftime('%Y-%m-%dT%H:%M:%SZ')
    except:
        sent_time = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

    # 5. Τοποθεσία (Hardcoded όπως στο παράδειγμα, αφού ο sensor δεν στέλνει GPS)
    fixed_location = "37.933687,23.745275"

    # 6. Δημιουργία EDXL-CAP JSON
    cap_message = {
        "alert": {
            "identifier": f"FET-LAMP-{uuid.uuid4().hex[:12].upper()}",
            "sender": "FET-LAMP",
            "sent": sent_time,
            "status": "Actual",
            "msgType": "Alert",
            "scope": "Restricted",
            "code": "urn:oasis:names:tc:emergency:cap:1.2:profile:CAP-AU:1.0",
            "info": [
                {
                    "category": ["CBRNE"],
                    "event": "Biological",
                    "urgency": urgency,
                    "severity": severity,
                    "certainty": certainty,
                    "description": event_desc,
                    "headline": headline,
                    "area": [
                        {
                            "areaDesc": "Sensor Location", # Ή null όπως στο παράδειγμα
                            "circle": [f"{fixed_location} 0.0"]
                        }
                    ],
                    "parameter": [
                        # Εδώ βάζουμε τα πεδία όπως ακριβώς ζητήθηκαν
                        { "valueName": "offset", "value": kafka_msg_value.get("offset") },
                        { "valueName": "partition", "value": str(kafka_msg_value.get("partition")) },
                        { "valueName": "timestamp", "value": ts_raw },
                        { "valueName": "device_name", "value": sensor_data.get("device_name") },
                        { "valueName": "sample_name", "value": sensor_data.get("sample_name") },
                        { "valueName": "target", "value": target_agent },
                        { "valueName": "result", "value": sensor_data.get("result") }
                    ]
                }
            ]
        }
    }
    
    return cap_message, is_positive

print("Listening for FET-LAMP Biological messages...\n")

try:
    for message in consumer:
        # Το message.value περιέχει όλο το JSON (μαζί με το wrapper offset/partition)
        # που έρχεται από τον producer.
        cap_output, alert_active = transform_fet_lamp_to_cap(message.value)
        
        # Εκτύπωση του JSON
        print(json.dumps(cap_output, indent=2))
        
        # Οπτική Ειδοποίηση
        if alert_active:
            target_name = cap_output['alert']['info'][0]['parameter'][-2]['value'] # Target
            print(f"\n☣️  BIOHAZARD ALERT: {target_name} DETECTED! ☣️")
        else:
            print("\n✅ Status: Negative Result (Clear)")
            
        print("-" * 50)

except Exception as e:
    print(f"Error processing message: {e}")
finally:
    consumer.close()
