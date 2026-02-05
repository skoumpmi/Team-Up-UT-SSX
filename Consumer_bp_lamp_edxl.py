from kafka import KafkaConsumer
import json
import uuid
from datetime import datetime, timezone

# Ρυθμίσεις Consumer
consumer = KafkaConsumer(
    "teamup.bp-lamp.dimbio",
    bootstrap_servers="10.8.0.1:9094",
    security_protocol="SSL",
    ssl_cafile="/home/skoumpmi/kafka_2.13-4.0.0/kafka-ssl-setup/ca.crt",
    auto_offset_reset="earliest",
    group_id="tls-test",
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    consumer_timeout_ms=60000
)

def transform_bp_lamp_to_cap(kafka_msg_value):
    """
    Μετατρέπει το μήνυμα του Backpack LAMP σε EDXL-CAP.
    Υποστηρίζει τη δομή: value -> samples -> test_results
    """
    
    # 1. Εξαγωγή δεδομένων από το value
    raw_data = kafka_msg_value.get("value", {})
    
    # --- ΔΙΟΡΘΩΣΗ ΕΔΩ ---
    # Παίρνουμε τη λίστα των samples. Αν δεν υπάρχει, παίρνουμε κενή λίστα.
    samples_list = raw_data.get("samples", [])
    
    positive_agents = []
    all_test_results = []
    sample_names = []

    # 2. Iteration μέσα στα samples και μετά στα test_results
    for sample in samples_list:
        sample_name = sample.get("sample_name", "Unknown")
        sample_names.append(sample_name)
        
        # Λήψη των αποτελεσμάτων για αυτό το sample
        results = sample.get("test_results", [])
        
        for test_item in results:
            # Κρατάμε όλα τα αποτελέσματα για το parameter list αργότερα
            # Προσθέτουμε και το sample_name για να ξέρουμε από ποιο δείγμα προήλθε
            test_item_with_meta = test_item.copy()
            test_item_with_meta['source_sample'] = sample_name
            all_test_results.append(test_item_with_meta)
            
            # Έλεγχος αν είναι Positive
            if test_item.get("result") == "Positive":
                positive_agents.append(test_item.get("test"))

    # Καθορισμός αν έχουμε Alert
    is_alert = len(positive_agents) > 0

    # 3. Καθορισμός Κρισιμότητας (Severity/Urgency)
    if is_alert:
        event_type = "Biological"
        urgency = "Immediate"
        severity = "Severe"
        certainty = "Likely"
        description = "Biological Detected"
        # Χρησιμοποιούμε set() για να μην έχουμε διπλότυπα ονόματα αν βρέθηκε σε 2 δείγματα
        unique_agents = list(set(positive_agents))
        headline = f"DANGER: Detected Agents: {', '.join(unique_agents)}"
    else:
        event_type = "Biological Test Negative"
        urgency = "Past"
        severity = "Minor"
        certainty = "Observed"
        description = "No Biological Threat"
        headline = "Routine Scan - All Negative"

    # 4. Διαχείριση Χρόνου
    ts_raw = kafka_msg_value.get("timestamp", 0)
    try:
        dt_object = datetime.fromtimestamp(ts_raw / 1000.0, tz=timezone.utc)
        sent_time = dt_object.strftime('%Y-%m-%dT%H:%M:%SZ')
    except:
        sent_time = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

    # 5. Κατασκευή λίστας παραμέτρων (Parameters)
    parameters = [
        { "valueName": "offset", "value": kafka_msg_value.get("offset") },
        { "valueName": "partition", "value": str(kafka_msg_value.get("partition")) },
        { "valueName": "timestamp", "value": ts_raw },
        { "valueName": "sample_names", "value": ", ".join(sample_names) }
    ]

    # Προσθήκη όλων των αποτελεσμάτων στα parameters
    for test_res in all_test_results:
        parameters.append({
            "valueName": "test_results",
            "value": test_res
        })

    # 6. Δημιουργία EDXL-CAP JSON
    cap_message = {
        "alert": {
            "identifier": f"BP-LAMP-PROD-{uuid.uuid4().hex[:12].upper()}",
            "sender": "BP-LAMP",
            "sent": sent_time,
            "status": "Actual",
            "msgType": "Alert",
            "scope": "Restricted",
            "code": "urn:oasis:names:tc:emergency:cap:1.2:profile:CAP-AU:1.0",
            "info": [
                {
                    "category": ["CBRNE"],
                    "event": event_type,
                    "urgency": urgency,
                    "severity": severity,
                    "certainty": certainty,
                    "description": description,
                    "headline": headline,
                    "area": [
                        {
                            "areaDesc": None,
                            "circle": ["37.933687,23.745275 0.0"]
                        }
                    ],
                    "parameter": parameters
                }
            ]
        }
    }
    
    return cap_message, positive_agents

# --- Main Execution ---
print("Listening for Biosensor Backpack messages (Nested Structure)...\n")

try:
    for message in consumer:
        # Το message.value έχει όλη τη δομή (offset, timestamp, value: { samples: [...] })
        cap_output, detected_agents = transform_bp_lamp_to_cap(message.value)
        
        # Εκτύπωση JSON
        print(json.dumps(cap_output, indent=2))
        
        # Οπτική Ειδοποίηση
        if detected_agents:
            unique_agents = list(set(detected_agents))
            print("\n☣️  BIOHAZARD ALERT: POSITIVE TESTS FOUND! ☣️")
            print(f"   Agents Detected: {unique_agents}")
        else:
            print("\n✅ Status: Negative (All Clear)")
            
        print("-" * 50)

except Exception as e:
    print(f"Error processing message: {e}")
finally:
    consumer.close()
