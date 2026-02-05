from kafka import KafkaConsumer
import json
import uuid
from datetime import datetime, timezone

# Ρυθμίσεις Consumer
consumer = KafkaConsumer(
    "teamup.dvr.001",
    bootstrap_servers="10.8.0.1:9094",
    security_protocol="SSL",
    ssl_cafile="/home/skoumpmi/kafka_2.13-4.0.0/kafka-ssl-setup/ca.crt",
    auto_offset_reset="earliest",
    group_id="tls-test",
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    consumer_timeout_ms=60000
)

def transform_to_edxl_cap(kafka_msg):
    raw_data = kafka_msg.value
    
    # Λήψη δεδομένων για έλεγχο
    ext_category = raw_data.get("ext-Category", "")
    description_text = raw_data.get("description", "")
    
    # --- ΔΙΑΧΕΙΡΙΣΗ ATTACHMENTS (Πρώτα διαβάζουμε για να δούμε αν είναι κενά) ---
    attachments = raw_data.get("attachment") or []
    compounds_found = []
    measurement_val = 0

    # Parsing των attachments με ασφάλεια
    for att in attachments:
        if isinstance(att, dict):
            if att.get("name") == "Value (mV)":
                measurement_val = int(att.get("content", 0))
            elif att.get("name") == "Compounds Detected":
                # Παίρνουμε τη λίστα ή κενή λίστα αν είναι None
                compounds_found = att.get("content") or []

    # --- ΒΕΛΤΙΩΜΕΝΗ ΛΟΓΙΚΗ ΓΙΑ ALARM ---
    # Θεωρούμε ότι είναι "Clear" (Καθαρό) αν ισχύει ΕΝΑ από τα παρακάτω:
    # 1. Το ext-Category είναι "Analysis.End"
    # 2. Το description ξεκινάει με "No " (πιάνει και το "No Chemichal" με typo)
    # 3. Η λίστα compounds_found είναι άδεια (και το ext-Category δεν είναι Detected)
    
    is_clear = False
    
    if "Analysis.End" in ext_category:
        is_clear = True
    elif "No " in description_text:  # Πιάνει "No Chemical" και "No Chemichal"
        is_clear = True
    elif not compounds_found and "Detected" not in ext_category:
        is_clear = True

    # Ρύθμιση πεδίων CAP
    if is_clear:
        event_type = "Chemical Analysis End"
        urgency = "Past"
        severity = "Minor"
        certainty = "Observed"
        final_desc = "No Chemical Detected" # Το διορθώνουμε στο output
    else:
        event_type = "chemical"
        urgency = "Immediate"
        severity = "Severe"
        certainty = "Likely"
        final_desc = "Chemical Detected"

    # --- ΓΕΩΓΡΑΦΙΚΑ ---
    target_list = raw_data.get("target") or [{}]
    target = target_list[0] if target_list else {}
    geo_location = target.get("geoLocation", "0.0, 0.0")
    
    # --- ΚΑΤΑΣΚΕΥΗ JSON ---
    cap_message = {
        "alert": {
            "identifier": raw_data.get("id", str(uuid.uuid4())),
            "sender": raw_data.get("analyzer", {}).get("name", "Unknown"),
            "sent": raw_data.get("createTime", datetime.now(timezone.utc).isoformat()).replace('z', 'Z'),
            "status": "Actual",
            "msgType": "Alert",
            "scope": "Public",
            "code": "urn:oasis:names:tc:emergency:cap:1.2:profile:CAP-AU:1.0",
            "info": [
                {
                    "category": ["CBRNE"],
                    "event": event_type,
                    "urgency": urgency,
                    "severity": severity,
                    "certainty": certainty,
                    "description": final_desc,
                    "area": [
                        {
                            "areaDesc": "See the latitude and longtitude of the area",
                            "circle": [f"{geo_location.replace(' ', '')} 0.0"]
                        }
                    ],
                    "parameter": [
                        { "valueName": "version", "value": raw_data.get("version") },
                        { "valueName": "ext-Category", "value": raw_data.get("ext-Category") },
                        { "valueName": "ip", "value": raw_data.get("analyzer", {}).get("ip") },
                        { "valueName": "name", "value": raw_data.get("analyzer", {}).get("name") },
                        { "valueName": "type", "value": (raw_data.get("analyzer", {}).get("type") or [None])[0] },
                        { "valueName": "data", "value": (raw_data.get("analyzer", {}).get("data") or [None])[0] },
                        { "valueName": "method", "value": (raw_data.get("analyzer", {}).get("method") or [None])[0] },
                        { "valueName": "note_detail", "value": target.get("note") },
                        { 
                          "valueName": "measurement", 
                          "value": { "name": "Value (mV)", "contentType": "Integer", "content": measurement_val }
                        },
                        { 
                          "valueName": "compoundsDetected", 
                          "value": { "name": "Compounds Detected", "contentType": "Array", "content": compounds_found }
                        }
                    ]
                }
            ]
        }
    }
    
    # Επιστρέφουμε True αν ΔΕΝ είναι clear (άρα Alert)
    return cap_message, not is_clear

# --- Main Loop ---
print("Starting Robust Chemical Consumer...\n")

try:
    for message in consumer:
        cap_output, is_alert = transform_to_edxl_cap(message)
        
        # Εκτύπωση JSON
        print(json.dumps(cap_output, indent=2, ensure_ascii=False))
        
        # Έλεγχος για το Terminal Output
        if is_alert:
            print("\n🚨 ALERT: CHEMICAL AGENTS DETECTED! 🚨")
            # Τυπώνουμε και ποιες ουσίες βρέθηκαν
            compounds = cap_output['alert']['info'][0]['parameter'][-1]['value']['content']
            print(f"   Detected: {compounds}")
        else:
            print("\n🟢 STATUS: Analysis End - Area Clear (No threats).")
            
        print("-" * 50)

except Exception as e:
    print(f"Runtime Error: {e}")
finally:
    consumer.close()
