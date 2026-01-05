import base64
import json
import gzip

def lambda_handler(event, context):
    out = []

    for r in event.get("records", []):
        record_id = r.get("recordId", "unknown")
        data_b64 = r.get("data", "")

        try:
            raw_bytes = base64.b64decode(data_b64)

            # CloudWatch->Firehose payload is gzipped JSON
            if raw_bytes.startswith(b"\x1f\x8b"):
                raw_bytes = gzip.decompress(raw_bytes)

            payload = raw_bytes.decode("utf-8", errors="replace").strip()
            if not payload:
                out.append({"recordId": record_id, "result": "Dropped", "data": data_b64})
                continue

            obj = json.loads(payload)

            if obj.get("messageType") != "DATA_MESSAGE":
                out.append({"recordId": record_id, "result": "Dropped", "data": data_b64})
                continue

            log_group = obj.get("logGroup", "")
            log_stream = obj.get("logStream", "")
            owner = obj.get("owner", "")

            lines = []
            for e in obj.get("logEvents", []):
                ts = e.get("timestamp", "")
                msg = (e.get("message", "") or "")
                msg = msg.replace("\n", "\\n").replace("\r", "\\r")
                msg = '"' + msg.replace('"', '""') + '"'
                lines.append(f"{ts},{msg},{log_group},{log_stream},{owner}")

            transformed = ("\n".join(lines) + "\n").encode("utf-8")

            out.append({
                "recordId": record_id,
                "result": "Ok",
                "data": base64.b64encode(transformed).decode("utf-8")
            })

        except Exception as ex:
            # log the exception so you can see the real cause in CloudWatch
            print("Transform error:", str(ex))
            out.append({"recordId": record_id, "result": "ProcessingFailed", "data": data_b64})

    return {"records": out}
