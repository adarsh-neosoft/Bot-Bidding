import hashlib, json
from datetime import datetime

def decision_inputs_hash(inputs: dict) -> str:
    s = json.dumps(inputs, sort_keys=True)
    return hashlib.sha256(s.encode('utf-8')).hexdigest()

def now_iso() -> str:
    return datetime.utcnow().isoformat() + 'Z'
