import json
with open("tests/quick-aws-smoke-test/tmp/targets_final.json") as f:
    d = json.load(f)
for t in d["data"]["activeTargets"]:
    lbl = t.get("labels", {})
    job = lbl.get("job", "?")
    health = t.get("health", "?")
    addr = lbl.get("__address__", "?")
    err = t.get("lastError", "")
    print(f"{job:40s} {health:10s} {addr:30s} {err[:60] if err else 'OK'}")
