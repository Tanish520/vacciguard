import json
with open("tests/quick-aws-smoke-test/tmp/query_result.json") as f:
    d = json.load(f)
print("Status:", d["status"])
for r in d["data"]["result"]:
    name = r["metric"].get("__name__", "?")
    val = r["value"]
    print(f"  {name}: {val[1]} at {val[0]}")
if not d["data"]["result"]:
    print("  No results yet - metrics may not have been scraped yet")
