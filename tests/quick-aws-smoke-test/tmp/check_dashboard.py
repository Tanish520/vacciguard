import json
with open("tests/quick-aws-smoke-test/tmp/dashboard.json") as f:
    d = json.load(f)
meta = d.get("meta", {})
dash = d.get("dashboard", {})
print("Title:", dash.get("title", "?"))
panels = dash.get("panels", [])
print("Panels:", len(panels))
for p in panels:
    print(f'  [{p.get("id","?")}] {p.get("title","?")} ({p.get("type","?")})')
