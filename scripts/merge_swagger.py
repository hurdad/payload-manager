#!/usr/bin/env python3
"""Merge per-service swagger files into a single apidocs.swagger.json."""
import glob
import json
import os
import sys

service_files = sorted(
    glob.glob("gateway/openapi/payload/manager/services/v1/*.swagger.json")
)
if not service_files:
    sys.exit("No service swagger files found under gateway/openapi/")

merged = {}
for f in service_files:
    d = json.load(open(f))
    if not merged:
        merged = d
    else:
        merged.setdefault("paths", {}).update(d.get("paths", {}))
        merged.setdefault("definitions", {}).update(d.get("definitions", {}))

os.makedirs("gateway/openapi", exist_ok=True)
with open("gateway/openapi/apidocs.swagger.json", "w") as out:
    json.dump(merged, out, indent=2)

print(
    f"merged {len(merged.get('paths', {}))} paths into"
    " gateway/openapi/apidocs.swagger.json"
)
