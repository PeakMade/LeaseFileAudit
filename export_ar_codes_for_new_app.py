"""
Export AR codes in multiple formats for easy import into new apps
"""
import json
import csv
import re

# Read the file and extract codes
with open('ar_code_name_usage_map.json', 'r', encoding='utf-8') as f:
    content = f.read()

pattern = r'"(\d{6})"\s*:\s*\{\s*"code"\s*:\s*(\d+),\s*"name"\s*:\s*"([^"]+)",\s*"usage"\s*:\s*"([^"]+)"'
matches = re.findall(pattern, content)

codes = []
for ar_code, code_num, name, usage in sorted(matches, key=lambda x: int(x[0])):
    codes.append({
        'ar_code': int(ar_code),
        'code_number': int(code_num),
        'name': name,
        'usage_category': usage
    })

print(f"Extracted {len(codes)} AR codes")

# Export as clean JSON
with open('ar_codes_export.json', 'w') as f:
    json.dump(codes, f, indent=2)
print("✅ Saved: ar_codes_export.json")

# Export as CSV
with open('ar_codes_export.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=['ar_code', 'code_number', 'name', 'usage_category'])
    writer.writeheader()
    writer.writerows(codes)
print("✅ Saved: ar_codes_export.csv")

# Export as Python dict for easy copy-paste
with open('ar_codes_export.py', 'w') as f:
    f.write('# AR Codes Dictionary\n')
    f.write('# Use this in your Python app\n\n')
    f.write('AR_CODES = {\n')
    for code in codes:
        f.write(f"    {code['ar_code']}: {{\n")
        f.write(f"        'name': '{code['name']}',\n")
        f.write(f"        'usage_category': '{code['usage_category']}',\n")
        f.write(f"        'code_number': {code['code_number']}\n")
        f.write(f"    }},\n")
    f.write('}\n')
print("✅ Saved: ar_codes_export.py")

# Summary by usage category
from collections import defaultdict
by_usage = defaultdict(list)
for code in codes:
    by_usage[code['usage_category']].append(code['ar_code'])

print("\n📊 Summary by Usage Category:")
print("-" * 50)
for usage, code_list in sorted(by_usage.items()):
    print(f"{usage:<20}: {len(code_list):>3} codes")

print("\n✨ Export complete! Files created:")
print("   - ar_codes_export.json (clean JSON)")
print("   - ar_codes_export.csv (spreadsheet)")
print("   - ar_codes_export.py (Python dictionary)")
