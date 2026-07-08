"""
Extract all AR codes from ar_code_name_usage_map.json
"""
import json
import re

# Read the file as text first to handle potential PowerShell formatting
with open('ar_code_name_usage_map.json', 'r', encoding='utf-8') as f:
    content = f.read()

# Try to parse as JSON
try:
    data = json.loads(content)
except:
    # If that fails, try to extract codes using regex
    print("File is not valid JSON, extracting with regex...")
    pattern = r'"(\d{6})"\s*:\s*\{\s*"code"\s*:\s*\d+,\s*"name"\s*:\s*"([^"]+)",\s*"usage"\s*:\s*"([^"]+)"'
    matches = re.findall(pattern, content)
    
    print(f"\nTotal AR Codes Found: {len(matches)}\n")
    print("AR_CODE | NAME | USAGE")
    print("-" * 100)
    
    for ar_code, name, usage in sorted(matches, key=lambda x: int(x[0])):
        print(f"{ar_code} | {name} | {usage}")
    
    exit()

# If JSON parsing succeeded
codes = []
for ar_code, details in data.get('mapping', {}).items():
    codes.append({
        'ar_code': ar_code,
        'name': details.get('name', 'Unknown'),
        'usage': details.get('usage', 'N/A'),
        'code': details.get('code', 'N/A')
    })

codes.sort(key=lambda x: int(x['ar_code']))

print(f"Total AR Codes: {len(codes)}\n")
print("AR_CODE | NAME | USAGE | CODE")
print("-" * 120)

for code in codes:
    print(f"{code['ar_code']} | {code['name']:<60} | {code['usage']:<15} | {code['code']}")
