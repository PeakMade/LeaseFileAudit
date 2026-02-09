import pandas as pd

# Load normalized expected detail
df = pd.read_csv('instance/runs/run_20260205_171911/inputs_normalized/expected_detail.csv')

# Find Nathan
nathan = df[df['CUSTOMER_NAME'].str.contains('NATHAN', case=False, na=False)]

print('Nathan lease IDs:', nathan['LEASE_INTERVAL_ID'].unique())
print('\nNathan scheduled charges:')
print(nathan[['SCHEDULED_CHARGES_ID', 'LEASE_INTERVAL_ID', 'AR_CODE_ID', 'AR_CODE_NAME', 'PERIOD_START', 'PERIOD_END']].head(10))

print('\n\nPERIOD_START dtype:', nathan['PERIOD_START'].dtype)
print('PERIOD_END dtype:', nathan['PERIOD_END'].dtype)

print('\nSample PERIOD_START values:')
for i, val in enumerate(nathan['PERIOD_START'].head(5)):
    print(f'  Row {i}: {repr(val)} (type: {type(val).__name__})')

print('\nSample PERIOD_END values:')
for i, val in enumerate(nathan['PERIOD_END'].head(5)):
    print(f'  Row {i}: {repr(val)} (type: {type(val).__name__})')
