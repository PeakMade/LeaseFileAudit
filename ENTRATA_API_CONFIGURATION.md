# Entrata API Configuration

This document contains all Entrata API connection details, credentials, and endpoints for the LeaseFileAudit application.

---

## Production Environment

### Organization
- **Organization Name:** `peakmade`
- **API Key:** `8e383808-eeae-4aa7-b838-08eeae7aa7e2`
- **Password:** `158a926R44!100419` *(Note: app doesn't use this - stored for reference only)*

### API Endpoints

#### Lease Details API
- **URL:** `https://apis.entrata.com/ext/orgs/peakmade/v1/leases?page_no=1&per_page=100`
- **Method:** `getLeaseDetails`
- **Version:** `r2`
- **Key Header:** `X-Api-Key`

#### AR Transactions API
- **URL:** `https://apis.entrata.com/ext/orgs/peakmade/v1/artransactions?page_no=1&per_page=100`
- **Method:** `getLeaseArTransactions`
- **Version:** `r1`
- **Key Header:** `X-Api-Key`

---

## Sandbox Environment

### Organization
- **Organization Name:** `peakmade-test-17291`
- **API Key:** `8e383808-eeae-4aa7-b838-08eeae7aa7e2`

### API Endpoints

#### Lease Details API (Sandbox)
- **URL:** `https://apis.entrata.com/ext/orgs/peakmade-test-17291/v1/leases?page_no=1&per_page=100`

#### AR Transactions API (Sandbox)
- **URL:** `https://apis.entrata.com/ext/orgs/peakmade-test-17291/v1/artransactions?page_no=1&per_page=100`

---

## Environment Variables

For easy integration into a new application, use these environment variable names:

```env
# Entrata API Configuration - Production
ENTRATA_API_KEY=8e383808-eeae-4aa7-b838-08eeae7aa7e2
ENTRATA_ORG=peakmade
ENTRATA_PASSWORD=158a926R44!100419

# Lease API Configuration
LEASE_API_KEY=8e383808-eeae-4aa7-b838-08eeae7aa7e2
LEASE_API_KEY_HEADER=X-Api-Key
LEASE_API_DETAILS_URL=https://apis.entrata.com/ext/orgs/peakmade/v1/leases?page_no=1&per_page=100
LEASE_API_AR_URL=https://apis.entrata.com/ext/orgs/peakmade/v1/artransactions?page_no=1&per_page=100
LEASE_API_DETAILS_METHOD=getLeaseDetails
LEASE_API_DETAILS_VERSION=r2
LEASE_API_AR_METHOD=getLeaseArTransactions
LEASE_API_AR_VERSION=r1

# Entrata API Configuration - Sandbox
LEASE_API_SANDBOX_DETAILS_URL=https://apis.entrata.com/ext/orgs/peakmade-test-17291/v1/leases?page_no=1&per_page=100
LEASE_API_SANDBOX_AR_URL=https://apis.entrata.com/ext/orgs/peakmade-test-17291/v1/artransactions?page_no=1&per_page=100
LEASE_API_SANDBOX_KEY=8e383808-eeae-4aa7-b838-08eeae7aa7e2
ENTRATA_API_SANDBOX_KEY=8e383808-eeae-4aa7-b838-08eeae7aa7e2
ENTRATA_SANDBOX_ORG=peakmade-test-17291
```

---

## API Usage Notes

### Authentication
- All API calls require the `X-Api-Key` header with the API key value
- The same API key is used for both production and sandbox environments

### Pagination
- Both endpoints support pagination via `page_no` and `per_page` query parameters
- Default values shown: `page_no=1&per_page=100`
- Adjust these parameters as needed for your data volume

### API Methods
- Lease Details uses JSON-RPC method: `getLeaseDetails` (version r2)
- AR Transactions uses JSON-RPC method: `getLeaseArTransactions` (version r1)

### Environment Switching
- The application supports toggling between production (`peakmade`) and sandbox (`peakmade-test-17291`)
- Use the settings page or environment variable to control which environment is active

---

## Integration Example

```python
import os
import requests

# Get credentials from environment
api_key = os.getenv('ENTRATA_API_KEY')
org = os.getenv('ENTRATA_ORG')  # or ENTRATA_SANDBOX_ORG
lease_url = os.getenv('LEASE_API_DETAILS_URL')  # or LEASE_API_SANDBOX_DETAILS_URL

# Make API request
headers = {
    'X-Api-Key': api_key,
    'Content-Type': 'application/json'
}

payload = {
    "method": "getLeaseDetails",
    "params": {
        # your parameters here
    }
}

response = requests.post(lease_url, json=payload, headers=headers)
data = response.json()
```

---

## Security Notes

⚠️ **Important:** Keep these credentials secure and never commit them to public repositories.

- Store credentials in environment variables or secure configuration management
- Use `.env` files for local development (add to `.gitignore`)
- Use Azure Key Vault, AWS Secrets Manager, or similar for production deployments
- Rotate API keys periodically according to your security policy

---

*Last Updated: 2026-07-08*
