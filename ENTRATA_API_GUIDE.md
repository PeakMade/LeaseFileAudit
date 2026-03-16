# Entrata API Integration Guide

**Complete beginner's guide to connecting to and using the Entrata API in the Lease File Audit application.**

---

## Table of Contents

1. [What is an API?](#what-is-an-api)
2. [How This Application Uses Entrata](#how-this-application-uses-entrata)
3. [Required Access & Credentials](#required-access--credentials)
4. [Environment Configuration](#environment-configuration)
5. [API Endpoints & Methods](#api-endpoints--methods)
6. [Request Structure](#request-structure)
7. [Response Structure](#response-structure)
8. [Common Scenarios](#common-scenarios)
9. [Troubleshooting](#troubleshooting)
10. [Security Best Practices](#security-best-practices)

---

## What is an API?

### API Basics

**API** stands for **Application Programming Interface**. Think of it as a messenger that:

1. Takes your request
2. Sends it to a system (like Entrata)
3. Gets the response
4. Brings it back to you

**Real-world analogy:** When you order food at a restaurant, you don't go into the kitchen yourself. You tell the waiter what you want, the waiter tells the kitchen, and then brings you your food. An API is like that waiter.

### How APIs Work

```
Your Application  →  API Request  →  Entrata Server
                                         |
                                    (processes request)
                                         |
Your Application  ←  API Response  ←  Entrata Server
```

### Key Concepts

- **Endpoint**: The specific URL where you send your request (like a specific restaurant location)
- **Request**: What you're asking for (the data you want)
- **Response**: What you get back (the data Entrata sends you)
- **API Key**: Your password/credentials to prove you're allowed to make requests
- **Method**: The type of action you want to perform (get lease details, get transactions, etc.)

---

## How This Application Uses Entrata

### Overview

The Lease File Audit application connects to Entrata to fetch two types of data:

1. **Lease Details** - Scheduled charges, lease terms, customer info
2. **AR Transactions** - Actual billed amounts, payment history, ledger data

This data is then compared to find discrepancies between what was scheduled and what was actually billed.

### Two Main Use Cases

#### 1. **Full Property Audit** (All Leases)
- Fetches all leases for a property
- Processes hundreds/thousands of residents
- Used for comprehensive portfolio analysis

#### 2. **Single Lease Audit** (One Resident)
- Fetches data for just one lease
- Used for debugging specific resident issues
- Faster and more targeted

### Data Flow

```
1. User initiates audit (selects property or lease)
   ↓
2. Application sends API request to Entrata
   ↓
3. Entrata returns JSON data (lease details + transactions)
   ↓
4. Application transforms data into standard format
   ↓
5. Application reconciles scheduled vs actual
   ↓
6. Results saved to SharePoint
```

---

## Required Access & Credentials

### What You Need

Before you can use the Entrata API, you need these items from Entrata:

#### 1. **API Key** (Required)
- Your unique authentication token
- Looks like: `abc123xyz789...` (random alphanumeric string)
- **How to get it:** Contact your Entrata account manager or support team
- **Never share this publicly** - treat it like a password

#### 2. **Organization Name** (Required)
- Your Entrata organization identifier
- Example: `peakmade`
- This appears in your Entrata URLs: `https://peakmade.entrata.com/`

#### 3. **API Access Permissions** (Required)
You need permission to call these specific Entrata API methods:
- `getLeaseDetails` (version r2)
- `getLeaseArTransactions` (version r1)

**How to verify:** Ask your Entrata administrator to confirm your API key has access to these methods.

### Getting Credentials

**Step-by-step process:**

1. **Contact Entrata Support**
   - Email: support@entrata.com
   - Or contact your account manager

2. **Request API Access**
   - Say: "I need API access for lease data integration"
   - Specify you need: `getLeaseDetails` and `getLeaseArTransactions`

3. **Receive API Key**
   - They will provide you with an API key
   - Save it securely (password manager recommended)

4. **Confirm Organization Name**
   - Usually the same as your Entrata portal subdomain
   - Example: If you log in at `companyname.entrata.com`, your org is likely `companyname`

---

## Environment Configuration

### Required Environment Variables

The application reads Entrata API configuration from **environment variables** (secure settings that don't go in code).

#### **Windows (PowerShell)**

```powershell
# Core API credentials
$env:LEASE_API_KEY = "your-api-key-here"
$env:ENTRATA_ORG = "peakmade"  # or your organization name

# API Endpoints (use defaults unless Entrata specifies otherwise)
$env:LEASE_API_DETAILS_URL = "https://apis.entrata.com/ext/orgs/peakmade/v1/leases"
$env:LEASE_API_AR_URL = "https://apis.entrata.com/ext/orgs/peakmade/v1/artransactions"

# Optional: Method configuration (defaults shown)
$env:LEASE_API_DETAILS_METHOD = "getLeaseDetails"
$env:LEASE_API_DETAILS_VERSION = "r2"
$env:LEASE_API_AR_METHOD = "getLeaseArTransactions"
$env:LEASE_API_AR_VERSION = "r1"
```

#### **Azure App Service Configuration**

If deploying to Azure App Service:

1. Go to **App Service** → **Configuration** → **Application settings**
2. Add each environment variable as a new setting:

| Name | Value | Notes |
|------|-------|-------|
| `LEASE_API_KEY` | `your-api-key` | **Required** - Your Entrata API key |
| `ENTRATA_ORG` | `peakmade` | Your organization name |
| `LEASE_API_DETAILS_URL` | `https://apis.entrata.com/ext/orgs/{org}/v1/leases` | Replace `{org}` with your org |
| `LEASE_API_AR_URL` | `https://apis.entrata.com/ext/orgs/{org}/v1/artransactions` | Replace `{org}` with your org |

3. Click **Save** and **Restart** the app service

### All Available Configuration Options

```bash
# === REQUIRED ===
LEASE_API_KEY                      # Your Entrata API key
ENTRATA_ORG                        # Your organization name (default: "peakmade")

# === API ENDPOINTS ===
LEASE_API_BASE_URL                 # Base URL if same for both endpoints
LEASE_API_DETAILS_URL              # Full URL for getLeaseDetails
LEASE_API_AR_URL                   # Full URL for getLeaseArTransactions

# === API METHODS ===
LEASE_API_DETAILS_METHOD           # Default: "getLeaseDetails"
LEASE_API_DETAILS_VERSION          # Default: "r2"
LEASE_API_AR_METHOD                # Default: "getLeaseArTransactions"
LEASE_API_AR_VERSION               # Default: "r1"

# === API BEHAVIOR ===
LEASE_API_TIMEOUT_SECONDS          # Request timeout (default: 60)
LEASE_API_KEY_HEADER               # Header name for API key (default: "X-Api-Key")

# === DATA FILTERS ===
LEASE_API_LEASE_STATUS_TYPE_IDS    # Lease statuses to fetch (default: "3,4" = current+future)
LEASE_API_AR_LEASE_STATUS_TYPE_IDS # AR transaction lease statuses (default: "3,4")
LEASE_API_TRANSACTION_TYPE_IDS     # Specific transaction types (default: all)
LEASE_API_AR_CODE_IDS              # Specific AR codes (default: all)
LEASE_API_LEDGER_IDS               # Specific ledger IDs (default: all)

# === API OPTIONS ===
LEASE_API_INCLUDE_ADDONS           # Include add-ons in lease details (default: "0")
LEASE_API_INCLUDE_CHARGE           # Include charges in lease details (default: "1")
LEASE_API_SHOW_FULL_LEDGER         # Show full ledger (default: "1")
LEASE_API_RESIDENT_FRIENDLY_MODE   # Resident-friendly mode (default: "0")
LEASE_API_INCLUDE_OTHER_INCOME_LEASES # Include other income leases (default: "0")
LEASE_API_INCLUDE_REVERSALS        # Include reversal transactions (default: "1")

# === PROPERTY PICKLIST (for UI) ===
LEASE_API_PROPERTIES_SHAREPOINT_LIST          # SharePoint list name (default: "Properties_0")
LEASE_API_PROPERTIES_REQUIRE_REPORTABLE       # Require LEGACY_ENTRATA_ID (default: "1")

# === DEBUGGING ===
LEASE_API_DEBUG_PARKING            # Debug parking charges specifically (default: "false")
```

---

## API Endpoints & Methods

### Endpoint 1: Get Lease Details

**Purpose:** Fetch scheduled charges, lease terms, and customer information

**URL Pattern:**
```
https://apis.entrata.com/ext/orgs/{YOUR_ORG}/v1/leases
```

**Method Name:** `getLeaseDetails`  
**Version:** `r2`  
**HTTP Method:** `POST`

**What it returns:**
- Lease ID, property ID, customer names
- Scheduled recurring charges (rent, utilities, etc.)
- One-time charges (deposits, fees)
- Lease start/end dates
- Charge start/end dates
- AR code IDs and names

### Endpoint 2: Get AR Transactions

**Purpose:** Fetch actual billable transactions from the accounts receivable ledger

**URL Pattern:**
```
https://apis.entrata.com/ext/orgs/{YOUR_ORG}/v1/artransactions
```

**Method Name:** `getLeaseArTransactions`  
**Version:** `r1`  
**HTTP Method:** `POST`

**What it returns:**
- Transaction IDs, amounts, dates
- AR codes and descriptions
- Posted/unposted status
- Reversals
- Ledger details
- Customer information

---

## Request Structure

### Anatomy of an Entrata API Request

All Entrata API requests use **POST** with a **JSON body** containing three main sections:

```json
{
  "auth": {
    "type": "apikey"
  },
  "requestId": "unique-identifier-12345",
  "method": {
    "name": "getLeaseDetails",
    "version": "r2",
    "params": {
      "propertyId": 1150907,
      "leaseIds": "15293094"
    }
  }
}
```

### Request Components

#### 1. **Authentication Block**
```json
"auth": {
  "type": "apikey"
}
```
- Always the same
- Tells Entrata you're using API key authentication
- **Note:** The actual API key goes in the HTTP headers, not the body

#### 2. **Request ID**
```json
"requestId": "1705432190000"
```
- Unique identifier for this request
- Often a timestamp (milliseconds since epoch)
- Used for logging and debugging
- Example: `str(int(datetime.utcnow().timestamp() * 1000))`

#### 3. **Method Block**
```json
"method": {
  "name": "getLeaseDetails",
  "version": "r2",
  "params": { ... }
}
```

**Method fields:**
- `name` - Which Entrata API method to call
- `version` - API version (some methods have multiple versions)
- `params` - Method-specific parameters (what data you want)

### HTTP Headers

**Required headers for every request:**

```http
Accept: application/json
Content-Type: application/json
X-Api-Key: your-api-key-here
```

**Header explanations:**
- `Accept` - Tells Entrata you want JSON response (not XML)
- `Content-Type` - Tells Entrata you're sending JSON
- `X-Api-Key` - Your authentication credential

### Example Requests

#### Request 1: Get Single Lease Details

```http
POST https://apis.entrata.com/ext/orgs/peakmade/v1/leases
Content-Type: application/json
X-Api-Key: your-api-key-here

{
  "auth": {
    "type": "apikey"
  },
  "requestId": "single-lease-12345",
  "method": {
    "name": "getLeaseDetails",
    "version": "r2",
    "params": {
      "propertyId": 1150907,
      "leaseIds": "15293094",
      "includeCharge": "1",
      "includeAddOns": "0",
      "leaseStatusTypeIds": "3,4"
    }
  }
}
```

**Parameters explained:**
- `propertyId` - Numeric property identifier (required with leaseIds)
- `leaseIds` - Comma-separated lease IDs (for single lease: just one number)
- `includeCharge` - "1" = include scheduled charges, "0" = exclude
- `includeAddOns` - "1" = include add-on charges, "0" = exclude
- `leaseStatusTypeIds` - "3,4" means current and future leases (common default)

#### Request 2: Get AR Transactions for Single Lease

```http
POST https://apis.entrata.com/ext/orgs/peakmade/v1/artransactions
Content-Type: application/json
X-Api-Key: your-api-key-here

{
  "auth": {
    "type": "apikey"
  },
  "requestId": "ar-transactions-12345",
  "method": {
    "name": "getLeaseArTransactions",
    "version": "r1",
    "params": {
      "propertyId": 1150907,
      "leaseIds": "15293094",
      "leaseStatusTypeIds": "3,4",
      "showFullLedger": "1",
      "includeReversals": "1",
      "residentFriendlyMode": "0"
    }
  }
}
```

**Parameters explained:**
- `propertyId` + `leaseIds` - Same as lease details (required together)
- `leaseStatusTypeIds` - Filter to specific lease statuses
- `showFullLedger` - "1" = show all transactions (recommended)
- `includeReversals` - "1" = include reversed transactions
- `residentFriendlyMode` - "0" = show all data (not resident-facing)
- `transactionFromDate` - Optional: filter by start date (MM/DD/YYYY)
- `transactionToDate` - Optional: filter by end date (MM/DD/YYYY)

#### Request 3: Get All Leases for Property

```http
POST https://apis.entrata.com/ext/orgs/peakmade/v1/leases
Content-Type: application/json
X-Api-Key: your-api-key-here

{
  "auth": {
    "type": "apikey"
  },
  "requestId": "property-leases-12345",
  "method": {
    "name": "getLeaseDetails",
    "version": "r2",
    "params": {
      "propertyIds": "1150907",
      "includeCharge": "1",
      "leaseStatusTypeIds": "3,4"
    }
  }
}
```

**Key difference:** Use `propertyIds` (plural) instead of `propertyId` + `leaseIds`

---

## Response Structure

### Success Response Format

All successful Entrata API responses follow this structure:

```json
{
  "code": 200,
  "response": {
    "code": 200,
    "result": {
      // Method-specific data here
    }
  }
}
```

### Response Components

1. **Top-level code** - HTTP-like status (200 = success)
2. **Response wrapper** - Contains nested code and result
3. **Result object** - Actual data you requested

### Example Response: getLeaseDetails

```json
{
  "code": 200,
  "response": {
    "code": 200,
    "result": {
      "leases": {
        "lease": {
          "leaseId": "15293094",
          "propertyId": "1150907",
          "propertyName": "Example Apartments",
          "customerId": "12345678",
          "customerName": "John Doe",
          "guarantorName": "",
          "scheduledCharges": {
            "recurringCharge": [
              {
                "leaseIntervalId": "98765432",
                "leaseStartDate": "01/01/2024",
                "leaseEndDate": "12/31/2024",
                "leaseIntervalStatus": "Current",
                "charges": {
                  "charge": [
                    {
                      "id": "111222333",
                      "scheduledChargeId": "111222333",
                      "arCodeId": "100001",
                      "chargeCode": "RENT",
                      "amount": "1500.00",
                      "chargeStartDate": "01/01/2024",
                      "chargeEndDate": "12/31/2024",
                      "chargeTiming": "Monthly",
                      "postedThrough": "11/30/2024"
                    }
                  ]
                }
              }
            ],
            "oneTimeCharge": [
              {
                "leaseIntervalId": "98765432",
                "charges": {
                  "charge": {
                    "id": "444555666",
                    "scheduledChargeId": "444555666",
                    "arCodeId": "100010",
                    "chargeCode": "DEPOSIT",
                    "amount": "1500.00",
                    "chargeStartDate": "01/01/2024",
                    "postedThrough": "01/01/2024"
                  }
                }
              }
            ]
          }
        }
      }
    }
  }
}
```

**Key fields:**
- `leaseId` - Unique lease identifier
- `scheduledCharges.recurringCharge` - Monthly/recurring charges (rent, utilities)
- `scheduledCharges.oneTimeCharge` - One-time charges (deposits, fees)
- `arCodeId` - Charge type identifier
- `chargeStartDate` / `chargeEndDate` - When charge is active
- `postedThrough` - Last date this charge was posted to ledger

### Example Response: getLeaseArTransactions

```json
{
  "code": 200,
  "response": {
    "code": 200,
    "result": {
      "leases": {
        "lease": {
          "leaseId": "15293094",
          "propertyId": "1150907",
          "customerId": "12345678",
          "customerName": "John Doe",
          "ledgers": {
            "ledger": {
              "ledgerId": "87654321",
              "transactions": {
                "transaction": [
                  {
                    "id": "999888777",
                    "arCodeId": "100001",
                    "arCodeName": "Rent",
                    "amount": "1500.00",
                    "postDate": "01/01/2024",
                    "postMonth": "01/01/2024",
                    "transactionDate": "01/01/2024",
                    "description": "January 2024 Rent",
                    "leaseIntervalId": "98765432",
                    "scheduledChargeId": "111222333"
                  },
                  {
                    "id": "999888778",
                    "arCodeId": "100001",
                    "arCodeName": "Rent",
                    "amount": "1500.00",
                    "postDate": "02/01/2024",
                    "postMonth": "02/01/2024",
                    "transactionDate": "02/01/2024",
                    "description": "February 2024 Rent",
                    "leaseIntervalId": "98765432",
                    "scheduledChargeId": "111222333"
                  }
                ]
              }
            }
          }
        }
      }
    }
  }
}
```

**Key fields:**
- `id` - Unique transaction identifier
- `arCodeId` - Matches to scheduled charge AR code
- `amount` - Transaction amount
- `postDate` - When transaction was posted
- `scheduledChargeId` - Links back to scheduled charge (important for reconciliation!)

### Error Response Format

```json
{
  "code": 400,
  "response": {
    "code": 400,
    "message": "Invalid API key"
  }
}
```

**Common error codes:**
- `400` - Bad request (invalid parameters)
- `401` - Unauthorized (bad API key)
- `403` - Forbidden (no permission for this method)
- `404` - Not found (invalid endpoint or method name)
- `500` - Server error (Entrata internal issue)

---

## Common Scenarios

### Scenario 1: Audit a Single Lease

**When to use:** Debugging a specific resident's billing issues

**Application flow:**
1. User enters Lease ID (e.g., `15293094`)
2. Application optionally discovers Property ID if not provided
3. Makes 2 API calls:
   - `getLeaseDetails` (scheduled charges)
   - `getLeaseArTransactions` (actual transactions)
4. Compares scheduled vs actual
5. Generates discrepancy report

**Code location:** `audit_engine/api_ingest.py` → `fetch_single_lease_api_sources()`

**Environment variables used:**
- `LEASE_API_KEY` (required)
- `LEASE_API_DETAILS_URL` (required)
- `LEASE_API_AR_URL` (required)
- `LEASE_API_TIMEOUT_SECONDS` (optional, default 60)

### Scenario 2: Audit Entire Property

**When to use:** Comprehensive audit of all residents at a property

**Application flow:**
1. User selects Property ID from dropdown (e.g., `1150907`)
2. Application makes bulk API calls for all leases
3. Processes hundreds/thousands of leases
4. Aggregates results by property, unit, bucket
5. Generates portfolio-level report

**Code location:** `audit_engine/api_ingest.py` → `fetch_property_api_sources()`

**Note:** Same API endpoints, but uses `propertyIds` parameter instead of `leaseIds`

### Scenario 3: Filter Transactions by Date

**When to use:** Analyzing billing for a specific time period

**Example:**
```python
fetch_single_lease_api_sources(
    lease_id=15293094,
    property_id=1150907,
    transaction_from_date="01/01/2024",
    transaction_to_date="03/31/2024"
)
```

**What happens:**
- Lease details still fetches all scheduled charges (no date filter)
- AR transactions only returns transactions from Jan-Mar 2024
- Results in focused reconciliation for that quarter

### Scenario 4: Property Picklist for UI

**When to use:** Populating dropdown menu with available properties

**Application flow:**
1. User navigates to API upload form
2. Application fetches property list from SharePoint (not directly from Entrata)
3. Displays properties with `LEGACY_ENTRATA_ID` field
4. User selects property to audit

**Code location:** `audit_engine/api_ingest.py` → `fetch_entrata_property_picklist()`

**Dependencies:**
- SharePoint access (Microsoft Graph API)
- SharePoint list named `Properties_0` (or configured via `LEASE_API_PROPERTIES_SHAREPOINT_LIST`)
- List must have columns: `PROPERTY_NAME`, `LEGACY_ENTRATA_ID`

---

## Troubleshooting

### Issue 1: "Missing LEASE_API_KEY env var"

**Error message:**
```
ValueError: Missing LEASE_API_KEY env var
```

**Cause:** API key not set in environment variables

**Solution:**
```powershell
# Windows PowerShell
$env:LEASE_API_KEY = "your-api-key-here"

# Or add to Azure App Service → Configuration → Application settings
```

**Verify:**
```powershell
echo $env:LEASE_API_KEY
```

### Issue 2: "HTTP 401" or "Invalid API key"

**Error message:**
```
ValueError: getLeaseDetails failed: {"code": 401, "message": "Invalid API key"}
```

**Causes:**
1. API key is incorrect
2. API key expired
3. API key doesn't have permission for this method

**Solution:**
1. Double-check your API key (no extra spaces)
2. Contact Entrata to verify key is active
3. Confirm API key has `getLeaseDetails` and `getLeaseArTransactions` permissions

### Issue 3: "No lease found with ID {lease_id}"

**Error message:**
```
ValueError: No lease found with ID 15293094
```

**Causes:**
1. Lease ID doesn't exist
2. Lease status not in your filter (default: current/future only)
3. Wrong property ID for this lease

**Solution:**
1. Verify lease ID exists in Entrata
2. Try expanding lease status filter:
   ```powershell
   $env:LEASE_API_LEASE_STATUS_TYPE_IDS = "1,2,3,4,5"  # Include past leases
   ```
3. Let application auto-discover property ID (leave `property_id=None`)

### Issue 4: Empty or Missing Data

**Symptom:** API succeeds but returns no charges or transactions

**Causes:**
1. `includeCharge` set to "0"
2. Date filters too restrictive
3. Lease has no scheduled charges in Entrata
4. Charges filtered out by status

**Solution:**
1. Verify `LEASE_API_INCLUDE_CHARGE = "1"`
2. Remove or expand date filters
3. Check Entrata portal manually for this lease
4. Review `leaseStatusTypeIds` filter

### Issue 5: Timeout Errors

**Error message:**
```
requests.exceptions.Timeout: Request timed out after 60 seconds
```

**Causes:**
1. Entrata servers slow/overloaded
2. Very large property (thousands of leases)
3. Network connectivity issues

**Solution:**
1. Increase timeout:
   ```powershell
   $env:LEASE_API_TIMEOUT_SECONDS = "180"  # 3 minutes
   ```
2. For large properties, consider batching by date ranges
3. Check network connectivity to `apis.entrata.com`

### Issue 6: Duplicate or Extra Leases in Response

**Symptom:** Requesting one lease returns multiple leases

**Cause:** Entrata API quirk - sometimes ignores `leaseIds` filter if not paired with `propertyId`

**Solution:** Application handles this defensively:
- Always sends both `propertyId` AND `leaseIds` together
- Filters results client-side to requested lease ID only
- See logging: "Defensive filtering triggered"

### Issue 7: Property Picklist Empty

**Error message:**
```
ValueError: SharePoint list 'Properties_0' not found
```

**Causes:**
1. SharePoint not configured
2. List name incorrect
3. No access permissions

**Solution:**
1. Verify SharePoint configuration (see `AUTHENTICATION_GUIDE.md`)
2. Check list name:
   ```powershell
   $env:LEASE_API_PROPERTIES_SHAREPOINT_LIST = "YourListName"
   ```
3. Ensure app has `Sites.ReadWrite.All` Microsoft Graph permission

---

## Security Best Practices

### 1. **Never Hardcode API Keys**

❌ **Bad:**
```python
api_key = "abc123xyz789..."  # NEVER DO THIS
```

✅ **Good:**
```python
api_key = os.getenv("LEASE_API_KEY")  # Always use environment variables
```

### 2. **Use Environment Variables**

Store sensitive credentials in:
- Windows: PowerShell environment (temporary) or System/User variables (permanent)
- Azure: App Service Configuration → Application Settings
- Never in source code or version control

### 3. **Restrict API Key Permissions**

- Request **minimum necessary permissions** from Entrata
- Only enable methods you actually use: `getLeaseDetails`, `getLeaseArTransactions`
- Avoid granting write/modify permissions unless required

### 4. **Protect API Keys in Transit**

- Always use HTTPS endpoints (`https://apis.entrata.com/...`)
- Never send API keys in URL query parameters
- Use headers (`X-Api-Key`) for authentication

### 5. **Monitor API Usage**

- Log all API requests with timestamps and request IDs
- Monitor for unusual patterns (too many requests, errors)
- Set up alerts for repeated failures

### 6. **Rotate API Keys Regularly**

- Change API keys every 90-180 days
- Have a rotation plan before keys expire
- Test new keys in development before production

### 7. **Use Azure Key Vault (Production)**

For production deployments:

1. Store API key in **Azure Key Vault**
2. Grant App Service managed identity access to Key Vault
3. Reference Key Vault secrets in App Service configuration:
   ```
   @Microsoft.KeyVault(SecretUri=https://yourkeyvault.vault.azure.net/secrets/EntrataApiKey/)
   ```

### 8. **Review Access Logs**

Regularly review:
- Who has access to production environment variables
- API key usage patterns in Entrata (if available)
- Application logs for unauthorized access attempts

### 9. **Separate Keys by Environment**

Use different API keys for:
- **Development** - Lower permissions, test data
- **Staging** - Production-like permissions, test data
- **Production** - Full permissions, real data

### 10. **Document Key Ownership**

Maintain a secure document with:
- Who requested the API key
- When it was created/last rotated
- What permissions it has
- Who to contact if compromised

---

## Quick Reference

### Minimal Configuration

**Absolute minimum to get started:**

```powershell
$env:LEASE_API_KEY = "your-entrata-api-key"
$env:ENTRATA_ORG = "peakmade"
```

That's it! The application uses sensible defaults for everything else.

### Common URL Patterns

```
# Lease Details
https://apis.entrata.com/ext/orgs/{YOUR_ORG}/v1/leases

# AR Transactions  
https://apis.entrata.com/ext/orgs/{YOUR_ORG}/v1/artransactions

# Alternative Legacy Format (if using older Entrata API)
https://{YOUR_ORG}.entrata.com/api/v1/...
```

### Lease Status Type IDs

Common values for `leaseStatusTypeIds`:

| ID | Status | Description |
|----|--------|-------------|
| 1 | Cancelled | Lease was cancelled |
| 2 | Denied | Application denied |
| 3 | Current | Active lease |
| 4 | Future | Signed but not yet started |
| 5 | Past | Expired/ended lease |
| 6 | Notice | Resident gave notice |

**Default:** `"3,4"` (current + future)

### Testing Your Connection

**Quick test script:**

```python
import os
import requests
import json

API_KEY = os.getenv("LEASE_API_KEY")
ORG = os.getenv("ENTRATA_ORG", "peakmade")
URL = f"https://apis.entrata.com/ext/orgs/{ORG}/v1/leases"

headers = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "X-Api-Key": API_KEY
}

body = {
    "auth": {"type": "apikey"},
    "requestId": "test-12345",
    "method": {
        "name": "getLeaseDetails",
        "version": "r2",
        "params": {
            "leaseIds": "YOUR_TEST_LEASE_ID",
            "propertyId": YOUR_TEST_PROPERTY_ID,
            "includeCharge": "1"
        }
    }
}

response = requests.post(URL, headers=headers, json=body, timeout=60)
print(f"Status: {response.status_code}")
print(json.dumps(response.json(), indent=2))
```

**Success output:**
```
Status: 200
{
  "code": 200,
  "response": {
    "code": 200,
    "result": { ... }
  }
}
```

---

## Additional Resources

### Documentation to Review

1. **SINGLE_LEASE_AUDIT_FLOW.md** - Detailed flow diagram for single lease audits
2. **AUTHENTICATION_GUIDE.md** - Azure AD and Microsoft Graph setup
3. **DATA_MODEL.md** - How API data is transformed
4. **MASTER_DOCUMENTATION.md** - Complete system overview

### Code References

- **API Integration:** `audit_engine/api_ingest.py`
- **Lease Terms Extraction:** `audit_engine/entrata_lease_terms.py`
- **Request Orchestration:** `web/views.py` → `execute_audit_run()`

### Getting Help

**Entrata Support:**
- Email: support@entrata.com
- Developer docs: https://developer.entrata.com/ (if available)

**Application Issues:**
- Check application logs in `instance/runs/` directory
- Review console output for debug messages (look for `[SINGLE LEASE API]` tags)
- Enable verbose logging with audit run

---

## Summary Checklist

Before you start using the Entrata API:

- [ ] Obtained API key from Entrata
- [ ] Confirmed organization name
- [ ] Set `LEASE_API_KEY` environment variable
- [ ] Set `ENTRATA_ORG` environment variable (if not "peakmade")
- [ ] Verified API endpoints are correct
- [ ] Tested connection with sample lease
- [ ] Reviewed security best practices
- [ ] Documented API key ownership and permissions
- [ ] Configured SharePoint for property picklist (optional)
- [ ] Set up monitoring/logging for API requests

**You're ready to audit leases!** 🎉

---

*Document Version: 1.0*  
*Last Updated: March 2026*  
*Maintained by: Lease File Audit Development Team*
