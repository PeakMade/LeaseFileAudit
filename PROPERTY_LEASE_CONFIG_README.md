# Property Lease Configuration Guide

## Overview

The `property_lease_config.json` file allows you to configure property-specific rules for lease document selection and reconciliation. This configuration is automatically loaded when processing leases, allowing you to customize behavior without modifying code.

## File Location

**Default**: `property_lease_config.json` (in the project root)

**Environment Override**: Set `PROPERTY_LEASE_CONFIG_PATH` to specify a custom location

```bash
PROPERTY_LEASE_CONFIG_PATH=/path/to/custom/config.json
```

## Configuration Structure

```json
{
  "description": "Property-specific configurations for lease document selection and processing",
  "default": {
    "lease_document_selection": {
      ...default rules applied to all properties...
    }
  },
  "properties": {
    "PROPERTY_ID": {
      "property_name": "Property Name",
      "lease_document_selection": {
        ...property-specific overrides...
      },
      "reconciliation": {
        ...property-specific reconciliation rules...
      }
    }
  }
}
```

## Lease Document Selection Configuration

### Available Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `preferred_codes_tier1` | array | `["LP", "OEP", "PACKET"]` | Highest priority document type codes |
| `preferred_codes_tier2` | array | `["LEASE", "LD", "OEL"]` | Second priority document type codes |
| `min_file_size_bytes` | number | `50000` | Minimum file size to consider (filters out stub documents) |
| `exclude_title_patterns` | array | `[]` | Regex patterns to exclude documents by title |
| `preferred_title_patterns` | array | `[]` | Regex patterns to prefer documents by title |
| `require_signed` | boolean | `true` | Whether to require documents to be signed |

### Selection Priority

The lease document selection follows this priority order:

1. **Tier 1 Codes**: Documents matching `preferred_codes_tier1`
2. **Tier 2 Codes**: Documents matching `preferred_codes_tier2`  
3. **Preferred Title Patterns**: Documents matching `preferred_title_patterns`
4. **Signed Lease/Packet**: Documents with "lease" or "packet" in title (if `require_signed` is true)

Within each tier, documents are further sorted by:
- Lease start date (matching audit period if specified)
- Added date (most recent first)
- Modified date (most recent first)
- File size (larger preferred)
- Document ID (higher preferred)

### Example: Chateau on Wells

```json
"100139752": {
  "property_name": "Chateau on Wells",
  "lease_document_selection": {
    "preferred_codes_tier1": ["LP", "OEP", "PACKET"],
    "preferred_codes_tier2": ["LEASE", "LD", "OEL"],
    "min_file_size_bytes": 100000,
    "exclude_title_patterns": [
      "e-?sign.*modification",
      "lease\\s+modification",
      "co-applicant",
      "applicant\\s+upload"
    ],
    "preferred_title_patterns": [
      "lease\\s+document",
      ".*_.*_.*_attachment.*\\.pdf"
    ],
    "require_signed": true,
    "notes": "Prefer documents with 'Lease Document' title or PDF attachments with specific naming pattern. Exclude e-Sign modifications and applicant uploads. Require minimum 100KB file size."
  }
}
```

This configuration:
- ✅ Prioritizes documents labeled "LP", "OEP", or "PACKET"
- ✅ Requires files to be at least 100KB (filters out small placeholder documents)
- ❌ Excludes e-sign modifications and lease modifications
- ❌ Excludes co-applicant and applicant uploads
- ✅ Prefers documents titled "Lease Document"
- ✅ Prefers PDF attachments with specific naming patterns
- ✅ Requires documents to be marked as signed

## Reconciliation Configuration

### Available Settings

| Setting | Type | Description |
|---------|------|-------------|
| `pre_acquisition_date` | string (YYYY-MM-DD) | Date property was acquired |
| `mark_pre_acquisition_scheduled_as_expected` | boolean | Whether to treat pre-acquisition scheduled charges as expected (prevents false "Scheduled Not Billed" exceptions) |

### Example: Mid-Lease Acquisition

```json
"100139752": {
  "reconciliation": {
    "pre_acquisition_date": "2026-01-01",
    "mark_pre_acquisition_scheduled_as_expected": true,
    "notes": "Property was acquired mid-lease in January 2026. Scheduled charges before this date should not flag as 'Scheduled Not Billed' exceptions since we have no transaction history before acquisition."
  }
}
```

This is useful when:
- You acquire a property mid-lease cycle
- You don't have AR transaction history before acquisition
- You want to avoid false exceptions for scheduled charges before your ownership

## Adding a New Property Configuration

1. **Get the Property ID** from Entrata or your system
2. **Add an entry** to the `properties` object in the config file
3. **Configure settings** - only include settings you want to override from defaults
4. **Test** by running an audit for that property
5. **Review logs** for `[LEASE CONFIG]` messages showing which config was applied

### Template

```json
"YOUR_PROPERTY_ID": {
  "property_name": "Property Display Name",
  "lease_document_selection": {
    "preferred_codes_tier1": ["LP", "OEP", "PACKET"],
    "min_file_size_bytes": 75000,
    "exclude_title_patterns": [
      "pattern1",
      "pattern2"
    ]
  },
  "notes": "Why this configuration is needed"
}
```

## Logging and Debugging

The system logs configuration decisions at various levels:

```
[LEASE CONFIG] Using property-specific config for property 100139752: tier1=['LP', 'OEP', 'PACKET'], tier2=['LEASE', 'LD', 'OEL'], min_size=100000
[LEASE CONFIG] Excluding doc 'lease modification document' - matched pattern 'lease\s+modification'
[LEASE CONFIG] Excluding doc 'stub.pdf' - file size 12000 < minimum 100000
```

To see these logs:
- Check your application logs during lease term extraction
- Look for `[LEASE CONFIG]` prefix in log messages
- Review lease selection reasons in audit output

## Best Practices

1. **Start with defaults** - Only override settings when you have a specific need
2. **Document your why** - Use the `notes` field to explain why custom rules are needed
3. **Test incrementally** - Add one property at a time and verify behavior
4. **Use specific patterns** - Make exclusion/preference patterns as specific as possible to avoid unintended matches
5. **Monitor logs** - Review `[LEASE CONFIG]` logs to ensure rules work as expected
6. **Keep it current** - Update configurations when property workflows change

## Common Use Cases

### Case 1: Exclude E-Sign Modifications

```json
"exclude_title_patterns": [
  "e-?sign.*modification",
  "lease\\s+modification"
]
```

### Case 2: Require Larger Files (Avoid Stubs)

```json
"min_file_size_bytes": 100000
```

### Case 3: Prefer Specific Document Naming

```json
"preferred_title_patterns": [
  "lease\\s+document",
  "final\\s+signed"
]
```

### Case 4: Mid-Lease Property Acquisition

```json
"reconciliation": {
  "pre_acquisition_date": "2026-01-15",
  "mark_pre_acquisition_scheduled_as_expected": true
}
```

## Regex Pattern Tips

Patterns use Python regex syntax (case-insensitive by default):

- `e-?sign` - Matches "esign" or "e-sign"
- `lease\\s+modification` - Matches "lease modification" with any amount of whitespace
- `.*attachment.*\\.pdf` - Matches any filename containing "attachment" ending in ".pdf"
- `^final` - Matches titles starting with "final"
- `signed$` - Matches titles ending with "signed"

Test your regex patterns online at [regex101.com](https://regex101.com) (select Python flavor).

## Troubleshooting

**Problem**: Configuration not being applied

- ✅ Verify JSON syntax is valid (use a JSON validator)
- ✅ Check property ID matches exactly (as string)
- ✅ Look for `[LEASE CONFIG]` logs
- ✅ Ensure file is in the correct location

**Problem**: Wrong document selected

- ✅ Review selection priority order
- ✅ Check if exclusion patterns are too broad
- ✅ Verify file size minimums aren't too restrictive
- ✅ Look at selection reason in logs

**Problem**: Too many documents excluded

- ✅ Make exclusion patterns more specific
- ✅ Lower minimum file size if needed
- ✅ Check if `require_signed` should be false
- ✅ Review matched patterns in debug logs

---

**Last Updated**: March 11, 2026  
**Related Files**: 
- `property_lease_config.json` - The configuration file
- `audit_engine/entrata_lease_terms.py` - Implementation code
- `MASTER_DOCUMENTATION.md` - Full system documentation
