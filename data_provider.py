"""
Data provider for Lease File Audit application.
Single source of truth for all audit data (mock in-memory data).
"""

from datetime import datetime, timedelta
from copy import deepcopy

# ============================================================================
# MOCK DATA STORAGE
# ============================================================================

PROPERTIES = [
    {
        "property_id": "PROP001",
        "property_name": "Sunset Apartments",
        "address": "123 Main St, Austin, TX",
        "total_units": 150,
        "accuracy_score": 87.5,
        "open_flags_count": 8,
        "leases_needing_review": 12
    },
    {
        "property_id": "PROP002",
        "property_name": "River View Condos",
        "address": "456 River Rd, Austin, TX",
        "total_units": 200,
        "accuracy_score": 92.3,
        "open_flags_count": 5,
        "leases_needing_review": 7
    },
    {
        "property_id": "PROP003",
        "property_name": "Mountain Peak Residences",
        "address": "789 Peak Blvd, Denver, CO",
        "total_units": 120,
        "accuracy_score": 78.9,
        "open_flags_count": 15,
        "leases_needing_review": 18
    }
]

LEASES = [
    # Sunset Apartments leases
    {
        "lease_id": "LSE001",
        "property_id": "PROP001",
        "property_name": "Sunset Apartments",
        "unit_number": "101",
        "resident_name": "John Smith",
        "lease_start_date": "2024-01-15",
        "lease_end_date": "2024-12-31",
        "lease_status": "Active",
        "monthly_rent": 1500.00,
        "accuracy_score": 95.0,
        "open_flags_count": 1
    },
    {
        "lease_id": "LSE002",
        "property_id": "PROP001",
        "property_name": "Sunset Apartments",
        "unit_number": "102",
        "resident_name": "Sarah Johnson",
        "lease_start_date": "2024-03-01",
        "lease_end_date": "2025-02-28",
        "lease_status": "Active",
        "monthly_rent": 1650.00,
        "accuracy_score": 82.5,
        "open_flags_count": 3
    },
    {
        "lease_id": "LSE003",
        "property_id": "PROP001",
        "property_name": "Sunset Apartments",
        "unit_number": "103",
        "resident_name": "Michael Brown",
        "lease_start_date": "2023-06-01",
        "lease_end_date": "2024-05-31",
        "lease_status": "Expired",
        "monthly_rent": 1400.00,
        "accuracy_score": 88.0,
        "open_flags_count": 2
    },
    # River View Condos leases
    {
        "lease_id": "LSE004",
        "property_id": "PROP002",
        "property_name": "River View Condos",
        "unit_number": "201",
        "resident_name": "Emily Davis",
        "lease_start_date": "2024-02-01",
        "lease_end_date": "2025-01-31",
        "lease_status": "Active",
        "monthly_rent": 2100.00,
        "accuracy_score": 93.5,
        "open_flags_count": 1
    },
    {
        "lease_id": "LSE005",
        "property_id": "PROP002",
        "property_name": "River View Condos",
        "unit_number": "202",
        "resident_name": "David Wilson",
        "lease_start_date": "2024-04-15",
        "lease_end_date": "2025-04-14",
        "lease_status": "Active",
        "monthly_rent": 1950.00,
        "accuracy_score": 91.0,
        "open_flags_count": 2
    },
    # Mountain Peak Residences leases
    {
        "lease_id": "LSE006",
        "property_id": "PROP003",
        "property_name": "Mountain Peak Residences",
        "unit_number": "301",
        "resident_name": "Jennifer Martinez",
        "lease_start_date": "2024-01-01",
        "lease_end_date": "2024-12-31",
        "lease_status": "Active",
        "monthly_rent": 1800.00,
        "accuracy_score": 75.0,
        "open_flags_count": 5
    },
    {
        "lease_id": "LSE007",
        "property_id": "PROP003",
        "property_name": "Mountain Peak Residences",
        "unit_number": "302",
        "resident_name": "Robert Taylor",
        "lease_start_date": "2024-05-01",
        "lease_end_date": "2025-04-30",
        "lease_status": "Active",
        "monthly_rent": 1750.00,
        "accuracy_score": 80.5,
        "open_flags_count": 4
    }
]

FLAGS = [
    # LSE001 flags
    {
        "flag_id": "FLG001",
        "lease_id": "LSE001",
        "property_id": "PROP001",
        "unit_number": "101",
        "resident_name": "John Smith",
        "category": "Rent Mismatch",
        "severity": "Low",
        "description": "Security deposit in lease differs from ledger",
        "expected_value": "$1500.00",
        "actual_value": "$1450.00",
        "recommended_fix": "Update ledger to match lease document",
        "source": "lease_to_ledger",
        "resolved": False,
        "created_date": "2024-12-01"
    },
    # LSE002 flags
    {
        "flag_id": "FLG002",
        "lease_id": "LSE002",
        "property_id": "PROP001",
        "unit_number": "102",
        "resident_name": "Sarah Johnson",
        "category": "Date Discrepancy",
        "severity": "High",
        "description": "Lease start date mismatch between lease and metadata",
        "expected_value": "2024-03-01",
        "actual_value": "2024-03-15",
        "recommended_fix": "Verify actual move-in date and update metadata",
        "source": "lease_to_metadata",
        "resolved": False,
        "created_date": "2024-12-02"
    },
    {
        "flag_id": "FLG003",
        "lease_id": "LSE002",
        "property_id": "PROP001",
        "unit_number": "102",
        "resident_name": "Sarah Johnson",
        "category": "Rent Mismatch",
        "severity": "Critical",
        "description": "Monthly rent amount differs between lease and ledger",
        "expected_value": "$1650.00",
        "actual_value": "$1600.00",
        "recommended_fix": "Correct ledger to match signed lease amount",
        "source": "lease_to_ledger",
        "resolved": False,
        "created_date": "2024-12-02"
    },
    {
        "flag_id": "FLG004",
        "lease_id": "LSE002",
        "property_id": "PROP001",
        "unit_number": "102",
        "resident_name": "Sarah Johnson",
        "category": "Missing Data",
        "severity": "Medium",
        "description": "Pet deposit missing in ledger",
        "expected_value": "$300.00",
        "actual_value": "$0.00",
        "recommended_fix": "Add pet deposit charge to ledger",
        "source": "lease_to_ledger",
        "resolved": False,
        "created_date": "2024-12-03"
    },
    # LSE003 flags
    {
        "flag_id": "FLG005",
        "lease_id": "LSE003",
        "property_id": "PROP001",
        "unit_number": "103",
        "resident_name": "Michael Brown",
        "category": "Status Mismatch",
        "severity": "Medium",
        "description": "Lease marked as Active in system but expired per lease document",
        "expected_value": "Expired",
        "actual_value": "Active",
        "recommended_fix": "Update lease status to Expired",
        "source": "lease_to_metadata",
        "resolved": False,
        "created_date": "2024-12-05"
    },
    {
        "flag_id": "FLG006",
        "lease_id": "LSE003",
        "property_id": "PROP001",
        "unit_number": "103",
        "resident_name": "Michael Brown",
        "category": "Rent Mismatch",
        "severity": "Low",
        "description": "Late fee schedule differs between lease and system",
        "expected_value": "$50.00",
        "actual_value": "$75.00",
        "recommended_fix": "Update system to match lease terms",
        "source": "lease_to_metadata",
        "resolved": True,
        "created_date": "2024-11-20"
    },
    # LSE004 flags
    {
        "flag_id": "FLG007",
        "lease_id": "LSE004",
        "property_id": "PROP002",
        "unit_number": "201",
        "resident_name": "Emily Davis",
        "category": "Missing Data",
        "severity": "Low",
        "description": "Parking space number missing in metadata",
        "expected_value": "P-45",
        "actual_value": "Not recorded",
        "recommended_fix": "Add parking space number to metadata",
        "source": "lease_to_metadata",
        "resolved": False,
        "created_date": "2024-12-08"
    },
    # LSE005 flags
    {
        "flag_id": "FLG008",
        "lease_id": "LSE005",
        "property_id": "PROP002",
        "unit_number": "202",
        "resident_name": "David Wilson",
        "category": "Date Discrepancy",
        "severity": "Medium",
        "description": "Lease end date mismatch between metadata and ledger",
        "expected_value": "2025-04-14",
        "actual_value": "2025-04-15",
        "recommended_fix": "Align ledger end date with lease document",
        "source": "metadata_to_ledger",
        "resolved": False,
        "created_date": "2024-12-10"
    },
    {
        "flag_id": "FLG009",
        "lease_id": "LSE005",
        "property_id": "PROP002",
        "unit_number": "202",
        "resident_name": "David Wilson",
        "category": "Rent Mismatch",
        "severity": "Low",
        "description": "Utility charge mismatch",
        "expected_value": "$150.00",
        "actual_value": "$125.00",
        "recommended_fix": "Update ledger utility charge",
        "source": "lease_to_ledger",
        "resolved": True,
        "created_date": "2024-11-15"
    },
    # LSE006 flags
    {
        "flag_id": "FLG010",
        "lease_id": "LSE006",
        "property_id": "PROP003",
        "unit_number": "301",
        "resident_name": "Jennifer Martinez",
        "category": "Rent Mismatch",
        "severity": "Critical",
        "description": "Significant rent amount discrepancy",
        "expected_value": "$1800.00",
        "actual_value": "$1700.00",
        "recommended_fix": "Correct ledger rent to match lease",
        "source": "lease_to_ledger",
        "resolved": False,
        "created_date": "2024-12-12"
    },
    {
        "flag_id": "FLG011",
        "lease_id": "LSE006",
        "property_id": "PROP003",
        "unit_number": "301",
        "resident_name": "Jennifer Martinez",
        "category": "Missing Data",
        "severity": "High",
        "description": "Co-signer information missing from metadata",
        "expected_value": "Maria Martinez",
        "actual_value": "None",
        "recommended_fix": "Add co-signer to resident records",
        "source": "lease_to_metadata",
        "resolved": False,
        "created_date": "2024-12-12"
    },
    {
        "flag_id": "FLG012",
        "lease_id": "LSE006",
        "property_id": "PROP003",
        "unit_number": "301",
        "resident_name": "Jennifer Martinez",
        "category": "Date Discrepancy",
        "severity": "Medium",
        "description": "Lease renewal date not recorded",
        "expected_value": "2024-12-01",
        "actual_value": "Not set",
        "recommended_fix": "Set renewal notification date",
        "source": "lease_to_metadata",
        "resolved": False,
        "created_date": "2024-12-13"
    },
    {
        "flag_id": "FLG013",
        "lease_id": "LSE006",
        "property_id": "PROP003",
        "unit_number": "301",
        "resident_name": "Jennifer Martinez",
        "category": "Missing Data",
        "severity": "Medium",
        "description": "Pet information incomplete",
        "expected_value": "1 Dog, 1 Cat",
        "actual_value": "1 Dog",
        "recommended_fix": "Update pet records with cat information",
        "source": "lease_to_metadata",
        "resolved": False,
        "created_date": "2024-12-13"
    },
    {
        "flag_id": "FLG014",
        "lease_id": "LSE006",
        "property_id": "PROP003",
        "unit_number": "301",
        "resident_name": "Jennifer Martinez",
        "category": "Rent Mismatch",
        "severity": "Low",
        "description": "Pet rent amount mismatch",
        "expected_value": "$50.00",
        "actual_value": "$25.00",
        "recommended_fix": "Correct pet rent in ledger",
        "source": "lease_to_ledger",
        "resolved": False,
        "created_date": "2024-12-13"
    },
    # LSE007 flags
    {
        "flag_id": "FLG015",
        "lease_id": "LSE007",
        "property_id": "PROP003",
        "unit_number": "302",
        "resident_name": "Robert Taylor",
        "category": "Date Discrepancy",
        "severity": "High",
        "description": "Move-in inspection date missing",
        "expected_value": "2024-05-01",
        "actual_value": "Not recorded",
        "recommended_fix": "Record inspection completion date",
        "source": "lease_to_metadata",
        "resolved": False,
        "created_date": "2024-12-14"
    },
    {
        "flag_id": "FLG016",
        "lease_id": "LSE007",
        "property_id": "PROP003",
        "unit_number": "302",
        "resident_name": "Robert Taylor",
        "category": "Missing Data",
        "severity": "Medium",
        "description": "Emergency contact not in system",
        "expected_value": "Jane Taylor (555-0123)",
        "actual_value": "Not recorded",
        "recommended_fix": "Add emergency contact information",
        "source": "lease_to_metadata",
        "resolved": False,
        "created_date": "2024-12-14"
    },
    {
        "flag_id": "FLG017",
        "lease_id": "LSE007",
        "property_id": "PROP003",
        "unit_number": "302",
        "resident_name": "Robert Taylor",
        "category": "Rent Mismatch",
        "severity": "Medium",
        "description": "Storage unit fee not in ledger",
        "expected_value": "$100.00",
        "actual_value": "$0.00",
        "recommended_fix": "Add storage unit charge to ledger",
        "source": "lease_to_ledger",
        "resolved": False,
        "created_date": "2024-12-14"
    },
    {
        "flag_id": "FLG018",
        "lease_id": "LSE007",
        "property_id": "PROP003",
        "unit_number": "302",
        "resident_name": "Robert Taylor",
        "category": "Status Mismatch",
        "severity": "Low",
        "description": "Concession end date mismatch",
        "expected_value": "2024-08-31",
        "actual_value": "2024-09-30",
        "recommended_fix": "Update concession schedule in system",
        "source": "metadata_to_ledger",
        "resolved": False,
        "created_date": "2024-12-15"
    }
]

# Comparison data for each lease
LEASE_COMPARISONS = {
    "LSE001": {
        "lease_to_metadata": [
            {"field_name": "Unit Number", "expected_value": "101", "actual_value": "101", "match": True},
            {"field_name": "Resident Name", "expected_value": "John Smith", "actual_value": "John Smith", "match": True},
            {"field_name": "Lease Start Date", "expected_value": "2024-01-15", "actual_value": "2024-01-15", "match": True},
            {"field_name": "Lease End Date", "expected_value": "2024-12-31", "actual_value": "2024-12-31", "match": True},
            {"field_name": "Monthly Rent", "expected_value": "$1500.00", "actual_value": "$1500.00", "match": True},
        ],
        "lease_to_ledger": [
            {"field_name": "Monthly Rent", "expected_value": "$1500.00", "actual_value": "$1500.00", "match": True},
            {"field_name": "Security Deposit", "expected_value": "$1500.00", "actual_value": "$1450.00", "match": False},
            {"field_name": "Pet Deposit", "expected_value": "$0.00", "actual_value": "$0.00", "match": True},
        ],
        "metadata_to_ledger": [
            {"field_name": "Lease Start Date", "expected_value": "2024-01-15", "actual_value": "2024-01-15", "match": True},
            {"field_name": "Lease End Date", "expected_value": "2024-12-31", "actual_value": "2024-12-31", "match": True},
            {"field_name": "Monthly Rent", "expected_value": "$1500.00", "actual_value": "$1500.00", "match": True},
        ]
    },
    "LSE002": {
        "lease_to_metadata": [
            {"field_name": "Unit Number", "expected_value": "102", "actual_value": "102", "match": True},
            {"field_name": "Resident Name", "expected_value": "Sarah Johnson", "actual_value": "Sarah Johnson", "match": True},
            {"field_name": "Lease Start Date", "expected_value": "2024-03-01", "actual_value": "2024-03-15", "match": False},
            {"field_name": "Lease End Date", "expected_value": "2025-02-28", "actual_value": "2025-02-28", "match": True},
            {"field_name": "Monthly Rent", "expected_value": "$1650.00", "actual_value": "$1650.00", "match": True},
            {"field_name": "Pet Deposit", "expected_value": "$300.00", "actual_value": "$300.00", "match": True},
        ],
        "lease_to_ledger": [
            {"field_name": "Monthly Rent", "expected_value": "$1650.00", "actual_value": "$1600.00", "match": False},
            {"field_name": "Security Deposit", "expected_value": "$1650.00", "actual_value": "$1650.00", "match": True},
            {"field_name": "Pet Deposit", "expected_value": "$300.00", "actual_value": "$0.00", "match": False},
        ],
        "metadata_to_ledger": [
            {"field_name": "Lease Start Date", "expected_value": "2024-03-15", "actual_value": "2024-03-01", "match": False},
            {"field_name": "Lease End Date", "expected_value": "2025-02-28", "actual_value": "2025-02-28", "match": True},
            {"field_name": "Monthly Rent", "expected_value": "$1650.00", "actual_value": "$1600.00", "match": False},
        ]
    },
    "LSE003": {
        "lease_to_metadata": [
            {"field_name": "Unit Number", "expected_value": "103", "actual_value": "103", "match": True},
            {"field_name": "Resident Name", "expected_value": "Michael Brown", "actual_value": "Michael Brown", "match": True},
            {"field_name": "Lease Start Date", "expected_value": "2023-06-01", "actual_value": "2023-06-01", "match": True},
            {"field_name": "Lease End Date", "expected_value": "2024-05-31", "actual_value": "2024-05-31", "match": True},
            {"field_name": "Lease Status", "expected_value": "Expired", "actual_value": "Active", "match": False},
            {"field_name": "Late Fee", "expected_value": "$50.00", "actual_value": "$75.00", "match": False},
        ],
        "lease_to_ledger": [
            {"field_name": "Monthly Rent", "expected_value": "$1400.00", "actual_value": "$1400.00", "match": True},
            {"field_name": "Security Deposit", "expected_value": "$1400.00", "actual_value": "$1400.00", "match": True},
        ],
        "metadata_to_ledger": [
            {"field_name": "Lease Start Date", "expected_value": "2023-06-01", "actual_value": "2023-06-01", "match": True},
            {"field_name": "Lease End Date", "expected_value": "2024-05-31", "actual_value": "2024-05-31", "match": True},
        ]
    },
    "LSE004": {
        "lease_to_metadata": [
            {"field_name": "Unit Number", "expected_value": "201", "actual_value": "201", "match": True},
            {"field_name": "Resident Name", "expected_value": "Emily Davis", "actual_value": "Emily Davis", "match": True},
            {"field_name": "Lease Start Date", "expected_value": "2024-02-01", "actual_value": "2024-02-01", "match": True},
            {"field_name": "Lease End Date", "expected_value": "2025-01-31", "actual_value": "2025-01-31", "match": True},
            {"field_name": "Parking Space", "expected_value": "P-45", "actual_value": "Not recorded", "match": False},
        ],
        "lease_to_ledger": [
            {"field_name": "Monthly Rent", "expected_value": "$2100.00", "actual_value": "$2100.00", "match": True},
            {"field_name": "Security Deposit", "expected_value": "$2100.00", "actual_value": "$2100.00", "match": True},
        ],
        "metadata_to_ledger": [
            {"field_name": "Lease Start Date", "expected_value": "2024-02-01", "actual_value": "2024-02-01", "match": True},
            {"field_name": "Monthly Rent", "expected_value": "$2100.00", "actual_value": "$2100.00", "match": True},
        ]
    },
    "LSE005": {
        "lease_to_metadata": [
            {"field_name": "Unit Number", "expected_value": "202", "actual_value": "202", "match": True},
            {"field_name": "Resident Name", "expected_value": "David Wilson", "actual_value": "David Wilson", "match": True},
            {"field_name": "Lease Start Date", "expected_value": "2024-04-15", "actual_value": "2024-04-15", "match": True},
            {"field_name": "Lease End Date", "expected_value": "2025-04-14", "actual_value": "2025-04-14", "match": True},
        ],
        "lease_to_ledger": [
            {"field_name": "Monthly Rent", "expected_value": "$1950.00", "actual_value": "$1950.00", "match": True},
            {"field_name": "Utility Charge", "expected_value": "$150.00", "actual_value": "$125.00", "match": False},
        ],
        "metadata_to_ledger": [
            {"field_name": "Lease Start Date", "expected_value": "2024-04-15", "actual_value": "2024-04-15", "match": True},
            {"field_name": "Lease End Date", "expected_value": "2025-04-14", "actual_value": "2025-04-15", "match": False},
        ]
    },
    "LSE006": {
        "lease_to_metadata": [
            {"field_name": "Unit Number", "expected_value": "301", "actual_value": "301", "match": True},
            {"field_name": "Resident Name", "expected_value": "Jennifer Martinez", "actual_value": "Jennifer Martinez", "match": True},
            {"field_name": "Co-signer", "expected_value": "Maria Martinez", "actual_value": "None", "match": False},
            {"field_name": "Pets", "expected_value": "1 Dog, 1 Cat", "actual_value": "1 Dog", "match": False},
            {"field_name": "Renewal Date", "expected_value": "2024-12-01", "actual_value": "Not set", "match": False},
        ],
        "lease_to_ledger": [
            {"field_name": "Monthly Rent", "expected_value": "$1800.00", "actual_value": "$1700.00", "match": False},
            {"field_name": "Pet Rent", "expected_value": "$50.00", "actual_value": "$25.00", "match": False},
            {"field_name": "Security Deposit", "expected_value": "$1800.00", "actual_value": "$1800.00", "match": True},
        ],
        "metadata_to_ledger": [
            {"field_name": "Monthly Rent", "expected_value": "$1800.00", "actual_value": "$1700.00", "match": False},
            {"field_name": "Lease Start Date", "expected_value": "2024-01-01", "actual_value": "2024-01-01", "match": True},
        ]
    },
    "LSE007": {
        "lease_to_metadata": [
            {"field_name": "Unit Number", "expected_value": "302", "actual_value": "302", "match": True},
            {"field_name": "Resident Name", "expected_value": "Robert Taylor", "actual_value": "Robert Taylor", "match": True},
            {"field_name": "Move-in Inspection Date", "expected_value": "2024-05-01", "actual_value": "Not recorded", "match": False},
            {"field_name": "Emergency Contact", "expected_value": "Jane Taylor (555-0123)", "actual_value": "Not recorded", "match": False},
            {"field_name": "Concession End Date", "expected_value": "2024-08-31", "actual_value": "2024-09-30", "match": False},
        ],
        "lease_to_ledger": [
            {"field_name": "Monthly Rent", "expected_value": "$1750.00", "actual_value": "$1750.00", "match": True},
            {"field_name": "Storage Unit Fee", "expected_value": "$100.00", "actual_value": "$0.00", "match": False},
            {"field_name": "Security Deposit", "expected_value": "$1750.00", "actual_value": "$1750.00", "match": True},
        ],
        "metadata_to_ledger": [
            {"field_name": "Concession End Date", "expected_value": "2024-09-30", "actual_value": "2024-08-31", "match": False},
            {"field_name": "Monthly Rent", "expected_value": "$1750.00", "actual_value": "$1750.00", "match": True},
        ]
    }
}


# ============================================================================
# PORTFOLIO-LEVEL FUNCTIONS
# ============================================================================

def get_portfolio_summary():
    """
    Returns portfolio-wide audit summary metrics.
    
    Returns:
        dict: Portfolio totals and KPIs
    """
    total_leases = len(LEASES)
    total_leases_audited = total_leases  # In our mock, all are audited
    open_flags = [f for f in FLAGS if not f["resolved"]]
    open_flags_count = len(open_flags)
    
    # Calculate estimated dollar impact from critical/high severity open flags
    impact_by_severity = {"Critical": 500, "High": 200, "Medium": 50, "Low": 10}
    estimated_dollar_impact = sum(
        impact_by_severity.get(f["severity"], 0) for f in open_flags
    )
    
    # Calculate average accuracy score
    avg_accuracy = sum(lease["accuracy_score"] for lease in LEASES) / len(LEASES) if LEASES else 0
    
    return {
        "total_properties": len(PROPERTIES),
        "total_leases": total_leases,
        "total_leases_audited": total_leases_audited,
        "open_flags_count": open_flags_count,
        "resolved_flags_count": len(FLAGS) - open_flags_count,
        "estimated_dollar_impact": estimated_dollar_impact,
        "average_accuracy_score": round(avg_accuracy, 1)
    }


def get_properties_audit_summary():
    """
    Returns audit summary for all properties in the portfolio.
    
    Returns:
        list: List of property audit summaries
    """
    return deepcopy(PROPERTIES)


# ============================================================================
# PROPERTY-LEVEL FUNCTIONS
# ============================================================================

def get_property_audit_summary(property_id):
    """
    Returns detailed audit summary for a specific property.
    
    Args:
        property_id (str): The property identifier
        
    Returns:
        dict: Property details and audit metrics, or None if not found
    """
    property_data = next((p for p in PROPERTIES if p["property_id"] == property_id), None)
    if not property_data:
        return None
    
    # Get property leases
    property_leases = [l for l in LEASES if l["property_id"] == property_id]
    property_flags = [f for f in FLAGS if f["property_id"] == property_id and not f["resolved"]]
    
    result = deepcopy(property_data)
    result["total_leases"] = len(property_leases)
    result["active_leases"] = len([l for l in property_leases if l["lease_status"] == "Active"])
    result["total_flags"] = len([f for f in FLAGS if f["property_id"] == property_id])
    result["resolved_flags"] = len([f for f in FLAGS if f["property_id"] == property_id and f["resolved"]])
    
    return result


def get_property_leases(property_id):
    """
    Returns all leases for a specific property with audit metrics.
    
    Args:
        property_id (str): The property identifier
        
    Returns:
        list: List of leases for the property
    """
    property_leases = [l for l in LEASES if l["property_id"] == property_id]
    return deepcopy(property_leases)


# ============================================================================
# LEASE-LEVEL FUNCTIONS
# ============================================================================

def get_lease_header(lease_id):
    """
    Returns lease header information.
    
    Args:
        lease_id (str): The lease identifier
        
    Returns:
        dict: Lease header fields, or None if not found
    """
    lease = next((l for l in LEASES if l["lease_id"] == lease_id), None)
    if not lease:
        return None
    
    return {
        "lease_id": lease["lease_id"],
        "property_id": lease["property_id"],
        "property_name": lease["property_name"],
        "unit_number": lease["unit_number"],
        "resident_name": lease["resident_name"],
        "lease_start_date": lease["lease_start_date"],
        "lease_end_date": lease["lease_end_date"],
        "lease_status": lease["lease_status"],
        "monthly_rent": lease["monthly_rent"],
        "accuracy_score": lease["accuracy_score"],
        "open_flags_count": lease["open_flags_count"]
    }


def get_lease_comparisons(lease_id):
    """
    Returns all comparison data for a lease across three comparison groups.
    
    Args:
        lease_id (str): The lease identifier
        
    Returns:
        dict: Comparison groups (lease_to_metadata, lease_to_ledger, metadata_to_ledger)
              or None if lease not found
    """
    if lease_id not in LEASE_COMPARISONS:
        return None
    
    return deepcopy(LEASE_COMPARISONS[lease_id])


def get_lease_flags(lease_id):
    """
    Returns all audit flags for a specific lease.
    
    Args:
        lease_id (str): The lease identifier
        
    Returns:
        list: List of flags for the lease
    """
    lease_flags = [f for f in FLAGS if f["lease_id"] == lease_id]
    return deepcopy(lease_flags)


# ============================================================================
# GLOBAL / WORKFLOW FUNCTIONS
# ============================================================================

def get_all_flags(status=None):
    """
    Returns all flags across the portfolio, optionally filtered by status.
    
    Args:
        status (str, optional): Filter by 'open', 'resolved', or None for all
        
    Returns:
        list: List of flags matching the filter criteria
    """
    if status == "open":
        filtered = [f for f in FLAGS if not f["resolved"]]
    elif status == "resolved":
        filtered = [f for f in FLAGS if f["resolved"]]
    else:
        filtered = FLAGS
    
    return deepcopy(filtered)


def resolve_flag(flag_id):
    """
    Marks a flag as resolved.
    
    Args:
        flag_id (str): The flag identifier
        
    Returns:
        bool: True if flag was found and resolved, False otherwise
    """
    for flag in FLAGS:
        if flag["flag_id"] == flag_id:
            flag["resolved"] = True
            flag["resolved_date"] = datetime.now().strftime("%Y-%m-%d")
            return True
    return False
