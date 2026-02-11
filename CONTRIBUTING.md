# Contributing to Lease File Audit

## Documentation-First Rule ⚠️

**CRITICAL**: Before committing code changes, you MUST update [MASTER_DOCUMENTATION.md](MASTER_DOCUMENTATION.md) if your changes affect:

- ✅ Data flow or reconciliation logic
- ✅ New features or functionality
- ✅ Configuration changes (environment variables, settings)
- ✅ Database/SharePoint schema changes
- ✅ API endpoints or route handlers
- ✅ New files or restructuring
- ✅ Deployment process
- ✅ Troubleshooting scenarios

## Commit Checklist

Before every `git commit`, ask yourself:

1. **Did I change how the app works?** → Update "Data Flow & Audit Process" section
2. **Did I add/remove files?** → Update "Project Structure" section
3. **Did I add/change environment variables?** → Update "Configuration & Environment" section
4. **Did I modify reconciliation logic?** → Update "Reconciliation Engine" section
5. **Did I add SharePoint lists/columns?** → Update "SharePoint Integration" section
6. **Did I fix a bug?** → Add to "Common Scenarios & Troubleshooting" section
7. **Did I change deployment steps?** → Update "Deployment" section

## Documentation Update Workflow

### 1. Make Code Changes
```bash
# Work on your feature
git checkout -b feature/my-feature
# ... make changes ...
```

### 2. Update Documentation **BEFORE** Committing
```bash
# Open MASTER_DOCUMENTATION.md
code MASTER_DOCUMENTATION.md

# Find relevant section(s) and update:
# - Add new component explanation
# - Update data flow diagrams
# - Add troubleshooting tips
# - Update configuration examples
```

### 3. Commit Both Together
```bash
git add .
git commit -m "feat: Add new feature X

- Implemented feature X in file Y
- Updated MASTER_DOCUMENTATION.md sections:
  - Core Components (added FeatureX explanation)
  - Configuration (added NEW_ENV_VAR)
  - Troubleshooting (added scenario for issue Z)
"
```

### 4. Review Before Push
```bash
# Self-review: Did I update docs?
git diff main MASTER_DOCUMENTATION.md

# If no diff shown, ask: Should there be documentation changes?
```

## Examples of Good Documentation Updates

### Example 1: Adding New Field
**Code Change**: Added `UNIT_NUMBER` field to lease view

**Documentation Update**:
```markdown
### Scenario 1: Adding a New Data Field (UPDATED)

**Example**: Add "Unit Number" to lease view

1. **Add to Canonical Fields**
   ```python
   # audit_engine/canonical_fields.py
   class CanonicalField(Enum):
       UNIT_NUMBER = "UNIT_NUMBER"
   ```
   
2. **Add to Source Mapping**...
```

### Example 2: Fixing Bug
**Code Change**: Fixed resolved exception filtering

**Documentation Update**:
```markdown
### Issue: Resolved exceptions still showing in current metrics

**Symptom**: Exception count doesn't decrease when marking as resolved

**Fix**: Check that SharePoint Exception Months list is accessible...
```

### Example 3: New Environment Variable
**Code Change**: Added `MAX_UPLOAD_SIZE_MB` config

**Documentation Update**:
```bash
# Configuration & Environment section
MAX_UPLOAD_SIZE_MB=100  # Maximum Excel file size in megabytes
```

## What NOT to Document

- Tiny refactoring that doesn't change behavior
- Comment-only changes
- Code formatting/linting fixes
- Version bumps without functionality changes

## Documentation Standards

### Use Clear Headings
```markdown
## Major Section
### Subsection
#### Detail Level
```

### Include Code Examples
Always show practical examples, not just descriptions:
```python
# GOOD: Shows actual code
storage.load_exception_months_from_sharepoint_list(run_id, property_id, lease_id, ar_code_id)

# BAD: Just describes
"Call the method to load exceptions"
```

### Update Change Log
At bottom of MASTER_DOCUMENTATION.md:
```markdown
### Change Log
- **2026-02-11**: Fixed duplicate loop in resolved exception filtering
- **2026-02-11**: Added comprehensive master documentation
```

### Keep It Accurate
- Remove outdated information
- Don't leave contradictory statements
- Update version numbers
- Fix broken links

## Enforcement

### Pre-Commit Hook (Optional)
We've included a git hook reminder. To enable:

```bash
# Make hook executable
chmod +x .git/hooks/pre-commit

# On Windows (PowerShell):
# The hook will run automatically
```

The hook will remind you to update docs when you commit code changes.

### Code Review Checklist
Reviewers should check:
- [ ] MASTER_DOCUMENTATION.md is included in PR if code changed
- [ ] Documentation changes are accurate
- [ ] Examples work (test code snippets)
- [ ] No contradictions with existing docs

## Questions?

If unsure whether to update docs, **err on the side of updating**. It's easier to remove unnecessary docs than to reconstruct lost knowledge.

**Contact**: svanorder@peakmade.com

---

**Remember**: Code without documentation is code that will be rewritten. Document as you go! 📝✅
