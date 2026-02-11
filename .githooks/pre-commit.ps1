# Pre-commit hook (PowerShell version for Windows)
# Reminds developers to update MASTER_DOCUMENTATION.md when making code changes

# Get list of staged files
$stagedFiles = git diff --cached --name-only

# Check if any code files are being committed
$codeChanged = $false
foreach ($file in $stagedFiles) {
    if ($file -match '\.(py|js|html|css)$|^config\.py$|^requirements\.txt$|^\.env') {
        $codeChanged = $true
        break
    }
}

# Check if MASTER_DOCUMENTATION.md is being updated
$docUpdated = $stagedFiles -contains "MASTER_DOCUMENTATION.md"

# If code changed but docs not updated, show reminder
if ($codeChanged -and -not $docUpdated) {
    Write-Host ""
    Write-Host "=======================================" -ForegroundColor Yellow
    Write-Host "   DOCUMENTATION REMINDER" -ForegroundColor Yellow
    Write-Host "=======================================" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "You are committing code changes, but " -NoNewline
    Write-Host "MASTER_DOCUMENTATION.md" -ForegroundColor Red -NoNewline
    Write-Host " is not included."
    Write-Host ""
    Write-Host "Does your change require documentation updates?" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "   - New features or functionality" -ForegroundColor Green
    Write-Host "   - Configuration changes" -ForegroundColor Green
    Write-Host "   - Data flow modifications" -ForegroundColor Green
    Write-Host "   - Bug fixes (add to troubleshooting)" -ForegroundColor Green
    Write-Host "   - New files or restructuring" -ForegroundColor Green
    Write-Host ""
    Write-Host "If YES, please:" -ForegroundColor Yellow
    Write-Host "  1. Ctrl+C to cancel this commit"
    Write-Host "  2. Update MASTER_DOCUMENTATION.md"
    Write-Host "  3. Run: git add MASTER_DOCUMENTATION.md"
    Write-Host "  4. Commit again"
    Write-Host ""
    Write-Host "If NO (refactoring/formatting only):" -ForegroundColor Yellow
    Write-Host "  - Type 'y' and press Enter to continue"
    Write-Host ""
    Write-Host "See CONTRIBUTING.md for documentation guidelines" -ForegroundColor Cyan
    Write-Host "=======================================" -ForegroundColor Yellow
    Write-Host ""
    
    # Prompt user to continue or cancel
    $response = Read-Host "Continue with commit? [y/N]"
    if ($response -match '^[yY]') {
        Write-Host "Proceeding with commit" -ForegroundColor Green
        exit 0
    } else {
        Write-Host "Commit cancelled. Please update documentation." -ForegroundColor Red
        exit 1
    }
} else {
    # Either no code changed, or docs were updated
    if ($docUpdated) {
        Write-Host "Documentation updated - good job!" -ForegroundColor Green
    }
    exit 0
}
