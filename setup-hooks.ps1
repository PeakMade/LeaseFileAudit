# Setup Git Hooks for LeaseFileAudit
# This script installs pre-commit hooks to remind developers to update documentation

Write-Host ""
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Cyan
Write-Host "  Git Hooks Setup - LeaseFileAudit" -ForegroundColor Cyan
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Cyan
Write-Host ""

# Check if .git directory exists
if (-not (Test-Path ".git")) {
    Write-Host "Error: Not in a git repository. Please run from project root." -ForegroundColor Red
    exit 1
}

# Create .git/hooks directory if it doesn't exist
if (-not (Test-Path ".git/hooks")) {
    New-Item -ItemType Directory -Path ".git/hooks" -Force | Out-Null
}

# Copy PowerShell pre-commit hook
Write-Host "Installing pre-commit hook..." -ForegroundColor Yellow

$sourceHook = ".githooks\pre-commit.ps1"
$targetHook = ".git\hooks\pre-commit"

if (Test-Path $sourceHook) {
    # Create a wrapper that calls PowerShell script
    $wrapperContent = @"
#!/bin/sh
# Git hook wrapper that calls PowerShell script
powershell.exe -ExecutionPolicy Bypass -File "`$(git rev-parse --show-toplevel)/.githooks/pre-commit.ps1"
exit `$?
"@
    
    Set-Content -Path $targetHook -Value $wrapperContent
    Write-Host "Pre-commit hook installed at .git/hooks/pre-commit" -ForegroundColor Green
} else {
    Write-Host "Source hook not found: $sourceHook" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Cyan
Write-Host "  Setup Complete!" -ForegroundColor Green
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Cyan
Write-Host ""
Write-Host "The pre-commit hook will now remind you to update" -ForegroundColor White
Write-Host "MASTER_DOCUMENTATION.md whenever you commit code changes." -ForegroundColor White
Write-Host ""
Write-Host "See CONTRIBUTING.md for documentation guidelines." -ForegroundColor Cyan
Write-Host ""
