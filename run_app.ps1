param(
    [int]$Port = 8000,
    [string]$AppFile = "app.py",
    [int]$StartupTimeoutSec = 90,
    [switch]$Background
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $projectRoot

function Stop-ListenerOnPort {
    param([int]$TargetPort)

    $listeners = Get-NetTCPConnection -LocalPort $TargetPort -State Listen -ErrorAction SilentlyContinue
    if (-not $listeners) {
        Write-Host "No existing listener on port $TargetPort"
        return
    }

    $pids = $listeners | Select-Object -ExpandProperty OwningProcess -Unique
    foreach ($procId in $pids) {
        try {
            Stop-Process -Id $procId -Force -ErrorAction Stop
            Write-Host ("Stopped stale PID {0} on port {1}" -f $procId, $TargetPort)
        }
        catch {
            Write-Warning ("Could not stop PID {0}: {1}" -f $procId, $_.Exception.Message)
        }
    }
}

Stop-ListenerOnPort -TargetPort $Port

if (-not $Background) {
    $env:OPEN_BROWSER = "true"
    $env:PORT = "$Port"
    Write-Host ("Starting {0} in foreground on port {1}; terminal output will stream below." -f $AppFile, $Port)
    & python $AppFile
    exit $LASTEXITCODE
}

$env:OPEN_BROWSER = "false"
$env:PORT = "$Port"
$appProc = Start-Process -FilePath "python" -ArgumentList $AppFile -WorkingDirectory $projectRoot -PassThru
Write-Host ("Started {0} with PID {1} in background mode" -f $AppFile, $appProc.Id)

$uri = "http://127.0.0.1:$Port"
$deadline = (Get-Date).AddSeconds($StartupTimeoutSec)
$healthy = $false

while ((Get-Date) -lt $deadline) {
    if ($appProc.HasExited) {
        throw ("App process exited early with code {0}" -f $appProc.ExitCode)
    }

    try {
        $resp = Invoke-WebRequest -Uri $uri -UseBasicParsing -TimeoutSec 5
        if ($resp.StatusCode -ge 200 -and $resp.StatusCode -lt 500) {
            $healthy = $true
            break
        }
    }
    catch {
        # Still starting up; keep waiting.
    }

    Start-Sleep -Milliseconds 500
}

if (-not $healthy) {
    throw ("App did not become healthy within {0}s at {1}" -f $StartupTimeoutSec, $uri)
}

Start-Process $uri
Write-Host ("App is ready at {0}" -f $uri)
Write-Host ("Use Stop-Process -Id {0} to stop it, or rerun this script to restart cleanly." -f $appProc.Id)
