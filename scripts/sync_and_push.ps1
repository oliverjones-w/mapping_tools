<#
.SYNOPSIS
    Sync K drive Excel maps to SQLite, then push DBs to Mac mini.
    Called by the MappingToolsSync scheduled task every hour.
#>

$ErrorActionPreference = "Stop"

$ProjectRoot  = Split-Path -Parent $PSScriptRoot
$Python       = "$ProjectRoot\.venv\Scripts\python.exe"
$ScriptsDir   = "$ProjectRoot\scripts"
$LogFile      = "$ProjectRoot\logs\sync.log"
$RemotePath   = "macdev:/Users/dev-server/workspace/services/mapping_tools"

function Log($msg) {
    $line = "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] $msg"
    Write-Host $line
    Add-Content -Path $LogFile -Value $line
}

# Ensure logs dir exists
New-Item -ItemType Directory -Force -Path "$ProjectRoot\logs" | Out-Null

Log "=== Sync started ==="

# ---------------------------------------------------------------------------
# Step 1 — Sync Excel -> SQLite (must run from scripts/ for local imports)
# ---------------------------------------------------------------------------

Push-Location $ScriptsDir

try {
    Log "Syncing hf_map..."
    & $Python sync_hf_map.py
    if ($LASTEXITCODE -ne 0) { throw "sync_hf_map.py failed (exit $LASTEXITCODE)" }

    Log "Syncing ir_map..."
    & $Python sync_ir_map.py
    if ($LASTEXITCODE -ne 0) { throw "sync_ir_map.py failed (exit $LASTEXITCODE)" }
}
catch {
    Log "ERROR during sync: $_"
    Pop-Location
    exit 1
}

Pop-Location

# ---------------------------------------------------------------------------
# Step 2 — Push DBs to Mac mini
# ---------------------------------------------------------------------------

try {
    Log "Pushing hf_map.db..."
    scp "$ProjectRoot\hf_map.db" "${RemotePath}/hf_map.db"
    if ($LASTEXITCODE -ne 0) { throw "scp hf_map.db failed (exit $LASTEXITCODE)" }

    Log "Pushing ir_map.db..."
    scp "$ProjectRoot\ir_map.db" "${RemotePath}/ir_map.db"
    if ($LASTEXITCODE -ne 0) { throw "scp ir_map.db failed (exit $LASTEXITCODE)" }

    if (Test-Path "$ProjectRoot\bbg_results.db") {
        Log "Pushing bbg_results.db..."
        scp "$ProjectRoot\bbg_results.db" "${RemotePath}/bbg_results.db"
        if ($LASTEXITCODE -ne 0) { throw "scp bbg_results.db failed (exit $LASTEXITCODE)" }
    } else {
        Log "bbg_results.db not found — skipping (no extractions run yet)."
    }
}
catch {
    Log "ERROR during push: $_"
    exit 1
}

Log "=== Done ==="
