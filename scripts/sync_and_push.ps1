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
$ScpOptions   = @(
    "-B"                              # never prompt; fail fast in unattended runs
    "-o", "BatchMode=yes"
    "-o", "ConnectTimeout=30"
    "-o", "StrictHostKeyChecking=yes"
)

function Log($msg) {
    $line = "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] $msg"
    Write-Host $line
    Add-Content -Path $LogFile -Value $line
}

function Invoke-LoggedCommand {
    param(
        [Parameter(Mandatory = $true)][string]$FilePath,
        [Parameter(Mandatory = $true)][string[]]$ArgumentList,
        [Parameter(Mandatory = $true)][string]$FailureMessage
    )

    # Use Continue locally so native command stderr (e.g. Python warnings) does
    # not trigger a terminating error before we can check $LASTEXITCODE ourselves.
    $ErrorActionPreference = 'Continue'

    $output = & $FilePath @ArgumentList 2>&1
    $exitCode = $LASTEXITCODE

    foreach ($line in $output) {
        $msg = if ($line -is [System.Management.Automation.ErrorRecord]) {
            $line.Exception.Message
        } else {
            "$line"
        }
        if ($msg.Trim() -ne "") { Log $msg }
    }

    if ($exitCode -ne 0) {
        throw "$FailureMessage (exit $exitCode)"
    }
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
    Invoke-LoggedCommand -FilePath $Python -ArgumentList @("sync_hf_map.py") -FailureMessage "sync_hf_map.py failed"

    Log "Syncing ir_map..."
    Invoke-LoggedCommand -FilePath $Python -ArgumentList @("sync_ir_map.py") -FailureMessage "sync_ir_map.py failed"
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
    Invoke-LoggedCommand -FilePath "scp" -ArgumentList ($ScpOptions + @("$ProjectRoot\hf_map.db", "${RemotePath}/hf_map.db")) -FailureMessage "scp hf_map.db failed"

    Log "Pushing ir_map.db..."
    Invoke-LoggedCommand -FilePath "scp" -ArgumentList ($ScpOptions + @("$ProjectRoot\ir_map.db", "${RemotePath}/ir_map.db")) -FailureMessage "scp ir_map.db failed"

    if (Test-Path "$ProjectRoot\bbg_results.db") {
        Log "Pushing bbg_results.db..."
        Invoke-LoggedCommand -FilePath "scp" -ArgumentList ($ScpOptions + @("$ProjectRoot\bbg_results.db", "${RemotePath}/bbg_results.db")) -FailureMessage "scp bbg_results.db failed"
    } else {
        Log "bbg_results.db not found - skipping (no extractions run yet)."
    }
}
catch {
    Log "ERROR during push: $_"
    exit 1
}

Log "=== Done ==="
