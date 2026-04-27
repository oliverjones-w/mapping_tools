<#
.SYNOPSIS
    Registers the MappingToolsSync scheduled task.
    Run once from PowerShell — no UI needed.

.USAGE
    powershell -ExecutionPolicy Bypass -File scripts/register_task.ps1

.NOTES  (run these from Git Bash with // prefix to avoid path mangling)
    Run now : schtasks //run //tn MappingToolsSync
    Status  : schtasks //query //tn MappingToolsSync
    Remove  : schtasks //delete //tn MappingToolsSync //f
#>

$ErrorActionPreference = "Stop"

$TaskName = "MappingToolsSync"
$Script   = (Resolve-Path "$PSScriptRoot\sync_and_push.ps1").Path

$PwshCmd = Get-Command pwsh -ErrorAction SilentlyContinue
$PwshExe = if ($PwshCmd) { $PwshCmd.Source } else { "powershell.exe" }

$Action = New-ScheduledTaskAction `
    -Execute $PwshExe `
    -Argument "-NoLogo -NoProfile -NonInteractive -WindowStyle Hidden -ExecutionPolicy Bypass -File `"$Script`""

# -RepetitionInterval on -Once trigger = repeat indefinitely every hour (PS5 native)
$StartAt = (Get-Date).AddMinutes(1)
$Trigger = New-ScheduledTaskTrigger `
    -Once `
    -At $StartAt `
    -RepetitionInterval (New-TimeSpan -Hours 1)

$Settings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 10) `
    -StartWhenAvailable `
    -RunOnlyIfNetworkAvailable `
    -MultipleInstances IgnoreNew

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action   $Action `
    -Trigger  $Trigger `
    -Settings $Settings `
    -Force | Out-Null

Write-Host "Task '$TaskName' registered."
Write-Host "  First run : $StartAt"
Write-Host "  Interval  : every 1 hour"
Write-Host ""
Write-Host "Git Bash commands (use // to avoid path mangling):"
Write-Host "  Run now : schtasks //run //tn MappingToolsSync"
Write-Host "  Status  : schtasks //query //tn MappingToolsSync"
Write-Host "  Remove  : schtasks //delete //tn MappingToolsSync //f"
