<#
.SYNOPSIS
Checks or configures Azure MySQL Flexible Server parameters for this CDC application.
.DESCRIPTION
Requires Azure CLI, az login, and permission to manage server configurations.
Run against the event-producing primary and the read replica used as the CDC source.
This script never restarts a server or changes read-only replication topology.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$Subscription,
    [Parameter(Mandatory = $true)]
    [string]$ResourceGroup,
    [Parameter(Mandatory = $true)]
    [string]$ServerName,
    [switch]$Apply
)

$ErrorActionPreference = "Stop"
if (-not (Get-Command az -ErrorAction SilentlyContinue)) {
    throw "Azure CLI is required. Install it and run az login first."
}

function Invoke-AzureJson {
    param([string[]]$Arguments)
    $output = & az @Arguments --subscription $Subscription --only-show-errors --output json
    if ($LASTEXITCODE -ne 0) {
        throw "Azure CLI failed with exit code $LASTEXITCODE. Earlier changes, if any, are not rolled back."
    }
    if ($output) {
        return (($output -join "`n") | ConvertFrom-Json)
    }
}

$required = [ordered]@{
    log_bin = "ON"
    binlog_format = "ROW"
    binlog_row_image = "FULL"
    binlog_row_metadata = "FULL"
    binlog_transaction_compression = "OFF"
    binlog_row_value_options = ""
}
$scope = @("--resource-group", $ResourceGroup, "--server-name", $ServerName)
$parameters = @(Invoke-AzureJson -Arguments (@("mysql", "flexible-server", "parameter", "list") + $scope))
$changes = @()
$blocked = @()

foreach ($name in $required.Keys) {
    $parameter = $parameters | Where-Object { $_.name -eq $name } | Select-Object -First 1
    if (-not $parameter) {
        Write-Warning "$name is not exposed by Azure on this server. Verify it with SQL."
        $blocked += $name
        continue
    }
    $desired = $required[$name]
    if ($null -eq $parameter.value) {
        Write-Warning "$name has no reported value. Verify it with SQL."
        $blocked += $name
        continue
    }
    $current = [string]$parameter.value
    Write-Host ("{0}: current='{1}', required='{2}', readOnly={3}, dynamic={4}" -f
        $name, $current, $desired, $parameter.isReadOnly, $parameter.isDynamicConfig)
    if ($current -ieq $desired) {
        continue
    }
    if ("$($parameter.isReadOnly)" -ieq "true") {
        Write-Warning "$name is read-only and differs from the required value."
        $blocked += $name
        continue
    }
    $changes += [pscustomobject]@{ Name = $name; Value = $desired }
}

if (-not $Apply) {
    Write-Host "Check only. Run again with -Apply to request supported parameter changes."
} else {
    foreach ($change in $changes) {
        # --value= preserves an empty value on Windows; omitting it resets the parameter.
        $arguments = @("mysql", "flexible-server", "parameter", "set") + $scope +
            @("--name", $change.Name, "--value=$($change.Value)")
        $result = Invoke-AzureJson -Arguments $arguments
        if ($null -eq $result -or $null -eq $result.value -or
            [string]$result.value -ine $change.Value) {
            throw "Azure did not confirm the requested value for $($change.Name). Verify it in the portal."
        }
        Write-Host "Configured $($change.Name)='$($change.Value)'."
        if ("$($result.isConfigPendingRestart)" -ieq "true" -or
            "$($result.isDynamicConfig)" -ieq "false") {
            Write-Warning "A server restart may be required. Schedule it separately; this script will not restart."
        }
    }
}

Write-Host "Verify GLOBAL variables using a new SQL connection on BOTH primary and CDC read replica."
Write-Host "Also verify log_replica_updates (or log_slave_updates) is ON on the read replica."
Write-Host "Reconnect source-writing applications so new sessions inherit the changed settings."
Write-Host "Existing binlog events are not rewritten. Reinitialize CDC after new compliant events reach the replica."
if ($blocked.Count -gt 0) {
    throw "Manual verification/action required for: $($blocked -join ', '). Supported changes may already have been applied."
}
