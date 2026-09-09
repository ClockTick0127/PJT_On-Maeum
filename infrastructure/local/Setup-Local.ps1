param([switch]$InitializeMqtt)
$ErrorActionPreference = 'Stop'
$envPath = Join-Path $PSScriptRoot '.env'
$infraPath = Split-Path $PSScriptRoot -Parent
$repoPath = Split-Path $infraPath -Parent
if (-not (Test-Path -LiteralPath $envPath)) {
    $overrides = @{}
    $legacyInfra = Join-Path $infraPath '.env'
    if (Test-Path -LiteralPath $legacyInfra) {
        foreach ($line in Get-Content -LiteralPath $legacyInfra) {
            if ($line -match '^([A-Z_]+)=(.*)$') { $overrides[$Matches[1]] = $line }
        }
    }
    $legacyBackend = Join-Path $repoPath 'backend/.env'
    if (Test-Path -LiteralPath $legacyBackend) {
        foreach ($line in Get-Content -LiteralPath $legacyBackend) {
            if ($line -match '^(JWT_ISSUER_URI|CORS_ALLOWED_ORIGINS)=(.*)$') {
                $overrides[$Matches[1]] = $line
            }
        }
    }
    $lines = foreach ($line in Get-Content -LiteralPath (Join-Path $PSScriptRoot '.env.example')) {
        if ($line -match '^([A-Z_]+)=' -and $overrides.ContainsKey($Matches[1])) {
            $overrides[$Matches[1]]
        } else { $line }
    }
    [IO.File]::WriteAllLines($envPath, [string[]]$lines, [Text.UTF8Encoding]::new($false))
    Write-Host 'Created local/.env, preserving existing database credentials.'
} else {
    Write-Host 'Existing local/.env preserved.'
}
$mqttConfig = Join-Path $infraPath 'mqtt/config'
$passwdPath = Join-Path $mqttConfig 'passwd'
if ($InitializeMqtt) {
    if ((Test-Path -LiteralPath $passwdPath) -and (Get-Item -LiteralPath $passwdPath).Length -gt 0) {
        throw 'Existing MQTT passwd preserved. Set MQTT_USER/MQTT_PASS in local/.env to the matching credentials.'
    }
    $initCommand = 'set -eu; test -n "$MQTT_PASS"; test -n "$MQTT_SIMULATOR_PASS"; mosquitto_passwd -b -c /config/passwd "$MQTT_USER" "$MQTT_PASS"; mosquitto_passwd -b /config/passwd "$MQTT_SIMULATOR_USER" "$MQTT_SIMULATOR_PASS"; chmod 644 /config/passwd'
    docker run --rm --env-file $envPath --mount "type=bind,source=$mqttConfig,target=/config" --entrypoint sh eclipse-mosquitto:2 -ec $initCommand
    if ($LASTEXITCODE -ne 0) { throw 'MQTT initialization failed.' }
}
Write-Host 'Set MQTT_USER/MQTT_PASS to match mqtt/config/passwd before starting the bridge.'
Write-Host 'No containers were restarted and no database volumes were changed.'
