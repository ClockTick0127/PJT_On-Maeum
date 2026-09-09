param([switch]$Apply)
$ErrorActionPreference = 'Stop'
if (-not $Apply) {
    Write-Host 'With -Apply: stop legacy Kafka, back up /tmp/kafka-logs, copy to its empty named volume, and remove the three stopped legacy backend containers.'
    Write-Host 'Existing PostgreSQL and its volume are preserved. No containers are changed without -Apply.'
    exit 0
}
function Invoke-Docker {
    param([Parameter(ValueFromRemainingArguments=$true)][string[]]$Arguments)
    & docker @Arguments
    if ($LASTEXITCODE -ne 0) { throw "Docker operation failed: $($Arguments[0])" }
}
$repoPath = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '../..'))
$backupPath = [IO.Path]::GetFullPath((Join-Path $repoPath ('.deployment/kafka-backup-' + [DateTime]::UtcNow.ToString('yyyyMMddHHmmss'))))
if (-not $backupPath.StartsWith($repoPath + [IO.Path]::DirectorySeparatorChar)) {
    throw 'Backup path must remain inside the repository.'
}
$project = (& docker inspect nilm-kafka --format '{{index .Config.Labels "com.docker.compose.project"}}')
if ($LASTEXITCODE -ne 0 -or $project -ne 'nilm-postgres') { throw 'Expected legacy nilm-postgres Kafka container not found.' }
# Refuse to merge/overwrite an existing Kafka volume.
Invoke-Docker run --rm --entrypoint sh --mount type=volume,source=nilm-postgres_kafka_data,target=/data,readonly apache/kafka:3.9.0 -ec 'test -z "$(ls -A /data)"'
Invoke-Docker exec nilm-kafka test -f /tmp/kafka-logs/meta.properties
Invoke-Docker stop nilm-kafka
$null = New-Item -ItemType Directory -Path $backupPath
try {
    Invoke-Docker cp nilm-kafka:/tmp/kafka-logs/. $backupPath
    $clusterLine = Get-Content -LiteralPath (Join-Path $backupPath 'meta.properties') | Where-Object { $_ -match '^cluster.id=' }
    $clusterId = ($clusterLine -split '=', 2)[1]
    if ($clusterId -notmatch '^[A-Za-z0-9_-]+$') { throw 'Cannot read the existing Kafka cluster ID.' }
    $localEnv = Join-Path $PSScriptRoot '.env'
    if (-not (Test-Path -LiteralPath $localEnv)) { throw 'Run Setup-Local.ps1 first.' }
    $envLines = @(Get-Content -LiteralPath $localEnv | Where-Object { $_ -notmatch '^KAFKA_CLUSTER_ID=' })
    $envLines += "KAFKA_CLUSTER_ID=$clusterId"
    [IO.File]::WriteAllLines($localEnv, [string[]]$envLines, [Text.UTF8Encoding]::new($false))
    Invoke-Docker run --rm --user 0 --entrypoint sh --mount "type=bind,source=$backupPath,target=/backup,readonly" --mount type=volume,source=nilm-postgres_kafka_data,target=/data apache/kafka:3.9.0 -ec 'test -z "$(ls -A /data)"; cp -a /backup/. /data/; chown -R 1000:1000 /data'
} catch {
    docker start nilm-kafka | Out-Null
    throw
}
foreach ($name in @('nilm-gateway', 'nilm-iot-device', 'nilm-monitoring')) {
    $owner = (& docker inspect $name --format '{{index .Config.Labels "com.docker.compose.project"}}' 2>$null)
    if ($LASTEXITCODE -eq 0 -and $owner -eq 'nilm-backend') {
        Invoke-Docker stop $name
        Invoke-Docker rm $name
    }
}
Write-Host "Kafka backup retained at $backupPath"
Write-Host 'Kafka is stopped. From infrastructure/local run: docker compose up -d --build'
Write-Host 'Do not use docker compose down -v.'
