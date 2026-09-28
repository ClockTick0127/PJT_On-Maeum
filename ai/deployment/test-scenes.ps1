param(
    [Parameter(Mandatory=$true)][string]$OutputDirectory,
    [string]$Docker = 'docker',
    [string]$OpenSSL = 'openssl',
    [string[]]$Appliances = @('kettle','induction','iron','microwave','hair_dryer','vacuum_cleaner'),
    [ValidateRange(0,60)][double]$Interval = 0
)
$ErrorActionPreference = 'Stop'
$repo = (Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
$output = [IO.Path]::GetFullPath($OutputDirectory)
if (Test-Path -LiteralPath $output) { throw 'Use a new output directory; never overwrite evidence.' }
if ($output.StartsWith($repo + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)) {
    throw 'Keep generated test keys and raw evidence outside the repository.'
}
$env:TEST_CERT_DIR = Join-Path $output 'tls'
$env:TEST_REPORT_DIR = Join-Path $output 'broker'
New-Item -ItemType Directory -Path $env:TEST_CERT_DIR,$env:TEST_REPORT_DIR | Out-Null
function Invoke-Docker {
    & $Docker @args
    if ($LASTEXITCODE -ne 0) { throw "Docker failed (exit $LASTEXITCODE). Test stack retained for diagnosis." }
}
Push-Location $repo
try {
    & $OpenSSL req -x509 -newkey rsa:2048 -nodes -keyout "$env:TEST_CERT_DIR/server.key" -out "$env:TEST_CERT_DIR/ca.crt" -days 2 -subj '/CN=mosquitto' -addext 'subjectAltName=DNS:mosquitto'
    if ($LASTEXITCODE -ne 0) { throw 'Test certificate generation failed' }
    @'
listener 8883
allow_anonymous true
certfile /mosquitto/config/ca.crt
keyfile /mosquitto/config/server.key
persistence false
'@ | Set-Content -Encoding ascii "$env:TEST_CERT_DIR/mosquitto.conf"
    $compose = @('compose','-f','ai/deployment/compose.test.yaml')
    Invoke-Docker @compose up -d --wait postgres kafka mosquitto
    foreach ($topic in @('power.demo.raw.v1','analysis.scene.v2','analysis.scene-session.v1','analysis.scene-event.v1','dlq.analysis')) {
        Invoke-Docker @compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic $topic --partitions 1 --replication-factor 1
    }
    $batch = [guid]::NewGuid().ToString('N').Substring(0,12)
    $env:TEST_INTERVAL = $Interval.ToString([Globalization.CultureInfo]::InvariantCulture)
    foreach ($appliance in $Appliances) {
        $env:TEST_APPLIANCE = $appliance
        $env:TEST_RUN_ID = "acceptance-$batch-$appliance"
        Invoke-Docker @compose up -d --wait worker bridge
        Invoke-Docker @compose run --rm fixture
        Invoke-Docker @compose run --rm verifier
        Invoke-Docker @compose stop worker
    }
    Invoke-Docker @compose stop bridge mosquitto kafka postgres
    Write-Output "AI broker verification complete: $env:TEST_REPORT_DIR"
    Write-Output 'Backend API, notification creation, production authentication and UI require separate acceptance.'
} finally {
    Pop-Location
}
