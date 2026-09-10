param(
    [string]$KafkaHome = "$PSScriptRoot/../tmp/kafka/kafka_2.13-4.1.2"
)
$ErrorActionPreference = 'Stop'
$kafkaRoot = (Resolve-Path -LiteralPath $KafkaHome).Path
$projectRoot = (Resolve-Path -LiteralPath "$PSScriptRoot/..").Path
$dataDir = Join-Path $projectRoot 'data/kafka'
New-Item -ItemType Directory -Force -Path $dataDir | Out-Null
$configFile = Join-Path $dataDir 'broker.properties'
$logDir = (Join-Path $dataDir 'logs').Replace('\', '/')
$classPath = Join-Path $kafkaRoot 'libs/*'
$loggingConfig = Join-Path $kafkaRoot 'config/log4j2.yaml'

# Calling Java directly avoids the long classpath limit in Kafka's Windows .bat scripts.
@"
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:9093
listeners=PLAINTEXT://127.0.0.1:9092,CONTROLLER://127.0.0.1:9093
advertised.listeners=PLAINTEXT://localhost:9092
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
inter.broker.listener.name=PLAINTEXT
log.dirs=$logDir
num.partitions=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
auto.create.topics.enable=false
"@ | Set-Content -LiteralPath $configFile -Encoding ascii

if (-not (Test-Path -LiteralPath "$logDir/meta.properties")) {
    $clusterId = [guid]::NewGuid().ToString('N')
    & java "-Dlog4j2.configurationFile=$loggingConfig" -cp $classPath kafka.tools.StorageTool format -t $clusterId -c $configFile
    if ($LASTEXITCODE -ne 0) { throw 'Kafka storage formatting failed' }
}

Write-Host 'Starting Kafka on localhost:9092. Keep this terminal open; press Ctrl+C to stop.'
& java -Xms256m -Xmx512m "-Dlog4j2.configurationFile=$loggingConfig" -cp $classPath kafka.Kafka $configFile
if ($LASTEXITCODE -ne 0) { throw "Kafka exited with code $LASTEXITCODE" }
