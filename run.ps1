$ErrorActionPreference = 'Stop'

$jar = Join-Path $PSScriptRoot 'target\flowlens-0.1.0-SNAPSHOT.jar'
if (-not (Test-Path -LiteralPath $jar)) {
    throw "Jar not found: $jar. Run 'mvn clean package' first."
}

java `
  -Xms64m `
  -Xmx160m `
  -XX:+UseG1GC `
  -XX:MaxMetaspaceSize=64m `
  -XX:ReservedCodeCacheSize=64m `
  -XX:+UseCodeCacheFlushing `
  -XX:MaxDirectMemorySize=16m `
  -XX:NativeMemoryTracking=summary `
  -Xss256k `
  -XX:ActiveProcessorCount=4 `
  -jar $jar