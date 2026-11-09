param(
    [string]$Container = 'flowlens-load',
    [int]$IntervalSec = 1,
    [string]$OutFile = 'memory.csv'
)

$ErrorActionPreference = 'SilentlyContinue'

"ts,memory_current,memory_peak,memory_max,oom,oom_kill,mem_usage" | Set-Content -LiteralPath $OutFile -Encoding UTF8

while ($true) {
    $ts = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
    $cur = (docker exec $Container sh -c "cat /sys/fs/cgroup/memory.current").Trim()
    $peak = (docker exec $Container sh -c "cat /sys/fs/cgroup/memory.peak").Trim()
    $max = (docker exec $Container sh -c "cat /sys/fs/cgroup/memory.max").Trim()
    $events = docker exec $Container sh -c "cat /sys/fs/cgroup/memory.events"
    $oom = (($events | Select-String '^oom ').ToString() -replace '^oom\s+','').Trim()
    $oomKill = (($events | Select-String '^oom_kill ').ToString() -replace '^oom_kill\s+','').Trim()
    $usage = (docker stats --no-stream --format '{{.MemUsage}}' $Container).Trim()

    Add-Content -LiteralPath $OutFile -Value "$ts,$cur,$peak,$max,$oom,$oomKill,$usage" -Encoding UTF8
    Start-Sleep -Seconds $IntervalSec
}