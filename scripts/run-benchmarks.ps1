param(
    [string]$BaseUrl = "http://localhost:8080",
    [double]$MaxP99Ms = 1000
)

$ErrorActionPreference = "Stop"
$BaseUrl = $BaseUrl.TrimEnd("/")
if ($MaxP99Ms -le 0) {
    throw "MaxP99Ms must be greater than zero."
}
$root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$profiles = Join-Path $root "docs\profiles"
$composeFile = Join-Path $root "deploy\docker-compose.yml"
$runId = Get-Date -Format "yyyyMMdd-HHmmss"
$fallbackFile = Join-Path $root ("worker-agent\benchmark-fallback-" + [Guid]::NewGuid().ToString("N") + ".go")
$benchmarkFailed = $false

New-Item -ItemType Directory -Path $profiles -Force | Out-Null

function Save-ProfileSnapshot {
    param([string]$Phase)

    $goroutinePath = Join-Path $profiles ($runId + "-" + $Phase + ".goroutine.txt")
    $heapPath = Join-Path $profiles ($runId + "-" + $Phase + ".heap.pprof")
    $heapTextPath = Join-Path $profiles ($runId + "-" + $Phase + ".heap.txt")
    $goroutine = Invoke-WebRequest -UseBasicParsing `
        -Uri ($BaseUrl.TrimEnd("/") + "/debug/pprof/goroutine?debug=2") -TimeoutSec 15
    [System.IO.File]::WriteAllText($goroutinePath, $goroutine.Content)
    Invoke-WebRequest -UseBasicParsing `
        -Uri ($BaseUrl.TrimEnd("/") + "/debug/pprof/heap") `
        -OutFile $heapPath -TimeoutSec 30
    $heapText = Invoke-WebRequest -UseBasicParsing `
        -Uri ($BaseUrl.TrimEnd("/") + "/debug/pprof/heap?debug=1") -TimeoutSec 15
    [System.IO.File]::WriteAllText($heapTextPath, $heapText.Content)
    $heapMatch = [regex]::Match($heapText.Content, "(?m)^heap profile:\s*(\d+):\s*(\d+)")
    return [PSCustomObject]@{
        Goroutines = [regex]::Matches($goroutine.Content, "(?m)^goroutine \d+ \[").Count
        HeapObjects = if ($heapMatch.Success) { [long]$heapMatch.Groups[1].Value } else { "n/a" }
        HeapBytes = if ($heapMatch.Success) { [long]$heapMatch.Groups[2].Value } else { "n/a" }
    }
}

function Invoke-LocalMySqlQuery {
    param([string]$Query)

    $previousPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $output = & docker compose -f $composeFile exec -T mysql `
            mysql -uroot -patlasfarm-local -N -B atlasfarm -e $Query 2>&1
        $exitCode = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previousPreference
    }
    if ($exitCode -ne 0) {
        throw "MySQL benchmark quota command failed: $($output -join ' ')"
    }
    return @($output | ForEach-Object { [string]$_ })
}

function Test-JobSubmission {
    $payload = @{
        tenant_id = "tenant-a"
        idempotency_key = ("benchmark-preflight-" + [Guid]::NewGuid().ToString("N"))
        command = "echo ok"
        req_cpu = 1
        req_memory_mb = 64
        req_gpu = 0
    } | ConvertTo-Json -Compress
    try {
        $response = Invoke-WebRequest -UseBasicParsing -Uri ($BaseUrl + "/api/v1/jobs") `
            -Method Post -ContentType "application/json" -Body $payload -TimeoutSec 10
    } catch {
        $status = "unknown"
        $body = $_.ErrorDetails.Message
        if ($_.Exception.Response) {
            $status = [int]$_.Exception.Response.StatusCode
            if (-not $body) {
                try {
                    $reader = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
                    $body = $reader.ReadToEnd()
                    $reader.Dispose()
                } catch {
                    $body = $_.Exception.Message
                }
            }
        }
        if (-not $body) { $body = $_.Exception.Message }
        Write-Host "Preflight HTTP $status body: $body" -ForegroundColor Yellow
        throw "Benchmark preflight submission failed."
    }
    if ([int]$response.StatusCode -ne 201) {
        Write-Host "Preflight HTTP $($response.StatusCode) body: $($response.Content)" -ForegroundColor Yellow
        throw "Benchmark preflight job was rejected."
    }
}

$baseline = Save-ProfileSnapshot -Phase "baseline"
$loadOutputPath = Join-Path $profiles ($runId + "-load-output.txt")
$k6 = Get-Command "k6" -ErrorAction SilentlyContinue
$loadType = ""
$loadSummary = ""
$loadOutput = @()
$throughput = "n/a"
$p50 = "n/a"
$p95 = "n/a"
$p99 = "n/a"
$acceptedCount = 0
$submittedCount = 0
$quotaBackup = New-Object System.Collections.Generic.List[object]
$quotasRaised = $false

try {
    $quotaLines = @(Invoke-LocalMySqlQuery -Query `
        "SELECT id, max_cpu, max_ram_mb, max_gpu, max_concurrent_jobs FROM tenants WHERE id IN ('tenant-a','tenant-b','tenant-c') ORDER BY id")
    foreach ($line in $quotaLines) {
        $fields = ([string]$line).Trim() -split "`t"
        if ($fields.Count -ne 5) { continue }
        $quotaBackup.Add([PSCustomObject]@{
            ID = $fields[0]
            CPU = [long]$fields[1]
            MemoryMB = [long]$fields[2]
            GPU = [long]$fields[3]
            ConcurrentJobs = [long]$fields[4]
        })
    }
    if ($quotaBackup.Count -ne 3) {
        throw "Expected to read original quotas for tenant-a, tenant-b, and tenant-c before benchmarking."
    }
    $null = Invoke-LocalMySqlQuery -Query `
        "UPDATE tenants SET max_cpu=100000, max_ram_mb=10000000, max_gpu=10000, max_concurrent_jobs=20000 WHERE id IN ('tenant-a','tenant-b','tenant-c')"
    $quotasRaised = $true
    Write-Host "Temporarily raised benchmark tenant quotas; saved original values will be restored afterward."
    Test-JobSubmission
    Write-Host "Benchmark preflight accepted; beginning the 10,000-job burst."

if ($k6) {
    $loadType = "k6"
    Write-Host "Running k6 10,000-job burst against $BaseUrl"
    $previousPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $loadOutput = & $k6.Source run "-e" ("BASE_URL=" + $BaseUrl) `
            "-e" ("MAX_P99_MS=" + $MaxP99Ms) `
            (Join-Path $root "k6\load-test-10k.js") 2>&1
        $loadExitCode = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previousPreference
    }
    $loadSummary = @($loadOutput | Where-Object {
        $_ -match "jobs_(submitted|accepted)|job_submission_latency_ms|http_reqs"
    }) -join " | "
    $acceptedLine = [string](@($loadOutput | Where-Object { $_ -match "jobs_accepted\s*:" } | Select-Object -First 1))
    $submittedLine = [string](@($loadOutput | Where-Object { $_ -match "jobs_submitted\s*:" } | Select-Object -First 1))
    $latencyLine = [string](@($loadOutput | Where-Object { $_ -match "job_submission_latency_ms\s*:" } | Select-Object -First 1))
    $rateMatch = [regex]::Match($acceptedLine, "jobs_accepted\s*:\s*([\d.]+)\s+([\d.]+)/s")
    $latencyMatch = [regex]::Match($latencyLine, "p\(50\)=([\d.]+)ms.*?p\(95\)=([\d.]+)ms.*?p\(99\)=([\d.]+)ms")
    if ($rateMatch.Success) {
        $acceptedCount = [int]$rateMatch.Groups[1].Value
        $throughput = $rateMatch.Groups[2].Value
    }
    $submittedMatch = [regex]::Match($submittedLine, "jobs_submitted\s*:\s*([\d.]+)")
    if ($submittedMatch.Success) { $submittedCount = [int]$submittedMatch.Groups[1].Value }
    if ($acceptedCount -lt 10000) {
        $failures = @($loadOutput | Where-Object { $_ -match "job failure sample:" })
        if ($failures.Count -eq 0) {
            Write-Host "Load rejected requests; sample HTTP status/body was not emitted by k6. Inspect the response checks and raw output at $loadOutputPath." -ForegroundColor Yellow
        } else {
            $failures | ForEach-Object { Write-Host $_ -ForegroundColor Yellow }
        }
    }
    if ($latencyMatch.Success) {
        $p50 = $latencyMatch.Groups[1].Value
        $p95 = $latencyMatch.Groups[2].Value
        $p99 = $latencyMatch.Groups[3].Value
    }
    $loadOutput | Set-Content -Path $loadOutputPath
    $loadOutput | ForEach-Object { Write-Host $_ }
    if ($loadExitCode -ne 0) { $benchmarkFailed = $true }
} else {
    $loadType = "Go fallback"
    Write-Host "k6 was not found; using the built-in Go load generator."
    $generator = @'
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

type result struct {
	status int
	elapsed time.Duration
	detail string
}

func main() {
	baseURL := os.Getenv("BASE_URL")
	if baseURL == "" {
		baseURL = "http://localhost:8080"
	}
	baseURL = strings.TrimRight(baseURL, "/")
	const total, concurrency = 10000, 32
	transport := &http.Transport{
		MaxIdleConns: concurrency,
		MaxIdleConnsPerHost: concurrency,
		MaxConnsPerHost: concurrency,
		DialContext: (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
	}
	client := &http.Client{Transport: transport, Timeout: 15 * time.Second}
	results := make(chan result, total)
	jobs := make(chan int, concurrency)
	var workers sync.WaitGroup
	started := time.Now()
	for i := 0; i < concurrency; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for index := range jobs {
				tenant := "tenant-c"
				switch index % 10 {
				case 0, 1, 2, 3:
					tenant = "tenant-a"
				case 4, 5, 6:
					tenant = "tenant-b"
				}
				requestStarted := time.Now()
				body, err := json.Marshal(map[string]any{
					"tenant_id": tenant,
					"idempotency_key": fmt.Sprintf("bench-%d-%d", time.Now().UnixNano(), index),
					"command": "echo ok",
					"req_cpu": 1,
					"req_memory_mb": 64,
					"req_gpu": 0,
				})
				if err != nil {
					results <- result{elapsed: time.Since(requestStarted), detail: err.Error()}
					continue
				}
				request, err := http.NewRequest(http.MethodPost, baseURL+"/api/v1/jobs", bytes.NewReader(body))
				if err != nil {
					results <- result{elapsed: time.Since(requestStarted), detail: err.Error()}
					continue
				}
				request.Header.Set("Content-Type", "application/json")
				response, err := client.Do(request)
				if err != nil {
					results <- result{elapsed: time.Since(requestStarted), detail: err.Error()}
					continue
				}
				responseBody, copyErr := io.ReadAll(response.Body)
				closeErr := response.Body.Close()
				if copyErr != nil || closeErr != nil {
					results <- result{status: response.StatusCode, elapsed: time.Since(requestStarted), detail: "failed reading response body"}
					continue
				}
				detail := ""
				if response.StatusCode != http.StatusCreated {
					detail = strings.TrimSpace(string(responseBody))
					if len(detail) > 400 { detail = detail[:400] }
				}
				results <- result{status: response.StatusCode, elapsed: time.Since(requestStarted), detail: detail}
			}
		}()
	}
	for i := 0; i < total; i++ {
		jobs <- i
	}
	close(jobs)
	workers.Wait()
	close(results)
	elapsed := time.Since(started)
	latencies := make([]float64, 0, total)
	accepted, rejected := 0, 0
	failureSamples := make([]string, 0, 5)
	for item := range results {
		latencies = append(latencies, float64(item.elapsed.Microseconds())/1000)
		if item.status == http.StatusCreated {
			accepted++
		} else {
			rejected++
			if len(failureSamples) < cap(failureSamples) {
				failureSamples = append(failureSamples, fmt.Sprintf("status=%d body=%q", item.status, item.detail))
			}
		}
	}
	sort.Float64s(latencies)
	percentile := func(p float64) float64 {
		if len(latencies) == 0 { return 0 }
		index := int(float64(len(latencies)-1) * p)
		return latencies[index]
	}
	for _, sample := range failureSamples {
		fmt.Printf("job failure sample: %s\n", sample)
	}
	fmt.Printf("fallback submitted=%d accepted=%d rejected_or_transport_errors=%d elapsed_seconds=%.2f requests_per_second=%.2f accepted_per_second=%.2f p50_ms=%.2f p95_ms=%.2f p99_ms=%.2f\n",
		total, accepted, rejected, elapsed.Seconds(), float64(total)/elapsed.Seconds(),
		float64(accepted)/elapsed.Seconds(),
		percentile(.50), percentile(.95), percentile(.99))
	if rejected != 0 {
		os.Exit(2)
	}
}
'@
    [System.IO.File]::WriteAllText(
        $fallbackFile,
        $generator,
        (New-Object System.Text.UTF8Encoding($false))
    )
    $previousBaseUrl = $env:BASE_URL
    $previousErrorPreference = $ErrorActionPreference
    try {
        $env:BASE_URL = $BaseUrl
        $ErrorActionPreference = "Continue"
        $loadOutput = & go -C (Join-Path $root "worker-agent") run $fallbackFile 2>&1
        $loadExitCode = $LASTEXITCODE
        $loadSummary = @($loadOutput | Where-Object { $_ -match "fallback submitted=" }) -join " "
        $fallbackMatch = [regex]::Match(
            $loadSummary,
            "accepted_per_second=([\d.]+)\s+p50_ms=([\d.]+)\s+p95_ms=([\d.]+)\s+p99_ms=([\d.]+)"
        )
        if ($fallbackMatch.Success) {
            $throughput = $fallbackMatch.Groups[1].Value
            $p50 = $fallbackMatch.Groups[2].Value
            $p95 = $fallbackMatch.Groups[3].Value
            $p99 = $fallbackMatch.Groups[4].Value
        }
        $countsMatch = [regex]::Match($loadSummary, "fallback submitted=(\d+) accepted=(\d+)")
        if ($countsMatch.Success) {
            $submittedCount = [int]$countsMatch.Groups[1].Value
            $acceptedCount = [int]$countsMatch.Groups[2].Value
        }
        if ($loadExitCode -ne 0) {
            Write-Host "Go fallback failed; captured compiler/runtime output:" -ForegroundColor Red
            $loadOutput | ForEach-Object { Write-Host $_ -ForegroundColor Red }
        }
    } finally {
        $ErrorActionPreference = $previousErrorPreference
        if ($null -eq $previousBaseUrl) {
            Remove-Item Env:\BASE_URL -ErrorAction SilentlyContinue
        } else {
            $env:BASE_URL = $previousBaseUrl
        }
        if (Test-Path -LiteralPath $fallbackFile) {
            Remove-Item -LiteralPath $fallbackFile -Force
        }
    }
    $loadOutput | Set-Content -Path $loadOutputPath
    if ($loadExitCode -eq 0) {
        $loadOutput | ForEach-Object { Write-Host $_ }
    }
    if ($loadExitCode -ne 0) { $benchmarkFailed = $true }
}
} finally {
    if ($quotasRaised) {
        $restoreErrors = New-Object System.Collections.Generic.List[string]
        foreach ($quota in $quotaBackup) {
            try {
                $restoreQuery = "UPDATE tenants SET max_cpu=$($quota.CPU), max_ram_mb=$($quota.MemoryMB), max_gpu=$($quota.GPU), max_concurrent_jobs=$($quota.ConcurrentJobs) WHERE id='$($quota.ID)'"
                $null = Invoke-LocalMySqlQuery -Query $restoreQuery
            } catch {
                $restoreErrors.Add($_.Exception.Message)
            }
        }
        if ($restoreErrors.Count -gt 0) {
            throw "Failed to restore original tenant quotas: $($restoreErrors -join '; ')"
        }
        Write-Host "Restored original tenant quotas."
    }
}

Start-Sleep -Seconds 3
$postLoad = Save-ProfileSnapshot -Phase "post-load"
Write-Host ""
Write-Host "AtlasFarm benchmark summary ($loadType)"
$markdownSummary = @(
    "| Metric | Baseline | Post-load |"
    "| --- | ---: | ---: |"
    "| Throughput (jobs/sec) | n/a | $throughput |"
    "| p50 latency (ms) | n/a | $p50 |"
    "| p95 latency (ms) | n/a | $p95 |"
    "| p99 latency (ms) | n/a | $p99 |"
    "| Goroutines | $($baseline.Goroutines) | $($postLoad.Goroutines) |"
    "| Heap objects | $($baseline.HeapObjects) | $($postLoad.HeapObjects) |"
    "| Heap bytes | $($baseline.HeapBytes) | $($postLoad.HeapBytes) |"
)
$summaryPath = Join-Path $profiles ($runId + "-summary.md")
$markdownSummary | Set-Content -Path $summaryPath -Encoding UTF8
$markdownSummary | ForEach-Object { Write-Host $_ }
Write-Host "Goroutine leak check: compare the post-load count with baseline after the service settles."
Write-Host "Profiles, raw load output, and Markdown summary are under docs\profiles\."

if ($submittedCount -ne 10000 -or $acceptedCount -ne 10000) {
    $benchmarkFailed = $true
    Write-Host "Expected 10,000 accepted benchmark submissions; observed submitted=$submittedCount accepted=$acceptedCount." -ForegroundColor Yellow
}
if ($p99 -ne "n/a" -and [double]$p99 -ge $MaxP99Ms) {
    $benchmarkFailed = $true
    Write-Host "p99 latency threshold failed: ${p99}ms (configured limit <$MaxP99Ms ms)." -ForegroundColor Yellow
}

if (-not $benchmarkFailed) {
    $toolVersion = ""
    if ($k6) {
        $toolVersion = [string]((& $k6.Source version 2>&1 | Select-Object -First 1))
    } else {
        $toolVersion = [string]((& go version 2>&1 | Select-Object -First 1))
    }
    $environment = if ($env:COMPUTERNAME) { $env:COMPUTERNAME } else { "local" }
    $date = Get-Date -Format "yyyy-MM-dd"
    $baselineHeapMB = [math]::Round(([double]$baseline.HeapBytes / 1MB), 2)
    $postLoadHeapMB = [math]::Round(([double]$postLoad.HeapBytes / 1MB), 2)
    $row = "| $date / $environment | $loadType ($toolVersion) | $submittedCount / $acceptedCount | $throughput | $p50 | $p95 | $p99 | $($baseline.Goroutines) -> $($postLoad.Goroutines) | $baselineHeapMB MB -> $postLoadHeapMB MB | Not run by benchmark script | p99 <$MaxP99Ms ms; 100% accepted |"
    $benchmarkDoc = Join-Path $root "docs\benchmarks.md"
    $docText = [System.IO.File]::ReadAllText($benchmarkDoc)
    $placeholder = [regex]::Match($docText, "(?m)^\| Not measured \|[^\r\n]*")
    if ($placeholder.Success) {
        $docText = $docText.Substring(0, $placeholder.Index) + $row +
            $docText.Substring($placeholder.Index + $placeholder.Length)
    } else {
        $marker = "## Kind sizing and caveats"
        $markerIndex = $docText.IndexOf($marker, [System.StringComparison]::Ordinal)
        if ($markerIndex -lt 0) { throw "Could not locate benchmark results table insertion point in docs/benchmarks.md." }
        $docText = $docText.Insert($markerIndex, $row + [Environment]::NewLine + [Environment]::NewLine)
    }
    [System.IO.File]::WriteAllText($benchmarkDoc, $docText, (New-Object System.Text.UTF8Encoding($false)))
    Write-Host "Recorded successful benchmark results in docs\benchmarks.md."
}
if ($benchmarkFailed) {
    Write-Error "Benchmark load failed or its configured thresholds were not met."
    exit 1
}
