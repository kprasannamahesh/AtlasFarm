param(
    [string]$BaseUrl = "http://localhost:8080"
)

$ErrorActionPreference = "Stop"
$root = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$composeFile = Join-Path $root "deploy\docker-compose.yml"
$workerBinary = Join-Path $env:TEMP ("atlasfarm-chaos-worker-" + [Guid]::NewGuid().ToString("N") + ".exe")
$logDirectory = Join-Path $env:TEMP ("atlasfarm-chaos-" + [Guid]::NewGuid().ToString("N"))
$script:workers = New-Object System.Collections.Generic.List[object]
$script:results = New-Object System.Collections.Generic.List[object]
$api = $BaseUrl.TrimEnd("/") + "/api/v1"

function Write-ScenarioResult {
    param([string]$Name, [bool]$Passed, [string]$Detail)

    $label = if ($Passed) { "[PASS]" } else { "[FAIL]" }
    $color = if ($Passed) { "Green" } else { "Red" }
    Write-Host ("{0} {1}: {2}" -f $label, $Name, $Detail) -ForegroundColor $color
    $script:results.Add([PSCustomObject]@{ Scenario = $Name; Passed = $Passed; Detail = $Detail })
}

function Invoke-Scenario {
    param([string]$Name, [scriptblock]$Action)

    try {
        $detail = & $Action
        Write-ScenarioResult -Name $Name -Passed $true -Detail ([string]$detail)
    } catch {
        Write-ScenarioResult -Name $Name -Passed $false -Detail $_.Exception.Message
    }
}

function Get-ApiStatusCode {
    param([string]$Method, [string]$Path, [object]$Body)

    try {
        $request = @{
            Uri = ($api + $Path)
            Method = $Method
            TimeoutSec = 10
        }
        if ($null -ne $Body) {
            $request.ContentType = "application/json"
            $request.Body = $Body | ConvertTo-Json -Depth 10 -Compress
        }
        $null = Invoke-WebRequest -UseBasicParsing @request
        return 200
    } catch {
        $response = $_.Exception.Response
        if ($null -ne $response -and $null -ne $response.StatusCode) {
            return [int]$response.StatusCode
        }
        if ($null -ne $_.Exception.StatusCode) {
            return [int]$_.Exception.StatusCode
        }
        throw
    }
}

function Get-JobDetail {
    param([string]$Id)
    return Invoke-RestMethod -Uri ($api + "/jobs/" + $Id) -TimeoutSec 10
}

function New-TestJob {
    param(
        [string]$Name,
        [string]$Command,
        [int]$CPU = 1,
        [int]$MemoryMB = 256,
        [int]$GPU = 0
    )

    $payload = @{
        tenant_id = "tenant-a"
        idempotency_key = ("chaos-" + $Name + "-" + [Guid]::NewGuid().ToString("N"))
        command = $Command
        req_cpu = $CPU
        req_memory_mb = $MemoryMB
        req_gpu = $GPU
    }
    return Invoke-RestMethod -Uri ($api + "/jobs") -Method Post `
        -ContentType "application/json" -Body ($payload | ConvertTo-Json -Compress) -TimeoutSec 15
}

function Wait-ForTerminalJob {
    param([string]$Id, [int]$TimeoutSeconds = 120)

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        $job = Get-JobDetail -Id $Id
        if ($job.status -in @("succeeded", "failed", "cancelled")) {
            return $job
        }
        Start-Sleep -Seconds 1
    } while ((Get-Date) -lt $deadline)
    throw "Job $Id did not reach a terminal state within $TimeoutSeconds seconds."
}

function Wait-ForWorker {
    param([string]$Id, [int]$TimeoutSeconds = 45)

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        $workers = @(Invoke-RestMethod -Uri ($api + "/workers?active_only=true") -TimeoutSec 10)
        if (@($workers | Where-Object { $_.id -eq $Id }).Count -gt 0) {
            return
        }
        Start-Sleep -Milliseconds 500
    } while ((Get-Date) -lt $deadline)
    throw "Worker $Id did not register and heartbeat in time."
}

function Start-TestWorker {
    param(
        [string]$Id,
        [int]$CPU,
        [int]$MemoryMB,
        [int]$GPU = 0
    )

    $stdout = Join-Path $logDirectory ($Id + ".stdout.log")
    $stderr = Join-Path $logDirectory ($Id + ".stderr.log")
    $process = Start-Process -FilePath $workerBinary -WorkingDirectory $root -PassThru `
        -ArgumentList @(
            "--id=$Id",
            "--cpu=$CPU",
            "--memory=$MemoryMB",
            "--gpu=$GPU",
            "--version=chaos-test",
            "--control-plane=$BaseUrl",
            "--metrics-addr=127.0.0.1:0"
        ) -RedirectStandardOutput $stdout -RedirectStandardError $stderr
    $script:workers.Add($process)
    Wait-ForWorker -Id $Id
    return $process
}

function Wait-ForReady {
    param([int]$TimeoutSeconds = 120)

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    $lastError = "readiness returned a non-200 response"
    do {
        try {
            $response = Invoke-WebRequest -UseBasicParsing `
                -Uri ($BaseUrl.TrimEnd("/") + "/readyz") -TimeoutSec 5
            if ([int]$response.StatusCode -eq 200) { return }
        } catch {
            $lastError = $_.Exception.Message
        }
        Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)
    throw "Control-plane readiness did not recover within $TimeoutSeconds seconds. Last result: $lastError"
}

function Restart-ComposeContainer {
    param(
        [string]$Service,
        [string[]]$ContainerCandidates
    )

    $previousPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $composeOutput = & docker compose -f $composeFile ps -q $Service 2>&1
        $composeExitCode = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previousPreference
    }

    $container = @($composeOutput | ForEach-Object {
        ([string]$_).Trim()
    } | Where-Object { $_ -match "^[a-fA-F0-9]{12,64}$" } | Select-Object -First 1)
    if ($container.Count -eq 0) {
        foreach ($candidate in $ContainerCandidates) {
            try {
                $previousPreference = $ErrorActionPreference
                $ErrorActionPreference = "Continue"
                $inspectOutput = & docker inspect --format "{{.Id}}" $candidate 2>&1
                $inspectExitCode = $LASTEXITCODE
            } finally {
                $ErrorActionPreference = $previousPreference
            }
            if ($inspectExitCode -eq 0 -and $inspectOutput) {
                $container = @($candidate)
                break
            }
        }
    }
    if ($container.Count -eq 0) {
        throw "Could not resolve the Compose '$Service' container. docker compose exit code: $composeExitCode; output: $($composeOutput -join ' ')"
    }

    $containerName = $container[0]
    try {
        $previousPreference = $ErrorActionPreference
        $ErrorActionPreference = "Continue"
        $restartOutput = & docker restart $containerName 2>&1
        $restartExitCode = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previousPreference
    }
    if ($restartExitCode -ne 0) {
        throw "docker restart $containerName failed with exit code ${restartExitCode}: $($restartOutput -join ' ')"
    }

    $deadline = (Get-Date).AddSeconds(30)
    $lastHealth = "unknown"
    do {
        try {
            $previousPreference = $ErrorActionPreference
            $ErrorActionPreference = "Continue"
            $healthOutput = & docker inspect --format "{{.State.Health.Status}}" $containerName 2>&1
            $healthExitCode = $LASTEXITCODE
        } finally {
            $ErrorActionPreference = $previousPreference
        }
        if ($healthExitCode -eq 0 -and $healthOutput) {
            $lastHealth = ([string]($healthOutput | Select-Object -First 1)).Trim()
            if ($lastHealth -eq "healthy") {
                return $containerName
            }
        } else {
            $lastHealth = "inspect failed: $($healthOutput -join ' ')"
        }
        Start-Sleep -Seconds 1
    } while ((Get-Date) -lt $deadline)
    throw "Container $containerName did not become healthy within 30 seconds; last health status: $lastHealth"
}

try {
    New-Item -ItemType Directory -Path $logDirectory -Force | Out-Null
    $docker = Get-Command "docker" -ErrorAction SilentlyContinue
    $go = Get-Command "go" -ErrorAction SilentlyContinue
    if (-not $docker) { throw "Required command 'docker' was not found on PATH." }
    if (-not $go) { throw "Required command 'go' was not found on PATH." }

    & go -C (Join-Path $root "worker-agent") build -o $workerBinary ./cmd/worker
    if ($LASTEXITCODE -ne 0) { throw "Unable to build the temporary chaos worker binary." }

    $stableID = "chaos-base-" + [Guid]::NewGuid().ToString("N").Substring(0, 8)
    Start-TestWorker -Id $stableID -CPU 4 -MemoryMB 4096 | Out-Null

    Invoke-Scenario -Name "Worker crash, lease retry, and recovery" -Action {
        $rolloutStatus = Get-ApiStatusCode -Method "GET" -Path "/rollouts/current" -Body $null
        if ($rolloutStatus -eq 200) {
            $rollout = Invoke-RestMethod -Uri ($api + "/rollouts/current") -TimeoutSec 10
            if ($rollout.status -eq "ACTIVE") {
                throw "An active canary rollout is present; finish or roll it back before this worker-placement test."
            }
        }

        $token = [Guid]::NewGuid().ToString("N").Substring(0, 8)
        $crashID = "00-crash-" + $token
        $survivorID = "chaos-surv-" + $token
        $crashProcess = Start-TestWorker -Id $crashID -CPU 4 -MemoryMB 2048
        Start-TestWorker -Id $survivorID -CPU 8 -MemoryMB 4096 | Out-Null

        $job = New-TestJob -Name "worker-crash" `
            -Command "powershell.exe -NoProfile -Command `"Start-Sleep -Seconds 10; Write-Output 'chaos-crash'`"" `
            -CPU 4 -MemoryMB 2048
        $deadline = (Get-Date).AddSeconds(30)
        do {
            $detail = Get-JobDetail -Id $job.id
            if ($detail.status -eq "running") { break }
            if ($detail.worker_id -and $detail.worker_id -ne $crashID) {
                throw "Job was assigned to $($detail.worker_id), not the temporary crash worker $crashID."
            }
            Start-Sleep -Milliseconds 250
        } while ((Get-Date) -lt $deadline)
        if ($detail.status -ne "running" -or $detail.worker_id -ne $crashID) {
            throw "Job did not start on the temporary worker within 30 seconds."
        }
        Stop-Process -Id $crashProcess.Id -Force

        $finished = Wait-ForTerminalJob -Id $job.id -TimeoutSeconds 90
        $retried = @($finished.events | Where-Object { $_.event_type -eq "JOB_RETRIED" }).Count -gt 0
        if (-not $retried) { throw "Job completed without a JOB_RETRIED event after worker termination." }
        if ($finished.status -ne "succeeded") { throw "Recovered job finished with status '$($finished.status)'." }
        return "job $($job.id) retried and completed on worker $($finished.worker_id)"
    }

    Invoke-Scenario -Name "RabbitMQ restart and reconnect" -Action {
        $jobs = New-Object System.Collections.Generic.List[object]
        for ($index = 0; $index -lt 6; $index++) {
            $jobs.Add((New-TestJob -Name "broker-$index" -Command "echo chaos-broker"))
        }

        $container = Restart-ComposeContainer -Service "rabbitmq" `
            -ContainerCandidates @("deploy-rabbitmq-1", "atlasfarm-rabbitmq")
        Wait-ForReady -TimeoutSeconds 120
        $verificationJob = New-TestJob -Name "broker-after-restart" -Command "echo broker-reconnected"
        $jobs.Add($verificationJob)
        foreach ($job in $jobs) {
            $result = Wait-ForTerminalJob -Id $job.id -TimeoutSeconds 120
            if ($result.status -ne "succeeded") {
                throw "Job $($job.id) finished as '$($result.status)' during broker recovery."
            }
        }
        return "$($jobs.Count - 1) queued jobs and a post-restart job completed after reconnect (container $container)"
    }

    Invoke-Scenario -Name "MySQL restart and control-plane recovery" -Action {
        $container = Restart-ComposeContainer -Service "mysql" `
            -ContainerCandidates @("deploy-mysql-1", "atlasfarm-mysql")
        Wait-ForReady -TimeoutSeconds 120
        $health = Invoke-WebRequest -UseBasicParsing `
            -Uri ($BaseUrl.TrimEnd("/") + "/healthz") -TimeoutSec 5
        if ([int]$health.StatusCode -ne 200) {
            throw "Control-plane liveness returned HTTP $($health.StatusCode) after MySQL restart."
        }
        $job = New-TestJob -Name "mysql-recovery" -Command "echo chaos-mysql"
        $result = Wait-ForTerminalJob -Id $job.id -TimeoutSeconds 120
        if ($result.status -ne "succeeded") {
            throw "Post-restart job $($job.id) finished as '$($result.status)'."
        }
        return "MySQL container $container recovered; liveness remained available and job $($job.id) completed"
    }

    Invoke-Scenario -Name "Stale execution fencing returns HTTP 409" -Action {
        $job = New-TestJob -Name "stale-fencing" -Command "echo chaos-fencing"
        $finished = Wait-ForTerminalJob -Id $job.id -TimeoutSeconds 120
        if ($finished.status -ne "succeeded") {
            throw "Fencing probe job finished as '$($finished.status)'."
        }
        $staleToken = [Math]::Max(1, [int64]$finished.execution_id + 1)
        $body = @{
            status = "failed"
            worker_id = "chaos-stale-worker"
            execution_id = $staleToken
            error = "intentional stale-token probe"
        }
        $status = Get-ApiStatusCode -Method "POST" `
            -Path ("/jobs/" + $job.id + "/status") -Body $body
        if ($status -ne 409) { throw "Stale callback returned HTTP $status, expected HTTP 409." }
        return "outdated execution token was rejected with HTTP 409"
    }
} catch {
    Write-Host ("[FAIL] Chaos suite setup: {0}" -f $_.Exception.Message) -ForegroundColor Red
    $script:results.Add([PSCustomObject]@{
        Scenario = "Chaos suite setup"
        Passed = $false
        Detail = $_.Exception.Message
    })
} finally {
    foreach ($worker in $script:workers) {
        try {
            $worker.Refresh()
            if (-not $worker.HasExited) {
                Stop-Process -Id $worker.Id -Force -ErrorAction SilentlyContinue
            }
            if (-not $worker.WaitForExit(3000)) {
                Stop-Process -Id $worker.Id -Force -ErrorAction SilentlyContinue
                if (-not $worker.WaitForExit(3000)) {
                    Write-Warning "Temporary worker process $($worker.Id) did not exit after cleanup."
                }
            }
        } catch {
            Write-Warning ("Unable to clean up temporary worker process {0}: {1}" -f `
                $worker.Id, $_.Exception.Message)
        }
    }
    if (Test-Path -LiteralPath $workerBinary) {
        for ($attempt = 1; $attempt -le 5 -and (Test-Path -LiteralPath $workerBinary); $attempt++) {
            Remove-Item -LiteralPath $workerBinary -Force -ErrorAction SilentlyContinue
            if (Test-Path -LiteralPath $workerBinary) {
                Start-Sleep -Milliseconds 300
            }
        }
        if (Test-Path -LiteralPath $workerBinary) {
            Write-Warning "Unable to remove temporary worker binary: $workerBinary"
        }
    }
}

Write-Host ""
$failedScenarios = @($script:results | Where-Object { -not $_.Passed }).Count
if ($failedScenarios -eq 0 -and (Test-Path -LiteralPath $logDirectory)) {
    Get-ChildItem -LiteralPath $logDirectory -File | Remove-Item -Force
    Remove-Item -LiteralPath $logDirectory
} elseif (Test-Path -LiteralPath $logDirectory) {
    Write-Host "Temporary worker logs retained at $logDirectory"
}
Write-Host ("Chaos suite: {0} passed, {1} failed" -f `
    @($script:results | Where-Object { $_.Passed }).Count,
    $failedScenarios)
$script:results | Format-Table -AutoSize | Out-Host
if ($failedScenarios -gt 0) {
    exit 1
}
