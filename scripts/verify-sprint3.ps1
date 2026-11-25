param(
    [string]$BaseUrl = "http://localhost:8080"
)

$ErrorActionPreference = "Stop"
$script:BaseUrl = $BaseUrl.TrimEnd("/")
$script:Failures = 0
$script:RunId = [Guid]::NewGuid().ToString("N").Substring(0, 8)
$script:WorkerIds = @(
    "0verify-$($script:RunId)-gpu",
    "0verify-$($script:RunId)-cpu"
)
$script:JobIds = @()
$script:GpuWorkerId = $script:WorkerIds[0]
$script:CpuWorkerId = $script:WorkerIds[1]

function Invoke-AtlasRequest {
    param(
        [string]$Method,
        [string]$Path,
        [object]$Body
    )

    $request = @{
        Uri = "$($script:BaseUrl)$Path"
        Method = $Method
        TimeoutSec = 10
        ErrorAction = "Stop"
    }
    if ($PSBoundParameters.ContainsKey("Body")) {
        $request.ContentType = "application/json"
        $request.Body = ConvertTo-Json -InputObject $Body -Depth 10 -Compress
    }
    Invoke-RestMethod @request
}

function Get-AtlasCollection {
    param([string]$Path)

    $response = Invoke-WebRequest -Uri "$($script:BaseUrl)$Path" -UseBasicParsing `
        -TimeoutSec 10 -ErrorAction Stop
    ConvertFrom-Json -InputObject $response.Content
}

function Get-HttpStatusCode {
    param([System.Management.Automation.ErrorRecord]$ErrorRecord)

    $response = $ErrorRecord.Exception.Response
    if ($null -eq $response) {
        return 0
    }
    return [int]$response.StatusCode
}

function Write-TestResult {
    param(
        [string]$Name,
        [bool]$Passed,
        [string]$Detail
    )

    if ($Passed) {
        Write-Host "[PASS] $Name - $Detail" -ForegroundColor Green
    }
    else {
        $script:Failures++
        Write-Host "[FAIL] $Name - $Detail" -ForegroundColor Red
    }
}

function Send-WorkerHeartbeats {
    foreach ($workerId in $script:WorkerIds) {
        Invoke-AtlasRequest -Method "Post" -Path "/api/v1/workers/$workerId/heartbeat" `
            -Body @{ active_jobs = @() } | Out-Null
    }
}

function New-TestJob {
    param(
        [string]$TenantId,
        [string]$Key,
        [int]$Cpu,
        [int]$MemoryMB,
        [int]$Gpu
    )

    $job = Invoke-AtlasRequest -Method "Post" -Path "/api/v1/jobs" -Body @{
        tenant_id = $TenantId
        idempotency_key = $Key
        command = "echo atlasfarm-verification"
        req_cpu = $Cpu
        req_memory_mb = $MemoryMB
        req_gpu = $Gpu
    }
    $script:JobIds += $job.id
    return $job
}

function Wait-ForScheduledJob {
    param(
        [string]$JobId,
        [int]$TimeoutSeconds = 12
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        Send-WorkerHeartbeats
        $job = Invoke-AtlasRequest -Method "Get" -Path "/api/v1/jobs/$JobId"
        if ($job.status -eq "scheduled") {
            return $job
        }
        Start-Sleep -Milliseconds 500
    } while ((Get-Date) -lt $deadline)
    throw "Job $JobId was not scheduled within $TimeoutSeconds seconds (last status: $($job.status))"
}

try {
    $passed = $false
    $detail = ""
    try {
        $health = Invoke-AtlasRequest -Method "Get" -Path "/healthz"
        if ([string]$health.Trim() -ne "ok") {
            throw "Unexpected health response: $health"
        }
        $existingWorkers = Get-AtlasCollection -Path "/api/v1/workers"
        foreach ($worker in $existingWorkers) {
            if ($worker.id -match "^(0?verify)-[0-9a-f]{8}-(gpu|cpu)$") {
                Invoke-AtlasRequest -Method "Post" -Path "/api/v1/workers" -Body @{
                    id = $worker.id
                    cpu = 1
                    memory_mb = 128
                    gpu = 0
                    runner_version = "verification"
                } | Out-Null
            }
        }
        Invoke-AtlasRequest -Method "Post" -Path "/api/v1/workers" -Body @{
            id = $script:GpuWorkerId
            cpu = 1
            memory_mb = 256
            gpu = 2
            runner_version = "verification"
        } | Out-Null
        Invoke-AtlasRequest -Method "Post" -Path "/api/v1/workers" -Body @{
            id = $script:CpuWorkerId
            cpu = 90
            memory_mb = 131072
            gpu = 0
            runner_version = "verification"
        } | Out-Null
        $workers = @(Invoke-AtlasRequest -Method "Get" -Path "/api/v1/workers")
        if ($workers.id -notcontains $script:GpuWorkerId -or
            $workers.id -notcontains $script:CpuWorkerId) {
            throw "Both verification workers were not present in the worker list"
        }
        $passed = $true
        $detail = "health endpoint and both worker registrations verified"
    }
    catch {
        $detail = $_.Exception.Message
    }
    Write-TestResult -Name "Control-plane health and multi-worker registration" -Passed $passed -Detail $detail

    $passed = $false
    $detail = ""
    try {
        if (-not $script:WorkerIds) {
            throw "Worker registration prerequisite failed"
        }
        $gpuJob = New-TestJob -TenantId "tenant-a" -Key "verify-$($script:RunId)-gpu" `
            -Cpu 1 -MemoryMB 256 -Gpu 2
        $gpuJob = Wait-ForScheduledJob -JobId $gpuJob.id
        if ($gpuJob.worker_id -ne $script:GpuWorkerId) {
            throw "Expected GPU worker $($script:GpuWorkerId), got $($gpuJob.worker_id)"
        }
        $script:GpuJobId = $gpuJob.id
        $passed = $true
        $detail = "2-GPU job was assigned only to $($gpuJob.worker_id)"
    }
    catch {
        $detail = $_.Exception.Message
    }
    Write-TestResult -Name "GPU-aware placement" -Passed $passed -Detail $detail

    $passed = $false
    $detail = ""
    try {
        $key = "verify-$($script:RunId)-idempotent"
        $first = New-TestJob -TenantId "tenant-a" -Key $key -Cpu 5 -MemoryMB 128 -Gpu 0
        $second = Invoke-AtlasRequest -Method "Post" -Path "/api/v1/jobs" -Body @{
            tenant_id = "tenant-a"
            idempotency_key = $key
            command = "echo atlasfarm-verification"
            req_cpu = 5
            req_memory_mb = 128
            req_gpu = 0
        }
        if ($first.id -ne $second.id) {
            throw "Duplicate idempotency key created a different job ID"
        }
        $job = Wait-ForScheduledJob -JobId $first.id
        $detailJob = Invoke-AtlasRequest -Method "Get" -Path "/api/v1/jobs/$($first.id)"
        $createdEvents = @($detailJob.events | Where-Object { $_.event_type -eq "JOB_CREATED" })
        if ($createdEvents.Count -ne 1) {
            throw "Expected exactly one JOB_CREATED event; found $($createdEvents.Count)"
        }
        $sequence = @($detailJob.events | ForEach-Object { [int]$_.sequence_num })
        for ($i = 1; $i -lt $sequence.Count; $i++) {
            if ($sequence[$i] -le $sequence[$i - 1]) {
                throw "Event sequence numbers are not strictly increasing"
            }
        }
        $script:IdempotentJobId = $first.id
        $script:IdempotentWorkerId = $job.worker_id
        $script:IdempotentExecutionId = [long]$job.execution_id
        $passed = $true
        $detail = "duplicate returned the same job; one JOB_CREATED event and ordered audit trail verified"
    }
    catch {
        $detail = $_.Exception.Message
    }
    Write-TestResult -Name "Event sourcing audit trail and idempotency" -Passed $passed -Detail $detail

    $passed = $false
    $detail = ""
    try {
        $quotaKey = "verify-$($script:RunId)-quota"
        try {
            New-TestJob -TenantId "tenant-a" -Key $quotaKey -Cpu 1 -MemoryMB 128 -Gpu 7 | Out-Null
            throw "Quota-breaching job was unexpectedly accepted"
        }
        catch {
            if ((Get-HttpStatusCode -ErrorRecord $_) -ne 429) {
                throw
            }
        }
        $passed = $true
        $detail = "GPU quota breach rejected with HTTP 429"
    }
    catch {
        $detail = $_.Exception.Message
    }
    Write-TestResult -Name "Tenant quota enforcement" -Passed $passed -Detail $detail

    $passed = $false
    $detail = ""
    try {
        if (-not $script:IdempotentJobId) {
            throw "Idempotency job prerequisite failed"
        }
        $claim = Invoke-AtlasRequest -Method "Post" `
            -Path "/api/v1/jobs/$($script:IdempotentJobId)/status" -Body @{
                status = "running"
                worker_id = $script:IdempotentWorkerId
                execution_id = $script:IdempotentExecutionId
            }
        if (-not $claim.claimed) {
            throw "Worker did not successfully claim the scheduled job"
        }
        $rejected = $false
        try {
            Invoke-AtlasRequest -Method "Post" `
                -Path "/api/v1/jobs/$($script:IdempotentJobId)/status" -Body @{
                    status = "succeeded"
                    worker_id = $script:IdempotentWorkerId
                    execution_id = $script:IdempotentExecutionId
                    result = "stale verification result"
                } | Out-Null
        }
        catch {
            if ((Get-HttpStatusCode -ErrorRecord $_) -eq 409) {
                $rejected = $true
            }
            else {
                throw
            }
        }
        if (-not $rejected) {
            throw "Stale execution token was not rejected with HTTP 409"
        }
        Invoke-AtlasRequest -Method "Post" `
            -Path "/api/v1/jobs/$($script:IdempotentJobId)/status" -Body @{
                status = "succeeded"
                worker_id = $script:IdempotentWorkerId
                execution_id = [long]$claim.execution_id
                result = "verification completed"
            } | Out-Null
        $passed = $true
        $detail = "stale execution token rejected with HTTP 409"
    }
    catch {
        $detail = $_.Exception.Message
    }
    Write-TestResult -Name "Stale execution fencing token" -Passed $passed -Detail $detail

    $passed = $false
    $detail = ""
    try {
        $fairPrefix = "verify-$($script:RunId)-fair-"
        $expected = 7
        Invoke-AtlasRequest -Method "Post" -Path "/api/v1/workers" -Body @{
            id = $script:CpuWorkerId
            cpu = 1
            memory_mb = 131072
            gpu = 0
            runner_version = "verification"
        } | Out-Null
        for ($i = 0; $i -lt $expected; $i++) {
            New-TestJob -TenantId "tenant-a" -Key ($fairPrefix + "a-$i") `
                -Cpu 9 -MemoryMB 128 -Gpu 0 | Out-Null
            New-TestJob -TenantId "tenant-b" -Key ($fairPrefix + "b-$i") `
                -Cpu 9 -MemoryMB 128 -Gpu 0 | Out-Null
            if (($i % 5) -eq 0) {
                Send-WorkerHeartbeats
            }
        }
        Invoke-AtlasRequest -Method "Post" -Path "/api/v1/workers" -Body @{
            id = $script:CpuWorkerId
            cpu = 90
            memory_mb = 131072
            gpu = 0
            runner_version = "verification"
        } | Out-Null
        Send-WorkerHeartbeats

        $deadline = (Get-Date).AddSeconds(12)
        $fairJobs = @()
        do {
            Send-WorkerHeartbeats
            $allJobs = Get-AtlasCollection -Path "/api/v1/jobs"
            $fairJobs = @($allJobs | Where-Object { $_.idempotency_key -like "$fairPrefix*" })
            $scheduled = @($fairJobs | Where-Object {
                -not [string]::IsNullOrWhiteSpace([string]$_.worker_id)
            })
            if ($scheduled.Count -ge 10) {
                break
            }
            Start-Sleep -Milliseconds 500
        } while ((Get-Date) -lt $deadline)

        $aCount = @($scheduled | Where-Object { $_.idempotency_key -like ($fairPrefix + "a-*") }).Count
        $bCount = @($scheduled | Where-Object { $_.idempotency_key -like ($fairPrefix + "b-*") }).Count
        if ($bCount -lt 3 -or $aCount -le $bCount -or $aCount -lt 5) {
            throw "Fair scheduling distribution was A=$aCount, B=$bCount; expected both served with A weighted above B"
        }
        $passed = $true
        $detail = "dispatched distribution: tenant-a=$aCount, tenant-b=$bCount (weights 40:30)"
    }
    catch {
        $detail = $_.Exception.Message
    }
    Write-TestResult -Name "Weighted fair scheduling" -Passed $passed -Detail $detail
}
finally {
    foreach ($jobId in $script:JobIds) {
        try {
            $job = Invoke-AtlasRequest -Method "Get" -Path "/api/v1/jobs/$jobId"
            if ($job.status -notin @("succeeded", "failed", "cancelled")) {
                Invoke-AtlasRequest -Method "Delete" -Path "/api/v1/jobs/$jobId" | Out-Null
            }
        }
        catch {
            Write-Host "[WARN] Cleanup for job $jobId failed: $($_.Exception.Message)" -ForegroundColor Yellow
        }
    }
}

if ($script:Failures -gt 0) {
    Write-Host "Sprint 1-3 verification failed: $($script:Failures) test(s) failed." -ForegroundColor Red
    exit 1
}

Write-Host "All Sprint 1-3 verification checks passed." -ForegroundColor Green
