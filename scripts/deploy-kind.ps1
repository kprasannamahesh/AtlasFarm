param(
    [switch]$Recreate
)

$ErrorActionPreference = "Stop"
$root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$k8s = Join-Path $root "deploy\k8s"
$composeInitSQL = Join-Path $root "deploy\init.sql"
$configMapFile = Join-Path $env:TEMP ("atlasfarm-kind-init-" + [Guid]::NewGuid().ToString("N") + ".yaml")
$secretFile = Join-Path $env:TEMP ("atlasfarm-kind-secret-" + [Guid]::NewGuid().ToString("N") + ".yaml")

foreach ($tool in @("docker", "kind", "kubectl", "go")) {
    if (-not (Get-Command $tool -ErrorAction SilentlyContinue)) {
        throw "Required command '$tool' was not found on PATH."
    }
}

if ($Recreate -and (& kind get clusters) -contains "atlasfarm") {
    & kind delete cluster --name atlasfarm
    if ($LASTEXITCODE -ne 0) { throw "Unable to delete the existing Kind cluster." }
}

if ((& kind get clusters) -notcontains "atlasfarm") {
    & kind create cluster --name atlasfarm
    if ($LASTEXITCODE -ne 0) { throw "Unable to create the Kind cluster." }
}

& kubectl config use-context kind-atlasfarm
if ($LASTEXITCODE -ne 0) { throw "Unable to select Kind context kind-atlasfarm." }

# The Kind GPU pool is a scheduling demo only; Kind does not provide GPU devices.
& kubectl label node atlasfarm-control-plane atlasfarm.io/pool=cpu atlasfarm.io/gpu=true --overwrite
if ($LASTEXITCODE -ne 0) { throw "Unable to label the Kind node for the demo worker pools." }

& kubectl apply -f (Join-Path $k8s "00-namespace.yaml")
if ($LASTEXITCODE -ne 0) { throw "Unable to apply the AtlasFarm namespace." }

$initFileArgument = "001-init.sql=" + $composeInitSQL
try {
    $configMapManifest = & kubectl create configmap atlasfarm-mysql-init --from-file $initFileArgument `
        --namespace atlasfarm --dry-run=client -o yaml
    if ($LASTEXITCODE -ne 0) { throw "Unable to generate the MySQL initialization ConfigMap." }
    [System.IO.File]::WriteAllText(
        $configMapFile,
        [string]::Join([Environment]::NewLine, @($configMapManifest)),
        (New-Object System.Text.UTF8Encoding($false))
    )
    & kubectl apply -f $configMapFile
    if ($LASTEXITCODE -ne 0) { throw "Unable to apply the MySQL initialization ConfigMap." }
} finally {
    if (Test-Path -LiteralPath $configMapFile) {
        Remove-Item -LiteralPath $configMapFile -Force
    }
}

$existingPassword = & kubectl get secret atlasfarm-credentials --namespace atlasfarm `
    --ignore-not-found -o "jsonpath={.data.MYSQL_ROOT_PASSWORD}"
if ($LASTEXITCODE -ne 0) { throw "Unable to check for existing AtlasFarm credentials." }

if (-not [string]::IsNullOrWhiteSpace($existingPassword)) {
    try {
        $password = [System.Text.Encoding]::UTF8.GetString(
            [Convert]::FromBase64String($existingPassword)
        )
    } catch {
        throw "The existing MySQL password in atlasfarm-credentials could not be decoded."
    }
} else {
    $pvcPhase = & kubectl get pvc atlasfarm-mysql-data --namespace atlasfarm `
        --ignore-not-found -o "jsonpath={.status.phase}"
    if ($LASTEXITCODE -ne 0) { throw "Unable to check for an existing MySQL volume." }
    if ($pvcPhase -eq "Bound") {
        throw "The MySQL volume exists but its credentials Secret is missing. Restore the original Secret or recreate this local Kind cluster."
    }
    $password = [Guid]::NewGuid().ToString("N") + [Guid]::NewGuid().ToString("N")
}

$configSecret = Get-Content (Join-Path $k8s "01-config-secret.yaml") -Raw
$configSecret = $configSecret.Replace("CHANGE_ME", $password)
[System.IO.File]::WriteAllText(
    $secretFile,
    $configSecret,
    (New-Object System.Text.UTF8Encoding($false))
)
try {
    & kubectl apply -f $secretFile
    if ($LASTEXITCODE -ne 0) { throw "Unable to apply AtlasFarm configuration and credentials." }

    & kubectl apply -f (Join-Path $k8s "02-infra.yaml")
    if ($LASTEXITCODE -ne 0) { throw "Unable to apply infrastructure manifests." }
    foreach ($deployment in @("atlasfarm-mysql", "atlasfarm-rabbitmq", "atlasfarm-prometheus", "atlasfarm-jaeger")) {
        & kubectl rollout status ("deployment/" + $deployment) --namespace atlasfarm --timeout=300s
        if ($LASTEXITCODE -ne 0) { throw "Infrastructure deployment '$deployment' did not become ready." }
    }

    & docker build -f (Join-Path $root "control-plane\Dockerfile") `
        -t atlasfarm/control-plane:local $root
    if ($LASTEXITCODE -ne 0) { throw "Unable to build the control-plane image." }
    & docker build -f (Join-Path $root "worker-agent\Dockerfile") `
        -t atlasfarm/worker-agent:local $root
    if ($LASTEXITCODE -ne 0) { throw "Unable to build the worker-agent image." }

    & kind load docker-image atlasfarm/control-plane:local --name atlasfarm
    if ($LASTEXITCODE -ne 0) { throw "Unable to load the control-plane image into Kind." }
    & kind load docker-image atlasfarm/worker-agent:local --name atlasfarm
    if ($LASTEXITCODE -ne 0) { throw "Unable to load the worker-agent image into Kind." }

    & kubectl apply -f (Join-Path $k8s "03-control-plane.yaml")
    if ($LASTEXITCODE -ne 0) { throw "Unable to apply the control-plane manifests." }
    & kubectl apply -f (Join-Path $k8s "04-worker-pool.yaml")
    if ($LASTEXITCODE -ne 0) { throw "Unable to apply the worker-pool manifests." }

    & kubectl wait --for=condition=Ready pods --all --namespace atlasfarm --timeout=300s
    if ($LASTEXITCODE -ne 0) { throw "Not all AtlasFarm pods became Ready." }

    Write-Host "AtlasFarm is ready in Kind cluster 'atlasfarm'." -ForegroundColor Green
    Write-Host "Use kubectl port-forward -n atlasfarm svc/atlasfarm-control-plane 8080:8080"
    Write-Host "Use kubectl port-forward -n atlasfarm svc/atlasfarm-jaeger 16686:16686"
    Write-Host "This script generated local demo credentials in the cluster; do not use them for production."
}
finally {
    if (Test-Path -LiteralPath $secretFile) {
        Remove-Item -LiteralPath $secretFile -Force
    }
}
