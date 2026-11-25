param(
    [Parameter(Mandatory = $true)]
    [string]$ContainerName
)

$ErrorActionPreference = "Stop"
docker kill $ContainerName
if ($LASTEXITCODE -ne 0) {
    throw "Failed to stop worker container '$ContainerName'."
}
