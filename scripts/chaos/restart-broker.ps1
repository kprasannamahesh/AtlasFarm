$ErrorActionPreference = "Stop"
docker compose -f deploy/docker-compose.yml restart rabbitmq
if ($LASTEXITCODE -ne 0) {
    throw "Failed to restart RabbitMQ."
}
