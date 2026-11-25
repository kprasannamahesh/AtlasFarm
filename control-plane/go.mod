module github.com/kprasannamahesh/AtlasFarm/control-plane

go 1.22

require (
	github.com/DATA-DOG/go-sqlmock v1.5.2
	github.com/go-sql-driver/mysql v1.8.1
	github.com/kprasannamahesh/AtlasFarm/scheduler v0.0.0
	github.com/rabbitmq/amqp091-go v1.17.0
)

require filippo.io/edwards25519 v1.1.0 // indirect

replace github.com/kprasannamahesh/AtlasFarm/scheduler => ../scheduler
