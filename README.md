# NATS Consumer Deadlock Reproduction

Start the nats server externally to the program. Fewer goroutines makes debug inspection easier.

```shell
docker compose up -d
```

Run the program and observe the timeout breakpoint log after 5 seconds.

```shell
go run .
```

The program deadlocks when either `MessagesContext.Drain()` or `MessagesContext.Stop()` are called.
[nats_consumer.go](nats_consumer.go) line 13-14 have flags to call Drain and Stop. When they are
both disabled there is no deadlock, but these logically must be called to clean up the subscription.
