# jobworker ![](https://github.com/go-jwdk/jobworker/workflows/Go/badge.svg)

## Description

Package jobworker provides a generic interface around message queue.

The jobworker package must be used in conjunction with some message queue connector.

list of connectors:

- [__go-jwdk/aws-sqs-connector/sqs__](https://github.com/go-jwdk/aws-sqs-connector/)
- [__go-jwdk/activemq-connector/activemq__](https://github.com/go-jwdk/activemq-connector/)
- [__go-jwdk/db-connector/mysql__](https://github.com/go-jwdk/db-connector/)
- [__go-jwdk/db-connector/postgres__](https://github.com/go-jwdk/db-connector/)
- [__go-jwdk/db-connector/sqlite3__](https://github.com/go-jwdk/db-connector/)

## Requirements

Go 1.13+

## Installation

This package can be installed with the go get command:

```
$ go get -u github.com/go-jwdk/jobworker
```

## Usage

### Basically

Implements worker processes using go-jwdk/awa-sqs-connector.

```go
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/go-jwdk/aws-sqs-connector"
	jw "github.com/go-jwdk/jobworker"
)

func main() {
	sqs, err := jw.Open("sqs", map[string]interface{}{
		"Region":          os.Getenv("REGION"),
		"NumMaxRetries":   3,
	})
	if err != nil {
		log.Println("Could not open a sqs conn", err)
		return
	}
	sqs.SetLoggerFunc(log.Println)
	worker, err := jw.New(&jw.Setting{
		Primary:    sqs,
		LoggerFunc: log.Println,
	})
	if err != nil {
		log.Println("Could not create a job worker", err)
		return
	}
	worker.Register("test", &HelloWorker{},
		jw.SubscribeMetadata("PollingInterval", "3"),
		jw.SubscribeMetadata("VisibilityTimeout", "20"),
		jw.SubscribeMetadata("WaitTimeSeconds", "10"),
		jw.SubscribeMetadata("MaxNumberOfJobs", "4"))

	go func() {
		log.Println("Start work")
		err := worker.Work(&jw.WorkSetting{
			WorkerConcurrency: 5,
		})
		if err != nil {
			log.Println("Failed to work", err)
			return
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	<-quit

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()
	log.Println("Received a signal of graceful shutdown")

	if err := worker.Shutdown(ctx); err != nil {
		log.Println("Failed to graceful shutdown:", err)
	}

	log.Println("Completed graceful shutdown")
}

type HelloWorker struct {
}

func (HelloWorker) Work(job *jw.Job) error {
	log.Println("[HelloWorker]", job.Content)
	return nil
}
```

### Enqueue/EnqueueBatch

Implements job enqueue.

```go
sqs, err := jw.Open("sqs", map[string]interface{}{
	"Region":          os.Getenv("REGION"),
	"NumMaxRetries":   3,
})
if err != nil {
	log.Println("Could not open a sqs conn", err)
	return
}
sqs.SetLoggerFunc(log.Println)
worker, err := jw.New(&jw.Setting{
	Primary:    sqs,
	LoggerFunc: log.Println,
})
if err != nil {
	log.Println("Could not create a job worker", err)
	return
}

_, err := worker.Enqueue(context.Background(), &jw.EnqueueInput{
	Queue:   "test",
	Content: fmt.Sprintf(`{"msg":"%s"}`, uuid.NewV4().String()),
	Metadata: map[string]string{
		"MessageDelaySeconds": "3",
	},
})
if err != nil {
	log.Println("Failed to enqueue", err)
}
```

### Primary/Secondary

Set up primary and secondary connectors.

- __Primary:__ go-jwdk/awa-sqs-connector/sqs
- __Secondary:__ go-jwdk/db-connector/mysql

```go
import (
	jw "github.com/go-jwdk/jobworker"
	_ "github.com/go-jwdk/awa-sqs-connector"
	_ "github.com/go-jwdk/db-connector/mysql"
)

sqs, err := jobworker.Open("sqs", map[string]interface{}{
	"Region": os.Getenv("REGION"),
})

mysql, err := jobworker.Open("mysql", map[string]interface{}{
	"DSN":             "test-db",
	"MaxOpenConns":    3,
	"MaxMaxIdleConns": 3,
	"ConnMaxLifetime": time.Minute,
	"NumMaxRetries":   3,
})

jw, err := jw.New(&jw.Setting{
    Primary:   sqs,
    Secondary: mysql,
})
```
