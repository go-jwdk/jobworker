# jobworker

## DESCRIPTION

Package jobworker provides a generic interface around message queue.

The jobworker package must be used in conjunction with some message queue connector.

list of connectors:

- [__go-job-worker-development-kit/aws-sqs-connector/sqs__](https://github.com/go-job-worker-development-kit/aws-sqs-connector/)
- [__go-job-worker-development-kit/db-connector/mysql__](https://github.com/go-job-worker-development-kit/db-connector/)
- [__go-job-worker-development-kit/db-connector/postgres__](https://github.com/go-job-worker-development-kit/db-connector/)
- [__go-job-worker-development-kit/db-connector/sqlite3__](https://github.com/go-job-worker-development-kit/db-connector/)

## Requirements

Go 1.13+

## Installation

This package can be installed with the go get command:

```
$ go get -u github.com/go-job-worker-development-kit/jobworker
```

## Usage

### Basically

```go
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-job-worker-development-kit/jobworker"
	_ "github.com/go-job-worker-development-kit/awa-sqs-connector/sqs"
)

func main() {
	sqs, err := jobworker.Open("sqs", map[string]interface{}{
		"Region":          os.Getenv("REGION"),
		"AccessKeyID":     os.Getenv("ACCESS_KEY_ID"),
		"SecretAccessKey": os.Getenv("SECRET_ACCESS_KEY"),
	})
	if err != nil {
		panic("could not open sqs connector")
	}
	sqs.SetLogger(logger)

	jw, err := jobworker.New(&jobworker.Setting{
		DeadConnectorRetryInterval: 10,
		Primary:                    sqs,
		Logger:                     logger,
	})
	if err != nil {
		panic(err)
	}

	jw.Register("hello", &HelloWorker{})

	go func() {
		if err := jw.Work(&jobworker.WorkSetting{
			WorkerConcurrency: 1,
			Queue2PollingInterval: map[string]int64{
				"test.fifo": 2,
			},
		}); err != nil {
			log.Println(err)
		}
	}()

	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, os.Interrupt, syscall.SIGTERM)

	<-sigint

	log.Println("Received a signal of graceful shutdown")

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	if err := jw.Shutdown(ctx); err != nil {
		log.Printf("Failed to graceful shutdown: %v\n", err)
	}

	log.Println("Completed graceful shutdown")

}

type HelloWorker struct {
}

func (HelloWorker) Work(job *jobworker.Job) error {
	time.Sleep(time.Second)
	log.Println("[HelloWorker]", job.Args)
	return nil
}

type Logger struct {
}

func (l *Logger) Debug(v ...interface{}) {
	log.Println(v...)
}

var logger = &Logger{}
```

### enqueue

```go
sqs, err := jobworker.Open("sqs", map[string]interface{}{
    "Region":          os.Getenv("REGION"),
    "AccessKeyID":     os.Getenv("ACCESS_KEY_ID"),
    "SecretAccessKey": os.Getenv("SECRET_ACCESS_KEY"),
})

jw, err := jobworker.New(&jobworker.Setting{
    DeadConnectorRetryInterval: 10,
    Primary:                    sqs,
    Logger:                     logger,
})

err := jw.EnqueueJob(context.Background(), &jobworker.EnqueueJobInput{
    Queue: "test_queue",
    Payload: &jobworker.Payload{
        Class:        "hello",
        Args:         fmt.Sprintf(`{"msg":"%s"}`, "Hello Go JWDK!"),
        DelaySeconds: 3,
    },
})
```

### with secondary

```go
import (
    "github.com/go-job-worker-development-kit/jobworker"
    _ "github.com/go-job-worker-development-kit/awa-sqs-connector/sqs"
    _ "github.com/go-job-worker-development-kit/db-connector/mysql"
)

sqs, err := jobworker.Open("sqs", map[string]interface{}{
    "Region":          os.Getenv("REGION"),
    "AccessKeyID":     os.Getenv("ACCESS_KEY_ID"),
    "SecretAccessKey": os.Getenv("SECRET_ACCESS_KEY"),
})

sqlite3, err := jobworker.Open("mysql", map[string]interface{}{
    "DSN":             "test-db",
    "MaxOpenConns":    3,
    "MaxMaxIdleConns": 3,
    "ConnMaxLifetime": time.Minute,
    "NumMaxRetries":   3,
})

jw, err := jobworker.New(&jobworker.Setting{
    DeadConnectorRetryInterval: 10,
    Primary:                    sqs,
    Secondary:                  mysql,
    Logger:                     logger,
})
```