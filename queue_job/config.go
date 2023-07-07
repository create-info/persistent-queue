package queue_job

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"time"
)

type Config struct {
	// ServiceName 服务名字
	ServiceName string
	// JobType 任务编号
	JobType int32
	// NeedRetry 异步任务是否需要重试
	NeedRetry bool
	// MaxRetries 最大重试次数
	MaxRetries int
	// WaitTime 重试等待时间
	WaitTime time.Duration
	// WorkerPoolCapacity worker池容量，默认100
	WorkerPoolCapacity int
	// WorkerExpirySecond worker空闲时间(s)，默认60s
	WorkerExpirySecond int
	// master DB
	Db DB
	// FirstPullDay 项目启动时拉取未完成任务的天数，无需可为-1，默认7天
	FirstPullDay int
	// QueueMaxSize 队列最大长度，默认50000
	QueueMaxSize int
	// LoadLimit load job的limit，默认10000
	LoadLimit int

	// HandleJobFunc Job具体处理过程，无论什么状况，都需要修改job.Status
	HandleJobFunc func(ctx context.Context, job *Job) error

	rwLock    *sync.RWMutex
	isRunning bool
}

type DB interface {
	// Add your required methods here
	Exec(string, ...interface{}) (sql.Result, error)
	Query(string, ...interface{}) (*sql.Rows, error)
	Ping() error
	Close() error
}

var (
	dbEngineError      = errors.New("wrong db engine")
	handleJobFuncError = errors.New("handJobFunc is nil")
)

func (c *Config) Validate() error {
	if err := c.Db.Ping(); err != nil {
		return dbEngineError
	}
	if c.HandleJobFunc == nil {
		return handleJobFuncError
	}
	return nil
}
