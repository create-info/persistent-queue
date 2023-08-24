package worker

import (
	"context"
	"fmt"
	"git.garena.com/xiaomin.xu/shark-async-job/job_constant"
	"git.garena.com/xiaomin.xu/shark-async-job/queue"
	"git.garena.com/xiaomin.xu/shark-async-job/queue_job"
	"testing"
	"time"
)

func TestPool(t *testing.T) {
	backChan := make(chan *queue_job.Job, 1000)
	pool := NewWorkerPool(100, 10, backChan, queue.Log{L: TestLog{}})

	for i := 1; i <= 3; i++ {
		go func() {
			for i := 0; i <= 100000; i++ {
				t.Log(pool.Submit(context.TODO(), &queue_job.Job{
					Id:            uint64(i),
					Status:        job_constant.JobStatusInit,
					JobType:       1,
					JobParams:     "",
					HandleJobFunc: f,
				}))
				time.Sleep(300 * time.Millisecond)
			}
		}()
	}

	for i := 0; i <= 1000000; i++ {
		j := <-backChan
		fmt.Printf("id:%d\tstatus:%d\n", j.Id, j.Status)
	}
}

type TestLog struct {
}

func (t TestLog) GetNewContext(serviceName string) context.Context {
	return context.TODO()
}

func (t TestLog) Info(ctx context.Context, info string) {
	fmt.Printf(info)
}

func (t TestLog) Warn(ctx context.Context, warning string) {
	fmt.Printf(warning)
}

func (t TestLog) Error(ctx context.Context, error string) {
	fmt.Printf(error)
}

func (t TestLog) Debug(ctx context.Context, info string) {
	fmt.Printf(info)
}

func f(ctx context.Context, job *queue_job.Job) error {
	time.Sleep(5 * time.Second)
	job.Status = job_constant.JobStatusContinue
	return nil
}
