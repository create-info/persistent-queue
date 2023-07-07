package worker

import (
	"context"
	"git.garena.com/xiaomin.xu/shark-async-job/queue"
	"git.garena.com/xiaomin.xu/shark-async-job/queue_job"
	"runtime/debug"
	"time"
)

type Worker struct {
	pool        *Pool
	jobChan     chan *queue_job.Job
	backChan    chan *queue_job.Job
	releaseChan chan struct{}
	stopChan    chan struct{}
	recycleTime time.Time
	log         queue.Log
}

func (w *Worker) run(ctx context.Context) {
	var (
		job *queue_job.Job
		ok  bool
	)

	defer func() {
		if err := recover(); err != nil {
			w.log.Error(ctx, "worker error:%+v, err_msg:%s", err, string(debug.Stack()))
			if job != nil {
				w.backChan <- job
			}
		}
	}()

	for {
		select {
		case job, ok = <-w.jobChan:
			if !ok {
				w.releaseChan <- struct{}{}
				return
			}
			ctx = job.Ctx
			err := job.HandleJobFunc(ctx, job)
			if err != nil {
				w.log.Error(ctx, "worker run queue_job error:%+v", err)
			}
			w.backChan <- job
			// 回收复用
			w.pool.putWorker(w)
			job = nil
		case <-w.stopChan:
			return
		}
	}
}
