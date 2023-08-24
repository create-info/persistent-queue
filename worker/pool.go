package worker

import (
	"context"
	"github.com/create-info/persistent-queue/queue"
	"github.com/create-info/persistent-queue/queue_job"
	"runtime/debug"
	"sync"
	"time"
)

type Pool struct {
	capacity   int32
	running    int32
	expiryTime time.Duration
	workers    []*Worker
	backChan   chan *queue_job.Job
	stopChan   chan struct{}
	lock       sync.Mutex
	log        queue.Log
}

func NewWorkerPool(size, expiry int, backChan chan *queue_job.Job, log queue.Log) *Pool {
	p := &Pool{
		capacity:   int32(size),
		expiryTime: time.Duration(expiry) * time.Second,
		backChan:   backChan,
		stopChan:   make(chan struct{}),
		log:        log,
	}
	// 定期清理过期worker任务
	go p.monitorAndClear()
	return p
}

func (p *Pool) Submit(ctx context.Context, job *queue_job.Job) bool {
	w := p.getWorker(ctx)
	if w != nil {
		job.Ctx = ctx
		w.jobChan <- job
		return true
	} else {
		return false
	}
}

func (p *Pool) Stop() {
	close(p.stopChan)
}

func (p *Pool) getWorker(ctx context.Context) *Worker {
	var w *Worker
	waiting := false

	p.lock.Lock()
	idleWorkers := p.workers
	n := len(idleWorkers) - 1
	if n < 0 {
		// 当前队列无可用worker
		// 判断运行中worker数目已达到容量上限，置等待标志
		waiting = p.running >= p.capacity
	} else {
		// 当前队列有可用的worker，从队列尾部取出一个使用
		w = idleWorkers[n]
		idleWorkers[n] = nil
		p.workers = idleWorkers[:n]
	}
	p.lock.Unlock()

	if waiting {
		// 容量已满，不接受请求
		return nil
	} else if w == nil {
		// 当前无空闲worker，但是pool还没有满，直接新开一个worker执行任务
		w = &Worker{
			pool:        p,
			jobChan:     make(chan *queue_job.Job, 1),
			backChan:    p.backChan,
			releaseChan: make(chan struct{}),
			stopChan:    make(chan struct{}),
			log:         p.log,
		}
		go w.run(ctx)
		p.running++
	}
	return w
}

func (p *Pool) putWorker(worker *Worker) {
	worker.recycleTime = time.Now() // 写入回收时间，即worker的最后一次结束运行的时间
	p.lock.Lock()
	p.workers = append(p.workers, worker)
	p.lock.Unlock()
}

func (p *Pool) monitorAndClear() {
	for {
		p.periodicallyPurge()
		time.Sleep(5 * time.Second)
	}
}

func (p *Pool) periodicallyPurge() {
	defer func() {
		if err := recover(); err != nil {
			p.log.Info(context.Background(), "worker pool periodicallyPurge error:%+v, err_msg:%+v", err, string(debug.Stack()))
		}
	}()

	heartbeat := time.NewTicker(p.expiryTime)
	for range heartbeat.C {
		currentTime := time.Now()
		p.lock.Lock()
		idleWorkers := p.workers
		if len(idleWorkers) == 0 && p.running == 0 {
			p.lock.Unlock()
			return
		}
		n := 0
		for i, w := range idleWorkers {
			if currentTime.Sub(w.recycleTime) <= p.expiryTime {
				break
			}
			n = i + 1
			close(w.jobChan)
			<-w.releaseChan
			idleWorkers[i] = nil
			p.running--
		}
		if n >= len(idleWorkers) {
			p.workers = idleWorkers[:0]
		} else {
			p.workers = idleWorkers[n:]
		}

		p.lock.Unlock()
	}
}
