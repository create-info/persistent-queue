package shark_async_job

import (
	"container/heap"
	"context"
	"git.garena.com/xiaomin.xu/shark-async-job/job_constant"
	"git.garena.com/xiaomin.xu/shark-async-job/queue"
	"git.garena.com/xiaomin.xu/shark-async-job/queue_job"
	"git.garena.com/xiaomin.xu/shark-async-job/shedlock"
	"git.garena.com/xiaomin.xu/shark-async-job/worker"
	"github.com/google/btree"
	"runtime/debug"
	"strconv"
	"sync"
	"time"
)

type PersistentQueue struct {
	jobQueue      *queue.FIFOJobQueue   // 即时任务队列
	delayJobQueue *queue.PriorityQueue  // 延迟任务队列
	skipInfoTree  *btree.BTree          // 空洞BTree
	workerPool    *worker.Pool          // 消费者池
	jobDao        queue_job.AsyncJobDao // 处理job的dao
	backChan      chan *queue_job.Job   // 回收chan

	ctx                context.Context
	serviceName        string         // 服务名字
	maxId              uint64         // 当前拉取异步任务tab的最大id
	jobType            int32          // 任务编号
	dbSession          queue_job.DB   // 更新job session
	needRetry          bool           // 是否需要重试
	maxReties          int            // 最大重试次数
	waitTime           time.Duration  // 重试等待时间
	firstPullDay       int            // 项目启动时拉取未完成任务的天数
	queueMaxSize       int            // 队列最大长度
	loadLimit          int            // load job的limit
	shedLock           *shedlock.Lock // db锁
	canRun             bool           // 是否能运行
	loadingRate        float64        // 循环时间
	workerPoolCapacity int            // worker pool容量
	workerExpiry       int            // worker空闲时间
	log                queue.Log      // 日志

	HandleJobFunc func(ctx context.Context, job *queue_job.Job) error // Job具体处理过程

	rwLock *sync.RWMutex
	wg     sync.WaitGroup
}

func newPersistentQueue(jobConfig queue_job.Config, logger queue.Logger) (*PersistentQueue, error) {
	var err error
	pq := new(PersistentQueue)
	pq.log = queue.Log{L: logger}

	pq.shedLock, err = shedlock.NewShedLock(shedlock.Config{
		DB:            jobConfig.Db,
		ServiceName:   jobConfig.ServiceName,
		LockSecond:    shedLockSecond,
		MinLockSecond: shedLockMinLockSecond,
	}, logger)
	if err != nil {
		return nil, err
	}

	pq.serviceName = jobConfig.ServiceName
	pq.maxId = 0
	pq.jobType = jobConfig.JobType
	pq.needRetry = jobConfig.NeedRetry
	pq.maxReties = jobConfig.MaxRetries
	pq.waitTime = jobConfig.WaitTime
	if pq.firstPullDay < 0 {
		pq.firstPullDay = defaultFirstPullDay
	} else {
		pq.firstPullDay = jobConfig.FirstPullDay
	}
	if pq.queueMaxSize <= 0 {
		pq.queueMaxSize = defaultQueueSizeLimit
	} else {
		pq.queueMaxSize = jobConfig.QueueMaxSize
	}
	if pq.loadLimit <= 0 {
		pq.loadLimit = defaultLoadLimit
	} else {
		pq.loadLimit = jobConfig.LoadLimit
	}
	pq.HandleJobFunc = jobConfig.HandleJobFunc
	pq.rwLock = new(sync.RWMutex)
	pq.workerPoolCapacity = jobConfig.WorkerPoolCapacity
	if pq.workerPoolCapacity <= 0 {
		pq.workerPoolCapacity = defaultWorkerPoolCapacity
	}
	pq.workerExpiry = jobConfig.WorkerExpirySecond
	if pq.workerExpiry <= 0 {
		pq.workerExpiry = defaultWorkerExpirySecond
	}
	pq.dbSession = jobConfig.Db
	return pq, nil
}

func (pq *PersistentQueue) start() {
	for {
		if !pq.startJob() {
			return
		}
		time.Sleep(30 * time.Second)
	}
}

func (pq *PersistentQueue) startJob() bool {
	pq.ctx = pq.log.GetNewContext(pq.serviceName)
	defer func() {
		if err := recover(); err != nil {
			pq.log.Error(pq.ctx, "%s persistent queue error:%+v, err_msg:%+v", pq.serviceName, err, string(debug.Stack()))
		}
		if err := pq.shedLock.Unlock(pq.ctx, false); err != nil {
			pq.log.Error(pq.ctx, "%s persistent queue unlock error:%+v", pq.serviceName, err)
		}
		if pq.workerPool != nil {
			pq.workerPool.Stop()
		}
	}()

	// 1. 抢锁
	if !pq.tryGetShedLock() {
		// 抢锁失败，退出该函数，等待30s
		return true
	} else {
		// 抢锁成功，开协程监听续期是否成功
		go pq.watchFailedExtendChan()
	}

	pq.init()

	pq.log.Info(pq.ctx, "%s persistent queue start, job:%+v", pq.serviceName, pq)
	for {
		if !pq.canRun {
			// 续期锁失败，退出该函数，等待30s
			return true
		}
		pq.ctx = pq.log.GetNewContext(pq.serviceName)

		// 2. 加载job
		err := pq.loadJob()
		if err != nil {
			pq.log.Error(pq.ctx, "%s persistent queue load queue_job error:%+v", pq.serviceName, err)
		}

		if pq.jobQueue.Len() == 0 && !pq.needExecuteDelayJob() {
			// 没有job需要执行
			pq.log.Info(pq.ctx, "%s persistent queue no job need run", pq.serviceName)
			pq.calcLoadingRate()
		} else {
			pq.loadingRate = fastLoadingRate

			// 3. 分发job
			needReceive, err := pq.dispatchJob()
			if err != nil {
				pq.log.Error(pq.ctx, "%s persistent queue dispatch queue_job error:%+v", pq.serviceName, err)
			} else if needReceive {
				// 4. 回收job
				pq.receiveJob()
			} else {
				pq.log.Info(pq.ctx, "%s persistent queue no job need dispatch", pq.serviceName)
			}
		}
		time.Sleep(time.Duration(pq.loadingRate) * time.Second)
	}
}

func (pq *PersistentQueue) init() {
	pq.loadingRate = fastLoadingRate

	pq.jobQueue = queue.NewFIFOJobQueue()
	pq.delayJobQueue = new(queue.PriorityQueue)
	heap.Init(pq.delayJobQueue)
	pq.skipInfoTree = btree.New(2)
	pq.backChan = make(chan *queue_job.Job, pq.queueMaxSize)
	pq.workerPool = worker.NewWorkerPool(pq.workerPoolCapacity, pq.workerExpiry, pq.backChan, pq.log)
	pq.jobDao = queue_job.NewAsyncJobDao(pq.dbSession)
}

// 加载job
func (pq *PersistentQueue) loadJob() error {
	pq.log.Info(pq.ctx, "%s start loadJob", pq.serviceName)
	firstLoad := pq.maxId == 0
	var (
		maxId         uint64
		err           error
		skipIdStrList = make([]string, 0)
		skipInfoMap   = make(map[uint64]SkipInfo)
	)
	if firstLoad {
		// 首次加载需要先找maxId
		maxId, err = pq.jobDao.GetMaxId()
		if err != nil {
			pq.log.Error(pq.ctx, "%s persistent queue get job maxId error:%+v", err, pq.serviceName)
			return err
		}
	} else {
		// 偏移加载需要查询空洞
		// 先删除过期数据
		l := pq.skipInfoTree.Len()
		for i := 0; i < l; i++ {
			min := pq.skipInfoTree.DeleteMin().(SkipInfo)
			if !min.CanDrop() {
				pq.skipInfoTree.ReplaceOrInsert(min)
				break
			}
		}

		pq.skipInfoTree.Ascend(func(a btree.Item) bool {
			info := a.(SkipInfo)
			skipIdStrList = append(skipIdStrList, strconv.FormatInt(int64(info.id), 10))
			skipInfoMap[info.id] = info
			if len(skipIdStrList) > 2000 {
				return false
			}
			return true
		})
	}
	if pq.jobQueue.Len()+pq.delayJobQueue.Len() > defaultQueueSizeLimit {
		pq.log.Info(pq.ctx, "%s persistent queue full, instant_queue size:%d, delay_queue size:%d", pq.serviceName, pq.jobQueue.Len(), pq.delayJobQueue.Len())
		return nil
	}

	// 查数据库
	tabList, err := pq.jobDao.FindUnhandledJobByMaxId(pq.maxId, pq.firstPullDay, skipIdStrList, defaultLoadLimit)
	if err != nil {
		pq.log.Error(pq.ctx, "%s persistent queue load jobs error:%+v", pq.serviceName, err)
		return err
	}
	if len(tabList) == 0 {
		if firstLoad {
			// 首次加载没数据
			pq.maxId = maxId
			return nil
		} else {
			// 偏移没数据
			return nil
		}
	}

	// 过滤job
	jobList := make([]*queue_job.Job, 0, len(tabList))
	for _, tab := range tabList {
		if !firstLoad {
			if tab.Id == pq.maxId+1 {
				// 正常逻辑
				pq.maxId = tab.Id
			} else if tab.Id < pq.maxId {
				// 删除旧空洞
				pq.skipInfoTree.Delete(skipInfoMap[tab.Id])
			} else {
				// 新增空洞
				for i := pq.maxId + 1; i < tab.Id; i++ {
					pq.skipInfoTree.ReplaceOrInsert(NewSkipInfo(i))
				}
				pq.maxId = tab.Id
			}
		} else {
			pq.maxId = tab.Id
		}

		if tab.JobType != pq.jobType || tab.JobStatus != job_constant.JobStatusInit {
			continue
		}

		job, err := queue_job.ConvertTabToJob(tab, pq.HandleJobFunc)
		if err != nil {
			pq.log.Error(pq.ctx, "%s persistent queue convert jobs error:%+v, jobId:%d", err, pq.serviceName, tab.Id)
			continue
		}
		pq.log.Info(pq.ctx, "%s persistent queue get job:%+v", pq.serviceName, job)
		if tab.StartTime == 0 {
			jobList = append(jobList, &job)
		} else {
			heap.Push(pq.delayJobQueue, job)
		}
	}
	pq.jobQueue.BatchOffer(jobList)
	return nil
}

// 分发job
func (pq *PersistentQueue) dispatchJob() (bool, error) {
	pq.log.Info(pq.ctx, "%s start dispatch", pq.serviceName)
	l1 := pq.jobQueue.Len()
	// 即时任务
	for i := 0; i < l1; i++ {
		job, _ := pq.jobQueue.Poll()

		// 未到消费时间
		if job.NextExecuteTime >= uint32(time.Now().Unix()) {
			pq.jobQueue.Offer(job)
			continue
		}

		if !pq.workerPool.Submit(pq.ctx, job) {
			// worker容量已满，下轮循环再处理
			pq.jobQueue.Push(job)
			pq.log.Info(pq.ctx, "%s PersistentQueue worker is full", pq.serviceName)
			return l1 != pq.jobQueue.Len(), nil
		} else {
			pq.log.Info(pq.ctx, "%s persistent queue dispatch job:%+v", pq.serviceName, job)
		}
		pq.wg.Add(1)
	}

	// 延迟任务
	l2 := pq.delayJobQueue.Len()
	for i := 0; i < l2; i++ {
		job := heap.Pop(pq.delayJobQueue).(queue_job.Job)

		// 未到消费时间
		if job.NextExecuteTime >= uint32(time.Now().Unix()) {
			heap.Push(pq.delayJobQueue, job)
		}

		pq.log.Info(pq.ctx, "%s PersistentQueue submit job:%+v", pq.serviceName, job)
		if !pq.workerPool.Submit(pq.ctx, &job) {
			// worker容量已满，下轮循环再处理
			pq.jobQueue.Push(&job)
			pq.log.Info(pq.ctx, "%s PersistentQueue worker is full", pq.serviceName)
			break
		}
		pq.wg.Add(1)
	}
	return l1 != pq.jobQueue.Len() || l2 != pq.delayJobQueue.Len(), nil
}

// 回收job
func (pq *PersistentQueue) receiveJob() {
	pq.log.Info(pq.ctx, "%s start receive", pq.serviceName)

	sig := make(chan interface{}, 1)
	go func() {
		pq.wg.Wait()
		sig <- nil
	}()

	finalJobList := make([]*queue_job.Job, 0)
loop:
	for {
		select {
		case j := <-pq.backChan:
			pq.log.Info(pq.ctx, "%s persistent receive job:%+v", pq.serviceName, j)
			switch j.Status {
			case job_constant.JobStatusInit, job_constant.JobStatusRetry:
				if pq.needRetry {
					j.Retries++
					if j.Retries > pq.maxReties {
						pq.log.Info(pq.ctx, "%s persistent go on retry job", pq.serviceName)
						j.Status = job_constant.JobStatusFailed
						finalJobList = append(finalJobList, j)
					} else {
						pq.log.Info(pq.ctx, "%s persistent end retry job", pq.serviceName)
						j.NextExecuteTime = uint32(time.Now().Add(pq.waitTime).Unix())
						pq.jobQueue.Offer(j)
					}
				}
			case job_constant.JobStatusContinue:
				pq.log.Info(pq.ctx, "%s persistent continue job", pq.serviceName)
				j.NextExecuteTime = uint32(time.Now().Add(pq.waitTime).Unix())
				pq.jobQueue.Offer(j)
			case job_constant.JobStatusSuccess, job_constant.JobStatusFailed:
				pq.log.Info(pq.ctx, "%s persistent finish job", pq.serviceName)
				finalJobList = append(finalJobList, j)
			default:
				pq.log.Error(pq.ctx, "%s persistent queue wrong queue_job status", pq.serviceName)
				j.Status = job_constant.JobStatusFailed
				finalJobList = append(finalJobList, j)
			}
			pq.wg.Done()
		case <-sig:
			break loop
		}
	}

	// 当前任务抢不到锁，丢弃
	if !pq.canRun {
		return
	}
	if len(finalJobList) != 0 {
		pq.log.Info(pq.ctx, "%s start BatchUpdateJobs, listLen:%d", pq.serviceName, len(finalJobList))
		for _, job := range finalJobList {
			tab := queue_job.ConvertJobToTab(*job)
			affected, err := pq.jobDao.UpdateJob(tab)
			if err != nil || affected != 1 {
				pq.log.Error(pq.ctx, "%s persistent queue batch update job error:%+v, affected:%d, job:%+v", pq.serviceName, err, affected, job)
			} else {
				pq.log.Info(pq.ctx, "%s persistent queue update job:%+v", pq.serviceName, job)
			}
		}
	}
	return
}

// 通用
func (pq *PersistentQueue) calcLoadingRate() {
	if pq.loadingRate == slowestLoadingRate {
		return
	}
	pq.loadingRate = pq.loadingRate * loadingBackoffRate
	if pq.loadingRate >= slowestLoadingRate {
		pq.loadingRate = slowestLoadingRate
	}
}

func (pq *PersistentQueue) needExecuteDelayJob() bool {
	if pq.delayJobQueue.Len() == 0 {
		return false
	}
	job := heap.Pop(pq.delayJobQueue).(queue_job.Job)
	heap.Push(pq.delayJobQueue, job)
	return job.NextExecuteTime <= uint32(time.Now().Unix())
}

func (pq *PersistentQueue) tryGetShedLock() bool {
	if pq.shedLock.TryLock(pq.ctx) {
		pq.canRun = true
		return true
	} else {
		pq.canRun = false
		return false
	}
}

func (pq *PersistentQueue) watchFailedExtendChan() {
	if pq.shedLock.ExtendFailed() == nil {
		return
	}
	select {
	case <-pq.shedLock.ExtendFailed():
		// 续期失败，pod退出该任务
		pq.rwLock.Lock()
		//  更改任务运行状态
		pq.canRun = false
		pq.rwLock.Unlock()
		return
	}
}
