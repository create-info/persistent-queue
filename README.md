### PersistentQueue

- 创建shedlock表

    ```sql
   CREATE TABLE `shedlock_tab` (
		`lock_name` varchar(64) NOT NULL COMMENT 'lock key',
		`lock_until` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'lock expiry time',
		`locked_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'lock time',
		`locked_by` varchar(255) NOT NULL COMMENT 'who lock',
		PRIMARY KEY (`lock_name`)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    ```
- 创建ShedLockConfig(shedlock.Config)

	```go
	shedlock.Config{
		// 支持底层是*sql.DB的go orm库
		Session:       engine.NewSession(),
		// 服务的名字
		ServiceName:   "my-test",
		// 自定义的lock key
		Key:           key,
		// 锁生存时间，单位s
		LockSecond:    10,
		// 最小锁时间，单位s
		MinLockSecond: 0,
	}
	```
- 创建Operation
	Opreation接口规定了job运行的3个主要步骤load->handle->update
	```go
	type Operation interface {
        // LoadJobs 从xx_job_tab按照maxId偏移拉取异步任务，同时将当前的maxId放回config.MaxId，返回Job切片
        LoadJobs(ctx context.Context, config *Config) ([]*Job, error)
        // HandleJob Job具体处理过程，无论什么状况，都需要修改job.Status
        HandleJob(ctx context.Context, job *Job) error
        // BatchUpdateJobs 批量将jobList的终态结果更新到db中，返回更新db的结果信息(affected, error)
        BatchUpdateJobs(ctx context.Context, jobList []*Job) (int64, error)
    }
	```
	具体可以查看例子：queue_job.TestOperation{}
	1. 从task_tab LoadJobs任务进内存时，需将tab映射成如下的Job Struct
	```go
	type Job struct {
		Id              int64 // task_tab_id
		Status          job_constant.JobStatus // task_tab_status
		retries         int // 不用管，内部自己用
		NextExecuteTime uint32 // 可自定义下次执行时间
		JobParams       interface{} // HandleJob的请求参数
		Config          *Config // 下面定义的JobConfig，需注入
	}
	```
	2. HandleJob 处理任务，结束是必须将任务的状态改成 Retry/Success/Failed
	3. BatchUpdateJobs更新Job时，会注入抵达终态(Success/Failed)的JobList，需将Job映射成自己的task_tab并更新
- 创建JobConfig(queue_job.Config)，并注入ShedLockConfig和Operation

	```go
	queue_job.Config{{
	    // MaxId 当前拉取异步任务tab的最大id
		MaxId:              0,
		// NeedRetry 异步任务是否需要重试
		NeedRetry:          true,
		// MaxRetries 最大重试次数
		MaxRetries:         1,
		// WaitTime 重试等待时间
		WaitTime:           10 * time.Second,
		// worker池容量
		WorkerPoolCapacity: 10,
		// worker空闲时间，单位s
		WorkerExpiry:       10,
		// shedLock 配置
		Operation:          queue_job.TestOperation{},
		ShedLockConfig:     getShedLockConfig("test-one", "hahaha"),
	}
	```
- 将创建好的JobConfigList，只需项目启动时，执行persistent_queue.Init(jobConfigList []queue_job.Config)一次即可。后续将任务插入到task_tab中，即可自动异步执行。
