# PersistentQueue

# 一、简介
项目中经常会出现需要异步处理的需求，比如用户下单后，我们需要给用户发送消息，发送消息的逻辑可以异步处理，不用阻塞核心业务逻辑，再比如有些任务需要在创建后，延迟一段时间再去处理等。
PersistentQueue基于go语言实现，提供了基于数据库的持久化任务队列，擅长于处理跟业务逻辑解耦异步任务，
不管是单点式应用还是分布式应用，只要底层DB不变，加入PersistentQueue中的任务就能保证只会被执行一次。

# 二、关键能力
### 1. 任务持久化到DB、任务立即或延迟执行、支持设置任务最大重试次数，重试间隔等。
### 2. 与常见的orm框架无缝对接，只要orm框架的DB底层实现了下面四个接口，则都可以支持，如sqlx、xorm、goorm等。
 ``` go
	type DB interface {
		// Add your required methods here
		Exec(string, ...interface{}) (sql.Result, error)
		Query(string, ...interface{}) (*sql.Rows, error)
		Ping() error
		Close() error
	}
```
### 3. 指定多工作协程来处理异步任务，基于本地内存处理。


# 三、接入方式
```shell
go get git.garena.com/shopee/pl/efficacy/shark-async-job@master
```

- 应用创建shedlock表-用于任务执行前的加锁和续约锁

    ```sql
	CREATE TABLE `shedlock_tab` (
		`lock_name` varchar(64) NOT NULL COMMENT 'lock key',
		`lock_until` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'lock expiry time',
		`locked_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'lock time',
		`locked_by` varchar(255) NOT NULL COMMENT 'who lock',
		PRIMARY KEY (`lock_name`)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    ```
- 应用创建async_job_tab表-用于新增待处理的异步任务到持久化队列
    ```sql
	CREATE TABLE `async_job_tab` (
		`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
		`params` varchar(4096) COLLATE utf8mb4_unicode_ci DEFAULT '' NOT NULL,
		`job_type` int(11) NOT NULL,
		`job_status` tinyint(4) DEFAULT NULL COMMENT '0:init, 1:success, 2:failed, 3:continue, 4:retry',
		`start_time` int(11) unsigned DEFAULT '0',
		`create_time` int(11) unsigned NOT NULL,
		`update_time` int(11) unsigned NOT NULL,
		PRIMARY KEY (`id`)
	) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    ```
  
- 创建ShedLockConfig(shedlock.Config)

    ``` go
    shedlock.Config{
        // 支持底层是*sql.DB的go orm库
        DB queue_job.DB
        // 服务的名字
        ServiceName:   "my-test",
        // 锁生存时间，单位s
        LockSecond:    10,
        // 最小锁时间，单位s，默认为 LockSecond>5:LockSecond-5 / LockSecond<5:LockSecond-1
        MinLockSecond uint32
    }
  ```
    
- 创建JobConfig(queue_job.Config)，并注入ShedLockConfig

      ``` go
      queue_job.Config{
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
      ```
- 将创建好的JobConfigList，只需项目启动时，执行persistent_queue.Init(jobConfigList []queue_job.Config, logger.QueueLog{})一次即可。后续将任务插入到async_job_tab中，即可自动异步执行。

# 四、应用使用示例
 ``` go
    var (
        TriggerCompareJobConfig = queue_job.Config{
            ServiceName:        "shark-compare-export-trigger-compare",
            JobType:            dbmodel.TrafficCompareAsyncJobType,
            NeedRetry:          true,
            MaxRetries:         MaxRetries,
            WaitTime:           3 * time.Minute,
            WorkerPoolCapacity: 100,
            WorkerExpirySecond: 30,
            FirstPullDay:       7,
            HandleJobFunc:      TriggerCompareJobHandler,//异步任务的handler
        }
    )
    
    const (
        MaxRetries = 10
    )

	func AsyncJobInit() {
		jobConfigList := make([]queue_job.Config, 0)
		jobConfigList = append(jobConfigList,
			TriggerCompareJobConfig, //将这个异步任务加入配置列表
		)
	
		for i := range jobConfigList {
			config := &jobConfigList[i]
			gorm, err := client.DB.DB() //*sql.DB
			if err != nil {
				panic(err)
			}
			config.Db = gorm
		}
		shark_async_job.Init(jobConfigList, logger.QueueLog{})
	}
```
