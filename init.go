package shark_async_job

import (
	"git.garena.com/xiaomin.xu/shark-async-job/queue"
	"git.garena.com/xiaomin.xu/shark-async-job/queue_job"
)

func Init(jobConfigList []queue_job.Config, queueLog queue.Logger) {
	for _, jobConfig := range jobConfigList {
		err := jobConfig.Validate()
		if err != nil {
			panic(err)
		}
		pq, err := newPersistentQueue(jobConfig, queueLog)
		if err != nil {
			panic(err)
		} else {
			go pq.start()
		}
	}
}
