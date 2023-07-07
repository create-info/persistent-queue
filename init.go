package shark_async_job

import (
	"context"
	"git.garena.com/shopee/pl/shopeepay-common/log"
	"git.garena.com/xiaomin.xu/shark-async-job/queue"
	"git.garena.com/xiaomin.xu/shark-async-job/queue_job"
)

func Init(jobConfigList []queue_job.Config, queueLog queue.Logger) {
	ctx := context.Background()
	for _, jobConfig := range jobConfigList {
		err := jobConfig.Validate()
		if err != nil {
			log.Infof(ctx, "queue jocConfig validate error:%+v", err)
			panic(err)
		}
		pq, err := newPersistentQueue(jobConfig, queueLog)
		if err != nil {
			log.Infof(ctx, "queue init error:%+v, jobConfig:%+v", err, jobConfig)
			panic(err)
		} else {
			go pq.start()
		}
	}
}
