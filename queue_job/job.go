package queue_job

import (
	"context"
	"encoding/json"
	"errors"
	"git.garena.com/xiaomin.xu/shark-async-job/job_constant"
	"time"
)

type Job struct {
	Id              uint64
	Status          job_constant.JobStatus
	Retries         int
	NextExecuteTime uint32
	JobType         int32
	JobParams       string // 请求参数
	StartTime       uint32 // 启动时间，为0则是立即执行

	Ctx           context.Context
	HandleJobFunc func(ctx context.Context, job *Job) error
}

type AddModelJob struct {
	JobType   int32
	JobParams interface{} // 请求参数
	StartTime uint32      // 启动时间，为0则是立即执行
}

func ConvertTabToJob(tab AsyncJobTab, f func(ctx context.Context, job *Job) error) (Job, error) {
	var nextExecuteTime uint32
	if tab.StartTime == 0 {
		nextExecuteTime = uint32(time.Now().Unix())
	} else {
		nextExecuteTime = tab.StartTime
	}
	return Job{
		Id:              tab.Id,
		Status:          tab.JobStatus,
		Retries:         0,
		NextExecuteTime: nextExecuteTime,
		JobType:         tab.JobType,
		JobParams:       tab.Params,
		StartTime:       tab.StartTime,
		HandleJobFunc:   f,
	}, nil
}

func ConvertJobToTab(job Job) AsyncJobTab {
	return AsyncJobTab{
		Id:         job.Id,
		Params:     job.JobParams,
		JobType:    job.JobType,
		JobStatus:  job.Status,
		StartTime:  job.StartTime,
		CreateTime: uint32(time.Now().Unix()),
		UpdateTime: uint32(time.Now().Unix()),
	}
}

func AddAsyncJob(session DB, modelJob AddModelJob) error {
	bytes, err := json.Marshal(modelJob.JobParams)
	if err != nil {
		return err
	}
	job := Job{
		Status:    job_constant.JobStatusInit,
		JobType:   modelJob.JobType,
		JobParams: string(bytes),
		StartTime: modelJob.StartTime,
	}
	tab := ConvertJobToTab(job)
	dao := NewAsyncJobDao(session)
	affected, err := dao.AddJob(tab)
	if err != nil || affected != 1 {
		return errors.New("add async job error")
	}
	return nil
}
