package queue

import "git.garena.com/xiaomin.xu/shark-async-job/queue_job"

type FIFOJobQueue struct {
	queue []*queue_job.Job
	len   int
}

func NewFIFOJobQueue() *FIFOJobQueue {
	jobQueue := new(FIFOJobQueue)
	jobQueue.queue = make([]*queue_job.Job, 0)
	jobQueue.len = 0
	return jobQueue
}

func (q *FIFOJobQueue) Offer(job *queue_job.Job) {
	q.queue = append(q.queue, job)
	q.len++
}

func (q *FIFOJobQueue) BatchOffer(jobList []*queue_job.Job) {
	q.queue = append(q.queue, jobList...)
	q.len += len(jobList)
}

func (q *FIFOJobQueue) Push(job *queue_job.Job) {
	q.queue = append([]*queue_job.Job{job}, q.queue...)
	q.len++
}

func (q *FIFOJobQueue) Poll() (*queue_job.Job, bool) {
	if q.len == 0 {
		return nil, false
	}

	res := q.queue[0]
	q.queue = q.queue[1:]
	q.len--
	return res, true
}

func (q *FIFOJobQueue) Peek() *queue_job.Job {
	if q.len == 0 {
		return nil
	}
	return q.queue[0]
}

func (q *FIFOJobQueue) isEmpty() bool {
	return q.len == 0
}

func (q *FIFOJobQueue) Len() int {
	return q.len
}
