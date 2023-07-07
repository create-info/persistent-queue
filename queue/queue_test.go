package queue

import (
	job2 "git.garena.com/xiaomin.xu/shark-async-job/queue_job"
	"testing"
)

func TestQueue(t *testing.T) {
	queue := NewFIFOJobQueue()
	queue.Offer(new(job2.Job))
	job, b := queue.Poll()
	t.Logf("queue_job:%+v, b:%+v", job, b)

	queue.Offer(new(job2.Job))
	job, b = queue.Poll()
	t.Logf("queue_job:%+v, b:%+v", job, b)

	job, b = queue.Poll()
	t.Logf("queue_job:%+v, b:%+v", job, b)
}
