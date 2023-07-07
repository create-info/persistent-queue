package job_constant

type JobStatus int32

const (
	JobStatusInit     JobStatus = 0
	JobStatusSuccess  JobStatus = 1
	JobStatusFailed   JobStatus = 2
	JobStatusContinue JobStatus = 3
	JobStatusRetry    JobStatus = 4
)
