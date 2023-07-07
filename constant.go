package shark_async_job

const (
	fastLoadingRate           = 5
	slowestLoadingRate        = 60
	loadingBackoffRate        = 1.2
	defaultWorkerPoolCapacity = 100
	defaultWorkerExpirySecond = 60
	defaultFirstPullDay       = 7
	defaultLoadLimit          = 10000
	defaultQueueSizeLimit     = 50000

	shedLockSecond        = 30
	shedLockMinLockSecond = 25
)
