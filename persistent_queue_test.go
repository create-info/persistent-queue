package shark_async_job

import (
	"context"
	"fmt"
	"github.com/create-info/persistent-queue/job_constant"
	"github.com/create-info/persistent-queue/queue_job"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"strings"
	"testing"
	"time"
	"xorm.io/xorm"
)

var (
	e      *xorm.Engine
	gormDB *gorm.DB
)

/*

type GormSession struct {
	Session *gorm.DB
}

func (g *GormSession) Exec(sql string, values ...interface{}) error {
	return g.Session.Exec(sql, values...).Error
}

func (g *GormSession) Query(dest interface{}, sql string, values ...interface{}) error {
	return g.Session.Raw(sql, values...).Scan(dest).Error
}

func (g *GormSession) Ping() error {
	db, err := g.Session.DB()
	if err != nil {
		return err
	}
	return db.Ping()
}

func (g *GormSession) Close() error {
	db, err := g.Session.DB()
	if err != nil {
		return err
	}
	return db.Close()
}
*/

func InitXorm() {
	engine, err := xorm.NewEngine("mysql", "root:123456@tcp(127.0.0.1:3306)/shopeepay_rr_replayer_sg_db?charset=utf8")
	if err != nil {
		panic(err)
	}
	engine.SetMaxOpenConns(300)
	engine.SetMaxIdleConns(300)

	e = engine
}

func InitGorm() {
	dsn := "root:123456@tcp(127.0.0.1:3306)/shopeepay_rr_replayer_sg_db"
	var err error
	gormDB, err = gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		fmt.Errorf("connect to mysql error : %s", err)
		panic(err)
	}
}

func TestPersistentQueue_One(t *testing.T) {
	InitXorm()
	Init([]queue_job.Config{{
		ServiceName:        "test-one",
		JobType:            1,
		NeedRetry:          false,
		MaxRetries:         0,
		WaitTime:           10 * time.Second,
		WorkerPoolCapacity: 100,
		WorkerExpirySecond: 30,
		Db:                 e.DB().DB,
		FirstPullDay:       1,
		HandleJobFunc:      handle,
	}}, TestLog{})
	time.Sleep(10 * time.Minute)
}

func TestPersistentQueue_Two(t *testing.T) {
	InitXorm()
	InitGorm()
	gormDb, err := gormDB.DB()
	if err != nil {
		return
	}
	Init([]queue_job.Config{
		{
			ServiceName:        "test-two-1",
			JobType:            1,
			NeedRetry:          true,
			MaxRetries:         2,
			WaitTime:           20 * time.Second,
			WorkerPoolCapacity: 10,
			WorkerExpirySecond: 10,
			Db:                 gormDb,
			FirstPullDay:       1,
			HandleJobFunc:      handle,
		},
		{
			ServiceName:        "test-two-2",
			JobType:            3,
			NeedRetry:          true,
			MaxRetries:         3,
			WaitTime:           3 * time.Minute,
			WorkerPoolCapacity: 10,
			WorkerExpirySecond: 10,
			Db:                 gormDb,
			FirstPullDay:       1,
			HandleJobFunc:      handle,
		},
	}, TestLog{})
	time.Sleep(10 * time.Minute)
}

var handle = func(ctx context.Context, job *queue_job.Job) error {
	//rand.Seed(time.Now().UnixNano())
	//r := rand.Intn(5000)
	//
	//time.Sleep(time.Duration(r) * time.Millisecond)
	//if rand.Intn(100) <= 15 {
	//	job.Status = job_constant.JobStatusSuccess
	//} else if rand.Intn(100) <= 35 {
	//	job.Status = job_constant.JobStatusFailed
	//} else {
	//	job.Status = job_constant.JobStatusRetry
	//}
	time.Sleep(200 * time.Millisecond)
	job.Status = job_constant.JobStatusSuccess
	return nil
}

func Test_AddJob(t *testing.T) {
	//loc, _ := time.LoadLocation("Local")
	//date := time.Date(2022, time.January, 11, 0, 0, 0, 0, loc)
	//AddJob(1, date)
	for i := 0; i <= 50000; i++ {
		AddJob(1)
	}
}

func AddJob(jobType int32) {
	if err := queue_job.AddAsyncJob(e.DB().DB, queue_job.AddModelJob{
		JobType:   jobType,
		JobParams: nil,
		StartTime: 0,
	}); err != nil {
		fmt.Println(err.Error())
	}
}

func Test_DB(t *testing.T) {
	//for {
	//	for i := 1; i <= 100; i++ {
	//		go transaction(engine)
	//		fmt.Println(i)
	//		//time.Sleep(100 * time.Millisecond)
	//	}
	//	time.Sleep(30 * time.Second)
	//}
	idList := []string{"2758", "2759"}
	res := make([]queue_job.AsyncJobTab, 0)
	err := e.NewSession().And("id > ?", 2793).Or(fmt.Sprintf("id in (%s)", strings.Join(idList, ","))).Find(&res)
	t.Log(res, err)
}

func transaction() {
	InitXorm()
	for i := 1; i <= 3; i++ {
		e.Transaction(func(session *xorm.Session) (interface{}, error) {
			//time.Sleep(500 * time.Millisecond)

			dao := queue_job.NewAsyncJobDao(e.DB().DB)
			now := uint32(time.Now().Unix())
			_, err := dao.AddJob(queue_job.AsyncJobTab{
				Params:     "",
				JobType:    3,
				JobStatus:  0,
				CreateTime: now,
				UpdateTime: now,
			})
			//fmt.Println(err.Error())
			return nil, err
		})
		time.Sleep(5 * time.Second)
	}
}

type TestLog struct {
}

func (t TestLog) GetNewContext(serviceName string) context.Context {
	return context.TODO()
}

func (t TestLog) Info(ctx context.Context, info string) {
	//log.Info(info)
}

func (t TestLog) Warn(ctx context.Context, warning string) {
	//log.Warn(warning)
}

func (t TestLog) Error(ctx context.Context, error string) {
	//log.Errorf(error)
}

func (t TestLog) Debug(ctx context.Context, info string) {
	//log.Debug(info)
}
