package queue_job

import (
	"fmt"
	"git.garena.com/xiaomin.xu/shark-async-job/job_constant"
	"strings"
	"time"
)

type AsyncJobTab struct {
	Id         uint64                 `xorm:"pk autoincr BIGINT(20)"`
	Params     string                 `xorm:"not null VARCHAR(4096)"`
	JobType    int32                  `xorm:"not null INT(11)"`
	JobStatus  job_constant.JobStatus `xorm:"TINYINT(4) comment('0: init, 1:success, 2:failed')"`
	StartTime  uint32                 `xorm:"not null INT(10)"`
	CreateTime uint32                 `xorm:"not null INT(10) created"`
	UpdateTime uint32                 `xorm:"not null INT(10) updated"`
}

type AsyncJobDao struct {
	session DB
}

func NewAsyncJobDao(session DB) AsyncJobDao {
	return AsyncJobDao{session: session}
}

func (dao AsyncJobDao) tabName() string {
	return "async_job_tab"
}

func (dao AsyncJobDao) AddJob(tab AsyncJobTab) (int64, error) {
	query := "INSERT INTO async_job_tab (params, job_type, job_status, start_time, create_time, update_time) VALUES (?, ?, ?, ?, ?, ?)"
	result, err := dao.session.Exec(query, tab.Params, tab.JobType, tab.JobStatus, tab.StartTime, tab.CreateTime, tab.UpdateTime)
	if err != nil {
		return 0, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return affected, nil
}

func (dao AsyncJobDao) UpdateJob(tab AsyncJobTab) (int64, error) {
	query := fmt.Sprintf("UPDATE %s SET job_status = ?, update_time = ? WHERE id = ?", dao.tabName())
	result, err := dao.session.Exec(query, tab.JobStatus, tab.UpdateTime, tab.Id)
	if err != nil {
		return 0, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return affected, nil
}

func (dao AsyncJobDao) FindUnhandledJobByMaxId(maxId uint64, firstPullDay int, skipIdStrList []string, limit int) ([]AsyncJobTab, error) {
	res := make([]AsyncJobTab, 0)
	var query string
	var args []interface{}

	if maxId == 0 && firstPullDay != -1 {
		startTime := time.Now().Add(time.Duration(-24*firstPullDay) * time.Hour).Unix()
		query = fmt.Sprintf("SELECT * FROM %s WHERE job_status = ? AND create_time >= ? ORDER BY id ASC LIMIT ?", dao.tabName())
		args = []interface{}{job_constant.JobStatusInit, startTime, limit}
	} else {
		query = fmt.Sprintf("SELECT * FROM %s WHERE id > ?", dao.tabName())
		args = []interface{}{maxId}

		if len(skipIdStrList) != 0 {
			query += fmt.Sprintf(" OR id IN (%s)", strings.Join(skipIdStrList, ","))
		}

		query += " ORDER BY id ASC LIMIT ?"
		args = append(args, limit)
	}

	rows, err := dao.session.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var job AsyncJobTab
		err = rows.Scan(&job.Id, &job.Params, &job.JobType, &job.JobStatus, &job.StartTime, &job.CreateTime, &job.UpdateTime) // 根据实际表结构修改扫描的字段
		if err != nil {
			return nil, err
		}
		res = append(res, job)
	}

	return res, nil
}

func (dao AsyncJobDao) GetMaxId() (uint64, error) {
	var res uint64
	query := fmt.Sprintf("SELECT MAX(id) FROM %s", dao.tabName())
	rows, err := dao.session.Query(query)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	if rows.Next() {
		err := rows.Scan(&res)
		if err != nil {
			return 0, err
		}
	}
	return res, nil
}
