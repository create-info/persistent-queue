package shedlock

import (
	"github.com/create-info/persistent-queue/queue_job"
)

type ShedlockTab struct {
	LockName   string `xorm:"VARCHAR(64) pk not null"`
	LockUntil  uint32 `xorm:"TIMESTAMP not null"`
	LockedAt   uint32 `xorm:"TIMESTAMP not null"`
	LockedBy   string `xorm:"VARCHAR(255) not null"`
	LockSecond uint32 `xorm:"-"` // 锁生存时间，不作数据库映射
}

type shedLockDao struct {
	session queue_job.DB
}

func newShedLockDao(session queue_job.DB) shedLockDao {
	return shedLockDao{
		session: session,
	}
}

// initShedLock 只设name，其他为默认
func (dao shedLockDao) initShedLock(tab ShedlockTab) (int64, error) {
	sqlStr := "INSERT INTO shedlock_tab(lock_name, lock_until, locked_at, locked_by) " +
		"VALUES(?, NOW(), NOW(), ?)"
	res, err := dao.session.Exec(sqlStr, tab.LockName, tab.LockedBy)
	if err != nil {
		return 0, err
	}
	affected, err := res.RowsAffected()
	return affected, err
}

func (dao shedLockDao) tryShedLock(tab ShedlockTab) (int64, error) {
	sqlStr := "UPDATE shedlock_tab " +
		"SET lock_until = ADDDATE(NOW(), interval ? second), locked_at = NOW(), locked_by = ? " +
		"WHERE lock_name = ? AND lock_until <= NOW()"
	res, err := dao.session.Exec(sqlStr, tab.LockSecond, tab.LockedBy, tab.LockName)
	if err != nil {
		return 0, err
	}
	affected, err := res.RowsAffected()
	return affected, err
}

func (dao shedLockDao) extendShedLock(tab ShedlockTab) (int64, error) {
	sqlStr := "UPDATE shedlock_tab " +
		"SET lock_until = ADDDATE(NOW(), interval ? second), locked_at = NOW() " +
		"WHERE lock_name = ? AND lock_until > NOW() AND locked_by = ?"
	res, err := dao.session.Exec(sqlStr, tab.LockSecond, tab.LockName, tab.LockedBy)
	if err != nil {
		return 0, err
	}
	affected, err := res.RowsAffected()
	return affected, err
}

func (dao shedLockDao) releaseShedLock(tab ShedlockTab, minLockSecond uint32) (int64, error) {
	sqlStr := "UPDATE shedlock_tab " +
		"SET lock_until = IF(ADDDATE(locked_at, interval ? second) >= NOW(), adddate(locked_at, interval ? second), NOW()) " +
		"WHERE lock_name = ? "
	res, err := dao.session.Exec(sqlStr, minLockSecond, minLockSecond, tab.LockName)
	if err != nil {
		return 0, err
	}
	affected, err := res.RowsAffected()
	return affected, err
}
