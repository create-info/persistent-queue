package shedlock

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"git.garena.com/xiaomin.xu/shark-async-job/queue"
	"git.garena.com/xiaomin.xu/shark-async-job/queue_job"
	"net"
	"os"
	"strings"
	"time"
)

type Lock struct {
	session       queue_job.DB
	dao           shedLockDao
	tab           ShedlockTab
	minLockSecond uint32
	log           queue.Log

	failedExtendChan chan struct{}
	stopWatchChan    chan struct{}
}

type Config struct {
	// DbSession db master session
	DB queue_job.DB
	// ServiceName 服务名
	ServiceName string
	// LockSecond 锁时间，单位s
	LockSecond uint32
	// MinLockSecond 最小锁时间，单位s，默认为 LockSecond>5:LockSecond-5 / LockSecond<5:LockSecond-1
	MinLockSecond uint32
}

var (
	sessionError     = errors.New("bad session")
	serviceNameError = errors.New("empty service name")
	lockSecondError  = errors.New("lock second less than 1")
	unlockError      = errors.New("unlock error")
)

func NewShedLock(config Config, logger queue.Logger) (*Lock, error) {
	if config.DB == nil {
		return nil, sessionError
	}
	//if err := config.DbSession.Ping(); err != nil {
	//	return nil, sessionError
	//}
	if config.ServiceName == "" {
		return nil, serviceNameError
	}
	if config.LockSecond <= 1 {
		return nil, lockSecondError
	}
	minLockSecond := config.MinLockSecond
	if minLockSecond == 0 {
		if config.LockSecond <= 5 {
			minLockSecond = config.LockSecond - 1
		} else {
			minLockSecond = config.LockSecond - 5
		}
	}

	ip, err := getClientIp()
	if err != nil {
		return nil, err
	}
	podName, _ := os.LookupEnv("POD_NAME")

	h := md5.New()
	h.Write([]byte(config.ServiceName))
	name := hex.EncodeToString(h.Sum(nil))
	return &Lock{
		session: config.DB,
		dao:     newShedLockDao(config.DB),
		tab: ShedlockTab{
			LockName:   name,
			LockedBy:   fmt.Sprintf("%s-%s-%s-%d", config.ServiceName, ip, podName, time.Now().UnixNano()),
			LockSecond: config.LockSecond,
		},
		minLockSecond: config.MinLockSecond,
		log:           queue.Log{L: logger},
	}, nil
}

func (l *Lock) initLock(ctx context.Context) bool {
	// 初始化，insert
	affected, err := l.dao.initShedLock(l.tab)
	if err == nil && affected == 1 {
		// 初始化成功
		l.log.Info(ctx, "shelock initLock success")
		return true
	} else if strings.Contains(err.Error(), "Duplicate entry") && affected == 0 {
		// 锁已存在
		l.log.Info(ctx, "shelock initLock has exist")
		return true
	}
	l.log.Error(ctx, "shelock initLock error:%+v, affected:%d", err, affected)
	return false
}

// TryLock 尝试性抢锁
func (l *Lock) TryLock(ctx context.Context) bool {
	if !l.initLock(ctx) {
		return false
	}

	// insert失败，尝试update
	affected, err := l.dao.tryShedLock(l.tab)
	if err != nil || affected != 1 {
		l.log.Info(ctx, "shedlock TryLock failed, err:%+v, affected:%d, lockedBy:%s", err, affected, l.tab.LockedBy)
		return false
	}

	l.log.Info(ctx, "shedlock TryLock success, lockedBy:%s", l.tab.LockedBy)
	l.failedExtendChan = make(chan struct{})
	l.stopWatchChan = make(chan struct{})
	go l.watchShedLock()
	return true
}

// Lock 阻塞性抢锁，默认每轮睡5s
func (l *Lock) Lock(ctx context.Context, trySecond uint32) {
	if !l.initLock(ctx) {
		return
	}

	for {
		affected, err := l.dao.tryShedLock(l.tab)
		if err == nil && affected == 1 {
			l.log.Info(ctx, "shedlock Lock success, lockedBy:%s", l.tab.LockedBy)
			l.failedExtendChan = make(chan struct{})
			l.stopWatchChan = make(chan struct{})
			go l.watchShedLock()
			return
		} else {
			l.log.Info(ctx, "shedlock Lock failed, err:%+v, affected:%d, lockedBy:%s", err, affected, l.tab.LockedBy)
		}

		if trySecond != 0 {
			time.Sleep(time.Duration(trySecond) * time.Second)
		} else {
			time.Sleep(5 * time.Second)
		}
	}
}

// LockWithTimeout timeout时间内抢锁
func (l *Lock) LockWithTimeout(ctx context.Context, trySecond uint32, timeoutSecond uint32) bool {
	if timeoutSecond == 0 {
		return false
	}

	if !l.initLock(ctx) {
		return false
	}

	timeout := time.NewTicker(time.Duration(timeoutSecond) * time.Second)
	for {
		select {
		case <-timeout.C:
			return false
		default:
			affected, err := l.dao.tryShedLock(l.tab)
			if err == nil && affected == 1 {
				l.log.Info(ctx, "shedlock LockWithTimeout success, lockedBy:%s", l.tab.LockedBy)
				l.failedExtendChan = make(chan struct{})
				l.stopWatchChan = make(chan struct{})
				go l.watchShedLock()
				return true
			} else {
				l.log.Info(ctx, "shedlock LockWithTimeout failed, err:%+v, affected:%d, lockedBy:%s", err, affected, l.tab.LockedBy)
			}
			if trySecond != 0 {
				time.Sleep(time.Duration(trySecond) * time.Second)
			}
		}
	}
}

func (l *Lock) watchShedLock() {
	var extendSecond uint32
	if l.tab.LockSecond <= 5 {
		extendSecond = l.tab.LockSecond - 1
	} else {
		extendSecond = l.tab.LockSecond - 5
	}

	t := time.NewTicker(time.Duration(extendSecond) * time.Second)
	for {
		select {
		case <-t.C:
			affected, err := l.dao.extendShedLock(l.tab)
			if err != nil || affected != 1 {
				// 续期失败
				l.log.Error(context.TODO(), "shedlock extendShedLock error:%+v, affected:%d", err, affected)
				close(l.failedExtendChan)
				return
			} else {
				l.log.Info(context.TODO(), "shedlock extendShedLock success")
			}
		case <-l.stopWatchChan:
			return
		}
	}
}

func (l *Lock) ExtendFailed() chan struct{} {
	return l.failedExtendChan
}

// Unlock 解锁，needCloseSession=true则会关闭db session
func (l *Lock) Unlock(ctx context.Context, needCloseSession bool) error {
	if l.stopWatchChan != nil {
		l.stopWatchChan <- struct{}{}

		affected, err := l.dao.releaseShedLock(l.tab, l.minLockSecond)
		if err != nil || affected != 1 {
			l.log.Error(ctx, "shedlock releaseShedLock error:%+v, affected:%d", err, affected)
			return unlockError
		}
		if needCloseSession {
			err = l.session.Close()
			if err != nil {
				l.log.Error(ctx, "shedlock session close error:%+v", err)
				return err
			}
		}
		l.log.Info(ctx, "shedlock unlock success, lockedBy:%s", l.tab.LockedBy)
	}
	return nil
}

func getClientIp() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}

	for _, address := range addrs {
		// 检查ip地址判断是否回环地址
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String(), nil
			}
		}
	}

	return "", errors.New("can not find the client ip address")
}
