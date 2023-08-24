package persistent_queue

import (
	"github.com/google/btree"
	"time"
)

type SkipInfo struct {
	id   uint64
	time uint64
}

func (a SkipInfo) Less(b btree.Item) bool {
	return a.time < b.(SkipInfo).time
}

func (a SkipInfo) CanDrop() bool {
	now := uint64(time.Now().Add(-5 * time.Minute).UnixNano())
	return a.time <= now
}

func NewSkipInfo(id uint64) SkipInfo {
	return SkipInfo{
		id:   id,
		time: uint64(time.Now().UnixNano()),
	}
}
