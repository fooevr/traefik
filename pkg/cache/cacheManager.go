package cache

import (
	sll "github.com/emirpasic/gods/lists/singlylinkedlist"
	"github.com/jhump/protoreflect/dynamic"
	gca "github.com/patrickmn/go-cache"
	"log"
	"strconv"
	"sync"
	"time"
)

type cacheItem struct {
	c             *gca.Cache
	latestVersion int64
	val           interface{}
	versions      *sll.List
	locker        *sync.RWMutex
}

type cacheManager struct {
	c *gca.Cache
}

func (m *cacheManager) GetNoVersionCache(id string, ttl int64) (bts []byte, hit bool) {
	cd, ext, hit := m.c.GetWithExpiration(id)
	if ext.Unix() < time.Now().Unix() {
		return nil, false
	}
	return cd.(*cacheItem).val.([]byte), hit
}

func (m *cacheManager) GetVersionCache(id string, version int64, ttl int64) (msg *dynamic.Message, changeDesc *change, hit bool) {
	cd, hit := m.c.Get(id)
	if !hit {
		return nil, nil, false
	}
	now := time.Now().Unix()
	if cd.(*cacheItem).latestVersion+ttl < now {
		return nil, nil, false
	}
	cursorIdx := cd.(*cacheItem).versions.IndexOf(version)
	if cursorIdx < 0 {
		return cd.(*cacheItem).val.(*dynamic.Message), &change{}, true
	}
	resultData := dynamic.NewMessage(cd.(*cacheItem).val.(*dynamic.Message).GetMessageDescriptor())
	resultChange := &change{changeType: ChangeType_NoChange}
	for i := cursorIdx; i < cd.(*cacheItem).versions.Size(); i++ {
		version, ok := cd.(*cacheItem).versions.Get(i)
		if !ok {
			log.Panic("缓存和版本索引不匹配")
		}
		cacheChange, ok := cd.(*cacheItem).c.Get(strconv.FormatInt(version.(int64), 10))
		if !ok {
			log.Panic("缓存和版本索引不匹配")
		}
		versionChange := cacheChange.(*change)
		if versionChange.changeType == ChangeType_Create {
			return cd.(*cacheItem).val.(*dynamic.Message), &change{changeType: ChangeType_Create}, true
		}
		changePartMessage(cd.(*cacheItem).val.(*dynamic.Message), resultData, versionChange, resultChange)
	}
	return resultData, resultChange, true
}

func (m *cacheManager) SetNoVersionCache(id string, data []byte, ttl int64) {
	m.c.Set(id, data, time.Duration(ttl)*time.Second)
}

func (m *cacheManager) SetVersionCache(id string, version int64, data *dynamic.Message, ttl int64) {
	ci, hit := m.c.Get(id)
	if !hit {
		ci = &cacheItem{
			c:             gca.New(-1, time.Minute*1),
			latestVersion: version,
			versions:      sll.New(),
			val:           data,
			locker:        new(sync.RWMutex),
		}
		ci.(*cacheItem).c.OnEvicted(func(s string, i interface{}) {
			versions := ci.(*cacheItem).versions
			versions.Remove(versions.IndexOf(s))
		})
		m.c.Set(id, ci, 0)
	} else {
		fullMessage := ci.(*cacheItem).val.(*dynamic.Message)
		if fullMessage == nil && data != nil {
			ci.(*cacheItem).val = data
		} else {
			change, changeDesc := mergeMessage(fullMessage, data)
			if !change {
				ci.(*cacheItem).c.Set(strconv.FormatInt(version, 10), nil, time.Millisecond*time.Duration(ttl))
			} else {
				ci.(*cacheItem).c.Set(strconv.FormatInt(version, 10), changeDesc, 0)
			}
		}
		ci.(*cacheItem).versions.Add(version)
		ci.(*cacheItem).latestVersion = version
	}
}

var CacheManager = &cacheManager{
	c: gca.New(time.Millisecond*0, time.Minute*1),
}
