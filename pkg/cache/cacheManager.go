package cache

import (
	sll "github.com/emirpasic/gods/lists/singlylinkedlist"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	gca "github.com/patrickmn/go-cache"
	"log"
	"strconv"
	"sync"
	"time"
)

type ChangeType uint8

const (
	ChangeType_Unchange ChangeType = 0
	ChangeType_Create   ChangeType = 1
	ChangeType_Update   ChangeType = 2
	ChangeType_Delete   ChangeType = 3
)

type ChangeMeta struct {
	Type         ChangeType
	FieldChanges map[int32]*ChangeMeta
	MapChange    map[interface{}]interface{}
	MapString    map[string]*ChangeMeta
	MapBool      map[bool]*ChangeMeta
	MapInt64     map[int64]*ChangeMeta
	MapInt32     map[int32]*ChangeMeta
}

type cacheItem struct {
	c             *gca.Cache
	latestVersion int64
	val           interface{}
	versions      *sll.List
	locker        *sync.RWMutex
	messageDesc   *desc.MessageDescriptor
	ttl           int64
}

type cacheManager struct {
	c *gca.Cache
}

func (m *cacheManager) GetNoVersionCache(id string, ttl int64) (bts []byte, hit bool) {
	cd, ext, hit := m.c.GetWithExpiration(id)
	if ext.Unix() < time.Now().Unix() {
		return nil, false
	}
	return cd.([]byte), hit
}

func (m *cacheManager) GetVersionCache(id string, version int64) (msg *dynamic.Message, changeDesc *ChangeMeta, newVersion int64, hit bool) {
	cd, hit := m.c.Get(id)
	if !hit {
		return nil, nil, 0, false
	}
	ci := cd.(*cacheItem)
	now := time.Now().Unix()
	if ci.latestVersion+ci.ttl < now {
		return nil, nil, 0, false
	}
	if ci.val == nil {
		return nil, &ChangeMeta{Type: ChangeType_Delete}, ci.latestVersion, true
	}
	cursorIdx := ci.versions.IndexOf(version)
	if cursorIdx < 0 {
		return cd.(*cacheItem).val.(*dynamic.Message), &ChangeMeta{Type: ChangeType_Create}, ci.latestVersion, true
	}
	resultData := dynamic.NewMessage(ci.messageDesc)
	resultChange := &ChangeMeta{Type: ChangeType_Unchange}
	for i := cursorIdx + 1; i < ci.versions.Size(); i++ {
		version, ok := ci.versions.Get(i)
		if !ok {
			log.Panic("缓存和版本索引不匹配")
		}
		cacheChange, ok := ci.c.Get(strconv.FormatInt(version.(int64), 10))
		change := cacheChange.(*ChangeMeta)
		if !ok {
			log.Panic("缓存和版本索引不匹配")
		}
		if change.Type == ChangeType_Unchange {
			continue
		}
		if change.Type == ChangeType_Create ||
			change.Type == ChangeType_Delete {
			resultData = ci.val.(*dynamic.Message)
			if resultData == nil {
				resultChange = &ChangeMeta{Type: ChangeType_Delete}
			} else {
				resultChange = &ChangeMeta{Type: ChangeType_Create}
			}
			break
		}
		getIncrementalMessage(ci.val.(*dynamic.Message), resultData, change)
		getIncrementalChange(resultChange, change)
	}
	return resultData, resultChange, ci.latestVersion, true
}

func (m *cacheManager) SetNoVersionCache(id string, data []byte, ttl int64) {
	m.c.Set(id, data, time.Duration(ttl)*time.Millisecond)
}

func (m *cacheManager) SetVersionCache(id string, version int64, data *dynamic.Message, messageDesc *desc.MessageDescriptor, ttl int64) {
	ci, hit := m.c.Get(id)
	if !hit {
		ci = &cacheItem{
			c:             gca.New(-1, time.Minute*1),
			latestVersion: version,
			versions:      sll.New(),
			val:           data,
			locker:        new(sync.RWMutex),
			messageDesc:   messageDesc,
			ttl:           ttl / 1000,
		}
		ci.(*cacheItem).c.OnEvicted(func(s string, i interface{}) {
			versions := ci.(*cacheItem).versions
			versions.Remove(versions.IndexOf(s))
		})
		change := &ChangeMeta{}
		if data == nil {
			change.Type = ChangeType_Delete
		} else {
			change.Type = ChangeType_Create
		}
		ci.(*cacheItem).versions.Add(version)
		ci.(*cacheItem).c.Set(strconv.FormatInt(version, 10), change, 0)
		m.c.Set(id, ci, 0)
	} else {
		if ci.(*cacheItem).val == nil && data == nil {
			ci.(*cacheItem).latestVersion = version
			ci.(*cacheItem).versions.Add(version)
			ci.(*cacheItem).c.Add(strconv.FormatInt(version, 10), &ChangeMeta{Type: ChangeType_Unchange}, 0)
		} else if ci.(*cacheItem).val == nil && data != nil {
			ci.(*cacheItem).val = data
			ci.(*cacheItem).latestVersion = version
			ci.(*cacheItem).versions.Add(version)
			ci.(*cacheItem).c.Add(strconv.FormatInt(version, 10), &ChangeMeta{Type: ChangeType_Create}, 0)
		} else if ci.(*cacheItem).val != nil && data == nil {
			ci.(*cacheItem).c.Set(strconv.FormatInt(version, 10), &ChangeMeta{Type: ChangeType_Delete}, 0)
			ci.(*cacheItem).versions.Add(version)
			ci.(*cacheItem).latestVersion = version
			ci.(*cacheItem).val = nil
		} else {
			fullMessage := ci.(*cacheItem).val.(*dynamic.Message)
			changeDesc := mergeAndDiffMessage(fullMessage, data)
			ci.(*cacheItem).c.Set(strconv.FormatInt(version, 10), changeDesc, 0)
			ci.(*cacheItem).versions.Add(version)
			ci.(*cacheItem).latestVersion = version
		}
	}
}

var CacheManager = &cacheManager{
	c: gca.New(time.Millisecond*0, time.Minute*1),
}
