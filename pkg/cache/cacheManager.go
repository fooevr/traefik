package cache

import (
	"fmt"
	com_variflight_middleware_gateway_cache "github.com/containous/traefik/v2/pkg/cache/proto"
	sll "github.com/emirpasic/gods/lists/singlylinkedlist"
	"github.com/imroc/biu"
	"github.com/jhump/protoreflect/desc"
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
	messageDesc   *desc.MessageDescriptor
	ttl           int64
}

type cacheManager struct {
	c *gca.Cache
}

type ChangeType int8

const (
	ChangeType_UnChange ChangeType = 0b00000000
	ChangeType_Create   ChangeType = 0b01000000
	ChangeType_Update   ChangeType = 0b10000000
	ChangeType_Delete   ChangeType = 0b11000000
)

func (m *cacheManager) GetNoVersionCache(id string, ttl int64) (bts []byte, hit bool) {
	cd, ext, hit := m.c.GetWithExpiration(id)
	if ext.Unix() < time.Now().Unix() {
		return nil, false
	}
	return cd.([]byte), hit
}

func (m *cacheManager) GetVersionCache(id string, version int64) (msg *dynamic.Message, changeDesc *com_variflight_middleware_gateway_cache.ChangeMeta, newVersion int64, hit bool) {
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
		return nil, &com_variflight_middleware_gateway_cache.ChangeMeta{Type: com_variflight_middleware_gateway_cache.ChangeMeta_deleted}, ci.latestVersion, true
	}
	cursorIdx := ci.versions.IndexOf(version)
	if cursorIdx < 0 {
		return cd.(*cacheItem).val.(*dynamic.Message), &com_variflight_middleware_gateway_cache.ChangeMeta{Type: com_variflight_middleware_gateway_cache.ChangeMeta_created}, ci.latestVersion, true
	}
	resultData := dynamic.NewMessage(ci.messageDesc)
	resultChange := &com_variflight_middleware_gateway_cache.ChangeMeta{Type: com_variflight_middleware_gateway_cache.ChangeMeta_unchanged}
	for i := cursorIdx + 1; i < ci.versions.Size(); i++ {
		version, ok := ci.versions.Get(i)
		if !ok {
			log.Panic("缓存和版本索引不匹配")
		}
		cacheChange, ok := ci.c.Get(strconv.FormatInt(version.(int64), 10))
		change := cacheChange.(*com_variflight_middleware_gateway_cache.ChangeMeta)
		if !ok {
			log.Panic("缓存和版本索引不匹配")
		}
		if change.Type == com_variflight_middleware_gateway_cache.ChangeMeta_unchanged {
			continue
		}
		if change.Type == com_variflight_middleware_gateway_cache.ChangeMeta_created ||
			change.Type == com_variflight_middleware_gateway_cache.ChangeMeta_deleted {
			resultData = ci.val.(*dynamic.Message)
			if resultData == nil {
				resultChange = &com_variflight_middleware_gateway_cache.ChangeMeta{Type: com_variflight_middleware_gateway_cache.ChangeMeta_deleted}
			} else {
				resultChange = &com_variflight_middleware_gateway_cache.ChangeMeta{Type: com_variflight_middleware_gateway_cache.ChangeMeta_created}
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
		change := &com_variflight_middleware_gateway_cache.ChangeMeta{}
		if data == nil {
			change.Type = com_variflight_middleware_gateway_cache.ChangeMeta_deleted
		} else {
			change.Type = com_variflight_middleware_gateway_cache.ChangeMeta_created
		}
		ci.(*cacheItem).versions.Add(version)
		ci.(*cacheItem).c.Set(strconv.FormatInt(version, 10), change, 0)
		m.c.Set(id, ci, 0)
	} else {
		if ci.(*cacheItem).val == nil && data == nil {
			ci.(*cacheItem).latestVersion = version
			ci.(*cacheItem).versions.Add(version)
			ci.(*cacheItem).c.Add(strconv.FormatInt(version, 10), &com_variflight_middleware_gateway_cache.ChangeMeta{Type: com_variflight_middleware_gateway_cache.ChangeMeta_unchanged}, 0)
		} else if ci.(*cacheItem).val == nil && data != nil {
			ci.(*cacheItem).val = data
			ci.(*cacheItem).latestVersion = version
			ci.(*cacheItem).versions.Add(version)
			ci.(*cacheItem).c.Add(strconv.FormatInt(version, 10), &com_variflight_middleware_gateway_cache.ChangeMeta{Type: com_variflight_middleware_gateway_cache.ChangeMeta_created}, 0)
		} else if ci.(*cacheItem).val != nil && data == nil {
			ci.(*cacheItem).c.Set(strconv.FormatInt(version, 10), &com_variflight_middleware_gateway_cache.ChangeMeta{Type: com_variflight_middleware_gateway_cache.ChangeMeta_deleted}, 0)
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

func setField(fieldDescriptor *desc.FieldDescriptor, messageDescriptor *desc.MessageDescriptor, ct ChangeType, fieldCM *com_variflight_middleware_gateway_cache.ChangeMeta, messageCM *com_variflight_middleware_gateway_cache.ChangeMeta) {
	var indexOfMsgFields int
	for idx, field := range messageDescriptor.GetFields() {
		if field.GetNumber() == fieldDescriptor.GetNumber() {
			indexOfMsgFields = idx
		}
	}
	byteIndex := indexOfMsgFields / 4
	bitIndex := (indexOfMsgFields % 4) * 2
	fieldArrIndex := 0
	fieldTagIndex := 0
	for i := 0; i <= len(messageCM.FieldTags); i++ {
		if fieldTagIndex >= indexOfMsgFields {
			break
		}
		for j := 0; j < 4; j++ {
			if fieldTagIndex >= indexOfMsgFields {
				break
			}
			if messageCM.FieldTags[i]<<(j*2)>>(6-j*2) != 0b00 {
				fieldArrIndex++
			}
			fieldTagIndex++
		}
	}

	bt := messageCM.FieldTags[byteIndex]
	// clean the set changeTag
	bt = bt&^(0b11000000>>bitIndex) | (ct >> bitIndex)
	messageCM.FieldTags[bitIndex] = bt
	temp := append([]*com_variflight_middleware_gateway_cache.ChangeMeta{}, messageCM.Fields[fieldArrIndex:]...)
	messageCM.Fields = append(append(messageCM.Fields[:fieldArrIndex], fieldCM), temp...)
}
