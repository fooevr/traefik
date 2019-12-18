package cache

import (
	"github.com/jhump/protoreflect/dynamic"
	gca "github.com/patrickmn/go-cache"
	"sync"
	"time"
)

type cacheItem struct {
	c       *gca.Cache
	version int64
	val     interface{}
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

func (m *cacheManager) GetVersionCache(id string, version int64, ttl int64) (msg *dynamic.Message, change *Change, hit bool) {
	cd, hit := m.c.Get(id)
	if !hit {
		return nil, nil, false
	}
	now := time.Now().Unix()
	if cd.(*cacheItem).version+ttl < now {
		return nil, nil, false
	}
	return nil, nil, false
}

func (m *cacheManager) SetNoVersionCache(id string, data []byte, ttl int64) {
	m.c.Set(id, data, time.Duration(ttl)*time.Second)
}

func (m *cacheManager) SetVersionCache(id string) {

}

var CacheManager = &cacheManager{
	c: gca.New(time.Millisecond*0, time.Minute*1),
}
