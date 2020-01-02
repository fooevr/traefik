package cache

import (
	"github.com/containous/traefik/v2/pkg/cache/proto/sys"
	sll "github.com/emirpasic/gods/lists/singlylinkedlist"
	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	gca "github.com/patrickmn/go-cache"
	"log"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type ChangeType uint8

const (
	ChangeType_Unchange ChangeType = 0b00000000
	ChangeType_Create   ChangeType = 0b01000000
	ChangeType_Update   ChangeType = 0b10000000
	ChangeType_Delete   ChangeType = 0b11000000
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

func (m *cacheManager) GetVersionFullCache(id string) (msg *dynamic.Message, changeType ChangeType, newVersion int64, hit bool) {
	cd, hit := m.c.Get(id)
	if !hit {
		return nil, ChangeType_Unchange, 0, false
	}
	ci := cd.(*cacheItem)
	if ci.latestVersion == 0 {
		return nil, ChangeType_Unchange, 0, false
	}
	return ci.val.(*dynamic.Message), ChangeType_Create, ci.latestVersion, true
}

func (m *cacheManager) GetVersionCache(id string, version int64) (msg *dynamic.Message, ct ChangeType, changeDesc *dataservice.ChangeDesc, newVersion int64, hit bool) {
	cd, hit := m.c.Get(id)
	if !hit {
		return nil, ChangeType_Unchange, nil, 0, false
	}
	ci := cd.(*cacheItem)
	now := time.Now().Unix()
	if ci.latestVersion+ci.ttl < now {
		return nil, ChangeType_Unchange, nil, 0, false
	}
	if ci.val == nil {
		return nil, ChangeType_Delete, nil, ci.latestVersion, true
	}
	cursorIdx := ci.versions.IndexOf(version)
	if cursorIdx < 0 {
		return cd.(*cacheItem).val.(*dynamic.Message), ChangeType_Create, nil, ci.latestVersion, true
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
		resultChange.Type = ChangeType_Update
		getIncrementalMessage(ci.val.(*dynamic.Message), resultData, change)
		getIncrementalChange(resultChange, change)
	}
	return resultData, resultChange.Type, messageChangeMetaToProto(resultChange, ci.messageDesc), ci.latestVersion, true
}

func (m *cacheManager) SetNoVersionCache(id string, data []byte, ttl int64) {
	m.c.Set(id, data, time.Duration(ttl)*time.Millisecond)
}

func (m *cacheManager) SetVersionCache(id string, version int64, data *dynamic.Message, messageDesc *desc.MessageDescriptor, ttl int64, maxVersionCount int) {
	ci, hit := m.c.Get(id)
	if !hit {
		ci = &cacheItem{
			c:           gca.New(-1, time.Millisecond*time.Duration(ttl*int64(maxVersionCount))),
			versions:    sll.New(),
			locker:      new(sync.RWMutex),
			messageDesc: messageDesc,
			ttl:         ttl / 1000,
		}
		ci.(*cacheItem).c.OnEvicted(func(s string, i interface{}) {
			versions := ci.(*cacheItem).versions
			versions.Remove(versions.IndexOf(s))
		})
		m.c.Set(id, ci, 0)
	}

	if ci.(*cacheItem).val == nil && data == nil {
		ci.(*cacheItem).latestVersion = version
		ci.(*cacheItem).versions.Clear()
		ci.(*cacheItem).c.Flush()
		ci.(*cacheItem).versions.Add(version)
		ci.(*cacheItem).c.Add(strconv.FormatInt(version, 10), &ChangeMeta{Type: ChangeType_Unchange}, 0)
	} else if ci.(*cacheItem).val == nil && data != nil {
		ci.(*cacheItem).val = data
		ci.(*cacheItem).latestVersion = version
		ci.(*cacheItem).versions.Add(version)
		ci.(*cacheItem).c.Add(strconv.FormatInt(version, 10), &ChangeMeta{Type: ChangeType_Create}, 0)
	} else if ci.(*cacheItem).val != nil && data == nil {
		ci.(*cacheItem).latestVersion = version
		ci.(*cacheItem).val = nil
		ci.(*cacheItem).versions.Clear()
		ci.(*cacheItem).versions.Add(version)
		ci.(*cacheItem).c.Flush()
		ci.(*cacheItem).c.Add(strconv.FormatInt(version, 10), &ChangeMeta{Type: ChangeType_Delete}, 0)
	} else {
		fullMessage := ci.(*cacheItem).val.(*dynamic.Message)
		changeDesc := mergeAndDiffMessage(fullMessage, data)
		ci.(*cacheItem).c.Add(strconv.FormatInt(version, 10), changeDesc, 0)
		ci.(*cacheItem).versions.Add(version)
		ci.(*cacheItem).latestVersion = version
		ci.(*cacheItem).val = data
	}
}

var CacheManager = &cacheManager{
	c: gca.New(time.Millisecond*0, time.Minute*1),
}

// 将ChangeMeta映射未ChangeDesc proto.
// CM中的Field映射到CD中的FieldTags，CD.FTs中每2bit表示Message中一个Field的变更类型，按Message中field Number排序。
// 0b00 unchanged, 0b01 create, 0b10 update, 0b11 delete
// CD.CT中每1bit表示一个Field是否有对应的ChangeDesc,与CD.FT相同，按message中field的number排序。
// CD.FCD表示字段的变更描述数组，与CD.CT中的标示顺序对应。
func messageChangeMetaToProto(msgChange *ChangeMeta, messageDesc *desc.MessageDescriptor) *dataservice.ChangeDesc {
	if msgChange.FieldChanges == nil && len(msgChange.FieldChanges) == 0 {
		return nil
	}
	result := &dataservice.ChangeDesc{
		FieldTags:         make([]byte, int(math.Ceil(float64(len(messageDesc.GetFields()))/4))),
		ChangeTags:        make([]byte, int(math.Ceil(float64(len(messageDesc.GetFields()))/8))),
		FieldsChangeDescs: []*dataservice.ChangeDesc{},
	}
	fieldNumbers := []int{}
	for _, f := range messageDesc.GetFields() {
		fieldNumbers = append(fieldNumbers, int(f.GetNumber()))
	}
	sort.Ints(fieldNumbers)
	if msgChange.FieldChanges != nil {
		for num, change := range msgChange.FieldChanges {
			idxOfMessage := 0
			for idx, item := range fieldNumbers {
				if item == int(num) {
					idxOfMessage = idx
					break
				}
			}
			result.FieldTags[int(math.Floor(float64(idxOfMessage)/4))] =
				result.FieldTags[int(math.Floor(float64(idxOfMessage)/4))]&^(0b11000000>>((idxOfMessage%4)*2)) | (byte(change.Type) >> ((idxOfMessage % 4) * 2))
			if change.Type != ChangeType_Update {
				continue
			}
			fieldDesc := messageDesc.FindFieldByNumber(num)
			var fieldChange *dataservice.ChangeDesc
			if fieldDesc.IsMap() {
				fieldChange = mapChangeMetaToProto(change, fieldDesc)
			} else if fieldDesc.IsRepeated() {
				fieldChange = nil
			} else if fieldDesc.GetMessageType() != nil && !strings.HasPrefix(fieldDesc.GetMessageType().GetFullyQualifiedName(), "google.protobuf.") { // value message
				fieldChange = messageChangeMetaToProto(change, fieldDesc.GetMessageType())
			}
			if fieldChange != nil {
				result.ChangeTags[int(math.Floor(float64(idxOfMessage)/8))] = result.ChangeTags[int(math.Floor(float64(idxOfMessage)/8))] | (0b10000000 >> (idxOfMessage % 8))
				result.FieldsChangeDescs = append(result.FieldsChangeDescs, fieldChange)
			}
		}
	}
	return result
}

func mapChangeMetaToProto(change *ChangeMeta, mapField *desc.FieldDescriptor) *dataservice.ChangeDesc {
	result := &dataservice.ChangeDesc{}
	if mapField.GetMapKeyType().GetType() == descriptor.FieldDescriptorProto_TYPE_BOOL {
		for k, c := range change.MapBool {
			if result.MapBool == nil {
				result.MapBool = map[bool]*dataservice.ChangeDesc{}
			}
			if result.MapBoolRemoved == nil {
				result.MapBoolRemoved = map[bool]*dataservice.ChangeDesc{}
			}
			if mapField.GetMapValueType().GetMessageType() == nil {
				if c.Type == ChangeType_Delete {
					result.MapBoolRemoved[k] = nil
				} else {
					result.MapBool[k] = nil
				}
			} else {
				if c.Type == ChangeType_Delete {
					result.MapBoolRemoved[k] = nil
				} else if c.Type == ChangeType_Create {
					result.MapBool[k] = nil
				} else {
					result.MapBool[k] = messageChangeMetaToProto(c, mapField.GetMapValueType().GetMessageType())
				}
			}
		}
	} else if mapField.GetMapKeyType().GetType() == descriptor.FieldDescriptorProto_TYPE_INT32 {
		for k, c := range change.MapInt32 {
			if result.MapInt32 == nil {
				result.MapInt32 = map[int32]*dataservice.ChangeDesc{}
			}
			if result.MapInt32Removed == nil {
				result.MapInt32Removed = map[int32]*dataservice.ChangeDesc{}
			}
			if mapField.GetMapValueType().GetMessageType() == nil {
				if c.Type == ChangeType_Delete {
					result.MapInt32Removed[k] = nil
				} else {
					result.MapInt32[k] = nil
				}
			} else {
				if c.Type == ChangeType_Delete {
					result.MapInt32Removed[k] = nil
				} else if c.Type == ChangeType_Create {
					result.MapInt32[k] = nil
				} else {
					result.MapInt32[k] = messageChangeMetaToProto(c, mapField.GetMapValueType().GetMessageType())
				}
			}
		}
	} else if mapField.GetMapKeyType().GetType() == descriptor.FieldDescriptorProto_TYPE_INT64 {
		for k, c := range change.MapInt64 {
			if result.MapInt64 == nil {
				result.MapInt64 = map[int64]*dataservice.ChangeDesc{}
			}
			if result.MapInt64Removed == nil {
				result.MapInt64Removed = map[int64]*dataservice.ChangeDesc{}
			}
			if mapField.GetMapValueType().GetMessageType() == nil {
				if c.Type == ChangeType_Delete {
					result.MapInt64Removed[k] = nil
				} else {
					result.MapInt64[k] = nil
				}
			} else {
				if c.Type == ChangeType_Delete {
					result.MapInt64Removed[k] = nil
				} else if c.Type == ChangeType_Create {
					result.MapInt64[k] = nil
				} else {
					result.MapInt64[k] = messageChangeMetaToProto(c, mapField.GetMapValueType().GetMessageType())
				}
			}
		}
	} else if mapField.GetMapKeyType().GetType() == descriptor.FieldDescriptorProto_TYPE_STRING {
		for k, c := range change.MapString {
			if result.MapString == nil {
				result.MapString = map[string]*dataservice.ChangeDesc{}
			}
			if result.MapStringRemoved == nil {
				result.MapStringRemoved = map[string]*dataservice.ChangeDesc{}
			}
			if mapField.GetMapValueType().GetMessageType() == nil {
				if c.Type == ChangeType_Delete {
					result.MapStringRemoved[k] = nil
				} else {
					result.MapString[k] = nil
				}
			} else {
				if c.Type == ChangeType_Delete {
					result.MapStringRemoved[k] = nil
				} else if c.Type == ChangeType_Create {
					result.MapString[k] = nil
				} else {
					result.MapString[k] = messageChangeMetaToProto(c, mapField.GetMapValueType().GetMessageType())
				}
			}
		}
	}
	if len(result.MapBool) == 0 {
		result.MapBool = nil
	}
	if len(result.MapBoolRemoved) == 0 {
		result.MapBoolRemoved = nil
	}

	if len(result.MapInt32) == 0 {
		result.MapInt32 = nil
	}
	if len(result.MapInt32Removed) == 0 {
		result.MapInt32Removed = nil
	}

	if len(result.MapInt64) == 0 {
		result.MapInt64 = nil
	}
	if len(result.MapInt64Removed) == 0 {
		result.MapInt64Removed = nil
	}

	if len(result.MapString) == 0 {
		result.MapString = nil
	}
	if len(result.MapStringRemoved) == 0 {
		result.MapStringRemoved = nil
	}
	return result
}
