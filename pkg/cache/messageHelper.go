package cache

import (
	com_variflight_middleware_gateway_cache "github.com/containous/traefik/v2/pkg/cache/proto"
	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"log"
	"math"
	"reflect"
)

func mergeAndDiffMessage(oldMessage, incrMessage *dynamic.Message) *com_variflight_middleware_gateway_cache.ChangeMeta {
	change := &com_variflight_middleware_gateway_cache.ChangeMeta{}
	for _, field := range oldMessage.GetMessageDescriptor().GetFields() {
		if checkNoFieldOrNil(field, oldMessage) && checkNoFieldOrNil(field, incrMessage) {
			continue
		}
		// create
		if checkNoFieldOrNil(field, oldMessage) && !checkNoFieldOrNil(field, incrMessage) {
			oldMessage.SetField(field, incrMessage.GetField(field))
			setField(field, oldMessage.GetMessageDescriptor(), ChangeType_Create, nil, change)
			continue
		}
		// delete
		if !checkNoFieldOrNil(field, oldMessage) && checkNoFieldOrNil(field, incrMessage) {
			oldMessage.ClearField(field)
			setField(field, oldMessage.GetMessageDescriptor(), ChangeType_Delete, nil, change)
			continue
		}

		if field.IsMap() {
			mc := mergeAndDiffMap(oldMessage, incrMessage, field)
			if len(mc.MapString) > 0 || len(mc.MapInt64) > 0 || len(mc.MapInt32) > 0 || len(mc.MapBool) > 0 {
				setField(field, oldMessage.GetMessageDescriptor(), ChangeType_Update, mc, change)
			}
		} else if field.IsRepeated() {
			rc := mergeAndDiffRepeated(oldMessage, incrMessage, field)
			if rc {
				setField(field, oldMessage.GetMessageDescriptor(), ChangeType_Update, nil, change)
			}
		} else if field.GetMessageType() != nil {
			mc := mergeAndDiffMessage(oldMessage.GetField(field).(*dynamic.Message), incrMessage.GetField(field).(*dynamic.Message))
			if len(mc.Fields) > 0 {
				setField(field, oldMessage.GetMessageDescriptor(), ChangeType_Update, mc, change)
			}
		} else {
			ov := oldMessage.GetField(field)
			nv := incrMessage.GetField(field)
			if ov != nv {
				setField(field, oldMessage.GetMessageDescriptor(), ChangeType_Update, nil, change)
			}
		}
	}
	return change
}

func mergeAndDiffMap(oldMessage, incrMessage *dynamic.Message, mapField *desc.FieldDescriptor) *com_variflight_middleware_gateway_cache.ChangeMeta {
	result := &com_variflight_middleware_gateway_cache.ChangeMeta{
		MapString: map[string]*com_variflight_middleware_gateway_cache.ChangeMeta{},
		MapInt32:  map[int32]*com_variflight_middleware_gateway_cache.ChangeMeta{},
		MapInt64:  map[int64]*com_variflight_middleware_gateway_cache.ChangeMeta{},
		MapBool:   map[bool]*com_variflight_middleware_gateway_cache.ChangeMeta{},
	}
	oldMessage.ForEachMapFieldEntry(mapField, func(key, val interface{}) bool {
		var c *com_variflight_middleware_gateway_cache.ChangeMeta
		if nv, ok := incrMessage.GetField(mapField).(map[interface{}]interface{})[key]; !ok {
			// deleted
			oldMessage.RemoveMapField(mapField, key)
			c = &com_variflight_middleware_gateway_cache.ChangeMeta{Deleted: true}
		} else {
			// updated
			if mapField.GetMapValueType().GetMessageType() != nil {
				c = mergeAndDiffMessage(val.(*dynamic.Message), nv.(*dynamic.Message))
				if len(c.Fields) == 0 {
					c = nil
				}
			} else {
				if nv != val {
					oldMessage.PutMapField(mapField, key, nv)
					c = &com_variflight_middleware_gateway_cache.ChangeMeta{}
				}
			}
		}
		if c != nil {
			if mapField.GetMapKeyType().GetType() == descriptor.FieldDescriptorProto_TYPE_INT32 {
				result.MapInt32[key.(int32)] = c
			} else if mapField.GetMapKeyType().GetType() == descriptor.FieldDescriptorProto_TYPE_INT64 {
				result.MapInt64[key.(int64)] = c
			} else if mapField.GetMapKeyType().GetType() == descriptor.FieldDescriptorProto_TYPE_BOOL {
				result.MapBool[key.(bool)] = c
			} else if mapField.GetMapKeyType().GetType() == descriptor.FieldDescriptorProto_TYPE_STRING {
				result.MapString[key.(string)] = c
			} else {
				log.Panicf("unsupport map key type:%s", mapField.GetMapKeyType().GetType())
			}
		}
		return true
	})

	incrMessage.ForEachMapFieldEntry(mapField, func(key, val interface{}) bool {
		//created
		if _, ok := oldMessage.GetField(mapField).(map[interface{}]interface{})[key]; !ok {
			c := &com_variflight_middleware_gateway_cache.ChangeMeta{Created: true}
			if mapField.GetMapKeyType().GetType() == descriptor.FieldDescriptorProto_TYPE_INT32 {
				result.MapInt32[key.(int32)] = c
			} else if mapField.GetMapKeyType().GetType() == descriptor.FieldDescriptorProto_TYPE_INT64 {
				result.MapInt64[key.(int64)] = c
			} else if mapField.GetMapKeyType().GetType() == descriptor.FieldDescriptorProto_TYPE_BOOL {
				result.MapBool[key.(bool)] = c
			} else if mapField.GetMapKeyType().GetType() == descriptor.FieldDescriptorProto_TYPE_STRING {
				result.MapString[key.(string)] = c
			} else {
				log.Panicf("unsupport map key type:%s", mapField.GetMapKeyType().GetType())
			}
			oldMessage.PutMapField(mapField, key, val)
		}
		return true
	})
	return result
}

func mergeAndDiffRepeated(oldMessage, incrMessage *dynamic.Message, repeatedField *desc.FieldDescriptor) bool {
	ov := oldMessage.GetField(repeatedField).([]interface{})
	nv := incrMessage.GetField(repeatedField).([]interface{})
	if len(ov) != len(nv) {
		return true
	} else {
		for idx, ovItem := range ov {
			nvItem := nv[idx]
			if repeatedField.GetMessageType() != nil {
				c := mergeAndDiffMessage(ovItem.(*dynamic.Message), nvItem.(*dynamic.Message))
				if checkMessageUpdated(c.FieldTags) {
					return true
				}
			} else {
				if ovItem != nvItem {
					return true
				}
			}
		}
	}
	return false
}

func checkNoFieldOrNil(field *desc.FieldDescriptor, message *dynamic.Message) bool {
	if !message.HasField(field) {
		return true
	}
	v := message.GetField(field)
	return v == nil || ((reflect.ValueOf(v).Kind() == reflect.Ptr ||
		reflect.ValueOf(v).Kind() == reflect.Map ||
		reflect.ValueOf(v).Kind() == reflect.Slice) && reflect.ValueOf(v).IsNil())
}

func getIncrementalMessage(fullMessage, incrementalMessage *dynamic.Message, change *com_variflight_middleware_gateway_cache.ChangeMeta) {
	curFiledIndex := 0
	for _, item := range change.FieldTags {
		for i := 0; i < 4; i++ {
			ct := item << (i * 2) & 0b11000000
			field := fullMessage.GetMessageDescriptor().GetFields()[curFiledIndex]
			ct, fieldChange := getField(field, fullMessage.GetMessageDescriptor(), change)
			// TODO: 验证desc中的field是否按序排列
			if ct == ChangeType_Create {
				fullMessage.SetField(field, incrementalMessage.GetField(field))
			} else if ct == ChangeType_Delete {
				fullMessage.ClearField(field)
			} else if ct == ChangeType_Update {
				if field.IsMap() {
					getIncrementalMap(fullMessage, incrementalMessage, field, fieldChange)
				} else if field.IsRepeated() {
					fullMessage.SetField(field, incrementalMessage.GetField(field))
				} else if field.GetMessageType() != nil {
					getIncrementalMessage(fullMessage.GetField(field).(*dynamic.Message), incrementalMessage.GetField(field).(*dynamic.Message), fieldChange)
				} else {
					fullMessage.SetField(field, incrementalMessage.GetField(field))
				}
			}
			curFiledIndex++
		}
	}
}

func getIncrementalMap(fullMessage *dynamic.Message, incrMessage *dynamic.Message, field *desc.FieldDescriptor, fieldChange *com_variflight_middleware_gateway_cache.ChangeMeta) {
	mapChange := map[interface{}]*com_variflight_middleware_gateway_cache.ChangeMeta{}
	if field.GetMapKeyType().GetType() == descriptor.FieldDescriptorProto_TYPE_INT32 {
		for k, v := range fieldChange.MapInt32 {
			mapChange[k] = v
		}
	} else if field.GetMapKeyType().GetType() == descriptor.FieldDescriptorProto_TYPE_INT64 {
		for k, v := range fieldChange.MapInt64 {
			mapChange[k] = v
		}
	} else if field.GetMapKeyType().GetType() == descriptor.FieldDescriptorProto_TYPE_BOOL {
		for k, v := range fieldChange.MapBool {
			mapChange[k] = v
		}
	} else if field.GetMapKeyType().GetType() == descriptor.FieldDescriptorProto_TYPE_STRING {
		for k, v := range fieldChange.MapString {
			mapChange[k] = v
		}
	} else {
		log.Panicf("can't increment message, cause unsupport map key type:%s", field.GetMapKeyType().GetType())
	}
	for key, change := range mapChange {
		if change.Created {
			incrMessage.PutMapField(field, key, fullMessage.GetMapField(field, key))
		} else if change.Deleted {
			incrMessage.RemoveMapField(field, key)
		} else if !change.Unchanged {
			if field.GetMapValueType().GetMessageType() != nil {
				getIncrementalMessage(fullMessage.GetMapField(field, key).(*dynamic.Message), incrMessage.GetMapField(field, key).(*dynamic.Message), change)
			} else {
				incrMessage.PutMapField(field, key, fullMessage.GetMapField(field, key))
			}
		}
	}
}

func getIncrementalChange(source, increment *com_variflight_middleware_gateway_cache.ChangeMeta) {
	if increment.Type == com_variflight_middleware_gateway_cache.ChangeMeta_unchanged {
		return
	}
	if increment.Type == com_variflight_middleware_gateway_cache.ChangeMeta_created || increment.Type == com_variflight_middleware_gateway_cache.ChangeMeta_deleted {
		source.Fields = nil
		source.MapBool = nil
		source.MapInt32 = nil
		source.MapInt64 = nil
		source.MapString = nil
		source.Type = increment.Type
		return
	}
	source.Type = com_variflight_middleware_gateway_cache.ChangeMeta_updated
	if increment.Fields != nil {
		for fieldIdx, change := range increment.Fields {
			if oldChange, exists := source.Fields[fieldIdx]; exists {
				getIncrementalChange(oldChange, change)
			} else {
				if source.Fields == nil {
					source.Fields = map[int32]*com_variflight_middleware_gateway_cache.ChangeMeta{}
				}
				source.Fields[fieldIdx] = change
			}
		}
	}
	if increment.MapBool != nil {
		for fieldIdx, change := range increment.MapBool {
			if oldChange, exists := source.MapBool[fieldIdx]; exists {
				getIncrementalChange(oldChange, change)
			} else {
				if source.MapBool == nil {
					source.MapBool = map[bool]*com_variflight_middleware_gateway_cache.ChangeMeta{}
				}
				source.MapBool[fieldIdx] = change
			}
		}
	}
	if increment.MapInt32 != nil {
		for fieldIdx, change := range increment.MapInt32 {
			if oldChange, exists := source.MapInt32[fieldIdx]; exists {
				getIncrementalChange(oldChange, change)
			} else {
				if source.MapInt32 == nil {
					source.MapInt32 = map[int32]*com_variflight_middleware_gateway_cache.ChangeMeta{}
				}
				source.MapInt32[fieldIdx] = change
			}
		}
	}
	if increment.MapInt64 != nil {
		for fieldIdx, change := range increment.MapInt64 {
			if oldChange, exists := source.MapInt64[fieldIdx]; exists {
				getIncrementalChange(oldChange, change)
			} else {
				if source.MapInt64 == nil {
					source.MapInt64 = map[int64]*com_variflight_middleware_gateway_cache.ChangeMeta{}
				}
				source.MapInt64[fieldIdx] = change
			}
		}
	}
	if increment.MapString != nil {
		for fieldIdx, change := range increment.MapString {
			if oldChange, exists := source.MapString[fieldIdx]; exists {
				getIncrementalChange(oldChange, change)
			} else {
				if source.MapString == nil {
					source.MapString = map[string]*com_variflight_middleware_gateway_cache.ChangeMeta{}
				}
				source.MapString[fieldIdx] = change
			}
		}
	}
}

func setField(fieldDescriptor *desc.FieldDescriptor, messageDescriptor *desc.MessageDescriptor, ct ChangeType, fieldCM *com_variflight_middleware_gateway_cache.ChangeMeta, messageCM *com_variflight_middleware_gateway_cache.ChangeMeta) {
	if ct != ChangeType_UnChange {
		messageCM.Fields = []*com_variflight_middleware_gateway_cache.ChangeMeta{}
		messageCM.FieldTags = make([]byte, int32(math.Ceil(float64(len(messageDescriptor.GetFields()))/4)))
		messageCM.ChangeTags = make([]byte, int32(math.Ceil(float64(len(messageDescriptor.GetFields()))/8)))
	}
	var indexOfMsgFields int
	for idx, field := range messageDescriptor.GetFields() {
		if field.GetNumber() == fieldDescriptor.GetNumber() {
			indexOfMsgFields = idx
			break
		}
	}

	// 设置FieldTag
	byteIndex := indexOfMsgFields / 4
	bitIndex := (indexOfMsgFields % 4) * 2

	bt := messageCM.FieldTags[byteIndex]
	// clean then set changeTag
	bt = bt&^(0b11000000>>bitIndex) | (ct >> bitIndex)
	messageCM.FieldTags[bitIndex] = bt

	if fieldCM != nil {
		fieldArrIndex := 0
		comparedCount := 0
		for _, changeTag := range messageCM.ChangeTags {
			if comparedCount >= indexOfMsgFields {
				break
			}
			for i := 0; i < 8; i++ {
				if comparedCount >= indexOfMsgFields {
					break
				}
				if (changeTag>>(7-i))&0b1 == 1 {
					fieldArrIndex++
				}
				comparedCount++
			}
		}
		cTag := messageCM.ChangeTags[int(math.Ceil(float64(indexOfMsgFields)/8))]
		messageCM.ChangeTags[int(math.Ceil(float64(indexOfMsgFields)/8))] = cTag | (0b1 << (7 - (indexOfMsgFields % 8)))
		temp := append([]*com_variflight_middleware_gateway_cache.ChangeMeta{}, messageCM.Fields[fieldArrIndex:]...)
		messageCM.Fields = append(append(messageCM.Fields[:fieldArrIndex], fieldCM), temp...)
	}
}

func getField(field *desc.FieldDescriptor, messageDesc *desc.MessageDescriptor, messageChange *com_variflight_middleware_gateway_cache.ChangeMeta) (ct ChangeType, change *com_variflight_middleware_gateway_cache.ChangeMeta) {
	indexOfMsgFields := 0
	for idx, item := range messageDesc.GetFields() {
		if item.GetNumber() == field.GetNumber() {
			indexOfMsgFields = idx
			break
		}
	}
	ct = messageChange.FieldTags[int(math.Ceil(float64(indexOfMsgFields)/4))] << ((indexOfMsgFields % 4) * 2) & 0b11000000
	hasChangeMeta := messageChange.FieldTags[int(math.Ceil(float64(indexOfMsgFields)/8))]>>(7-indexOfMsgFields%8)&0b1 == 1
	if hasChangeMeta {
		loopCount := 0
		changeMetaIndex := 0
		for _, item := range messageChange.ChangeTags {
			if loopCount >= indexOfMsgFields {
				break
			}
			for i := 0; i < 8; i++ {
				if loopCount >= indexOfMsgFields {
					break
				}
				curHasChangeMeta := item>>(7-i)*0b1 == 1
				if curHasChangeMeta {
					changeMetaIndex++
				}
				loopCount++
			}
		}
		change = messageChange.Fields[changeMetaIndex]
		return
	} else {
		return
	}
}

func checkMessageUpdated(fieldTags []byte) bool {
	for _, item := range fieldTags {
		if item > 0 {
			return true
		}
	}
	return false
}
