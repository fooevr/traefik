package cache

import (
	com_variflight_middleware_gateway_cache "github.com/containous/traefik/v2/pkg/cache/proto"
	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"log"
	"reflect"
)

func mergeAndDiffMessage(oldMessage, incrMessage *dynamic.Message) *com_variflight_middleware_gateway_cache.ChangeMeta {
	change := &com_variflight_middleware_gateway_cache.ChangeMeta{Type: com_variflight_middleware_gateway_cache.ChangeMeta_unchanged}
	for _, field := range oldMessage.GetMessageDescriptor().GetFields() {
		// create
		if checkNoFieldOrNil(field, oldMessage) && !checkNoFieldOrNil(field, incrMessage) {
			change.Fields[field.GetNumber()] = &com_variflight_middleware_gateway_cache.ChangeMeta{Type: com_variflight_middleware_gateway_cache.ChangeMeta_created}
			continue
		}
		// delete
		if !checkNoFieldOrNil(field, oldMessage) && checkNoFieldOrNil(field, incrMessage) {
			change.Fields[field.GetNumber()] = &com_variflight_middleware_gateway_cache.ChangeMeta{Type: com_variflight_middleware_gateway_cache.ChangeMeta_deleted}
			continue
		}

		if field.IsMap() {
			mc := mergeAndDiffMap(oldMessage, incrMessage, field)
			if mc.Type != com_variflight_middleware_gateway_cache.ChangeMeta_unchanged {
				change.Fields[field.GetNumber()] = mc
			}
		} else if field.IsRepeated() {
			rc := mergeAndDiffRepeated(oldMessage, incrMessage, field)
			if rc.Type != com_variflight_middleware_gateway_cache.ChangeMeta_unchanged {
				change.Fields[field.GetNumber()] = rc
			}
		} else if field.GetMessageType() != nil {
			mc := mergeAndDiffMessage(oldMessage, incrMessage)
			if mc.Type != com_variflight_middleware_gateway_cache.ChangeMeta_unchanged {
				change.Fields[field.GetNumber()] = mc
			}
		} else {
			ov := oldMessage.GetField(field)
			nv := incrMessage.GetField(field)
			if ov != nv {
				change.Fields[field.GetNumber()] = &com_variflight_middleware_gateway_cache.ChangeMeta{Type: com_variflight_middleware_gateway_cache.ChangeMeta_updated}
			}
			oldMessage.SetField(field, nv)
		}
	}
	if len(change.Fields) > 0 {
		change.Type = com_variflight_middleware_gateway_cache.ChangeMeta_updated
	}
	return change
}

func mergeAndDiffMap(oldMessage, incrMessage *dynamic.Message, mapField *desc.FieldDescriptor) *com_variflight_middleware_gateway_cache.ChangeMeta {
	result := &com_variflight_middleware_gateway_cache.ChangeMeta{Type: com_variflight_middleware_gateway_cache.ChangeMeta_unchanged}
	oldMessage.ForEachMapFieldEntry(mapField, func(key, val interface{}) bool {
		var c *com_variflight_middleware_gateway_cache.ChangeMeta
		if nv, err := incrMessage.TryGetMapField(mapField, key); err != nil {
			// deleted
			oldMessage.RemoveMapField(mapField, key)
			c = &com_variflight_middleware_gateway_cache.ChangeMeta{Type: com_variflight_middleware_gateway_cache.ChangeMeta_deleted}
			return true
		} else {
			// updated
			if mapField.GetMapValueType().GetMessageType() != nil {
				c = mergeAndDiffMessage(val.(*dynamic.Message), nv.(*dynamic.Message))
			} else {
				if nv != val {
					oldMessage.PutMapField(mapField, key, nv)
					c = &com_variflight_middleware_gateway_cache.ChangeMeta{Type: com_variflight_middleware_gateway_cache.ChangeMeta_updated}
				}
			}
		}
		if c != nil && c.Type != com_variflight_middleware_gateway_cache.ChangeMeta_unchanged {
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
		if nv, err := oldMessage.TryGetMapField(mapField, key); err != nil {
			c := &com_variflight_middleware_gateway_cache.ChangeMeta{Type: com_variflight_middleware_gateway_cache.ChangeMeta_created}
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
			oldMessage.PutMapField(mapField, key, nv)
		}
		return true
	})
	if len(result.MapBool) > 0 || len(result.MapInt32) > 0 || len(result.MapInt64) > 0 || len(result.MapString) > 0 {
		result.Type = com_variflight_middleware_gateway_cache.ChangeMeta_updated
	}
	return result
}

func mergeAndDiffRepeated(oldMessage, incrMessage *dynamic.Message, repeatedField *desc.FieldDescriptor) *com_variflight_middleware_gateway_cache.ChangeMeta {
	result := &com_variflight_middleware_gateway_cache.ChangeMeta{Type: com_variflight_middleware_gateway_cache.ChangeMeta_unchanged}
	ov := oldMessage.GetField(repeatedField).([]interface{})
	nv := incrMessage.GetField(repeatedField).([]interface{})
	if len(ov) != len(nv) {
		result.Type = com_variflight_middleware_gateway_cache.ChangeMeta_updated
	} else {
		for idx, ovItem := range ov {
			nvItem := nv[idx]
			if repeatedField.GetMessageType() != nil {
				c := mergeAndDiffMessage(ovItem.(*dynamic.Message), nvItem.(*dynamic.Message))
				if c.Type != com_variflight_middleware_gateway_cache.ChangeMeta_unchanged {
					result.Type = com_variflight_middleware_gateway_cache.ChangeMeta_updated
					break
				}
			} else {
				if ovItem != nvItem {
					result.Type = com_variflight_middleware_gateway_cache.ChangeMeta_updated
					break
				}
			}
		}
	}
	return result
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
	for fieldIdx, fieldChange := range change.Fields {
		field := fullMessage.GetMessageDescriptor().FindFieldByNumber(fieldIdx)
		if fieldChange.Type == com_variflight_middleware_gateway_cache.ChangeMeta_created {
			incrementalMessage.SetField(field, fullMessage.GetField(field))
		} else if fieldChange.Type == com_variflight_middleware_gateway_cache.ChangeMeta_deleted {
			incrementalMessage.ClearField(field)
		}
		if field.IsMap() {
			getIncrementalMap(fullMessage, incrementalMessage, field, fieldChange)
		} else if field.IsRepeated() {
			incrementalMessage.SetField(field, fullMessage.GetField(field))
		} else if field.GetMessageType() != nil {
			getIncrementalMessage(fullMessage.GetField(field).(*dynamic.Message), incrementalMessage.GetField(field).(*dynamic.Message), fieldChange)
		} else {
			incrementalMessage.SetField(field, fullMessage.GetField(field))
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
		if change.Type == com_variflight_middleware_gateway_cache.ChangeMeta_created {
			incrMessage.PutMapField(field, key, fullMessage.GetMapField(field, key))
		} else if change.Type == com_variflight_middleware_gateway_cache.ChangeMeta_deleted {
			incrMessage.RemoveMapField(field, key)
		} else if change.Type == com_variflight_middleware_gateway_cache.ChangeMeta_updated {
			if field.GetMapValueType().GetMessageType() != nil {
				getIncrementalMessage(fullMessage.GetMapField(field, key).(*dynamic.Message), incrMessage.GetMapField(field, key).(*dynamic.Message), change)
			} else {
				incrMessage.PutMapField(field, key, fullMessage.GetMapField(field, key))
			}
		}
	}
}
