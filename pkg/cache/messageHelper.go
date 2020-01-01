package cache

import (
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"log"
	"reflect"
	"strings"
)

func mergeAndDiffMessage(oldMessage, incrMessage *dynamic.Message) *ChangeMeta {
	change := &ChangeMeta{Type: ChangeType_Unchange, FieldChanges: map[int32]*ChangeMeta{}}
	for _, field := range oldMessage.GetMessageDescriptor().GetFields() {
		if checkNoFieldOrNil(field, oldMessage) && checkNoFieldOrNil(field, incrMessage) {
			continue
		}
		// create
		if checkNoFieldOrNil(field, oldMessage) && !checkNoFieldOrNil(field, incrMessage) {
			oldMessage.SetField(field, incrMessage.GetField(field))
			change.FieldChanges[field.GetNumber()] = &ChangeMeta{Type: ChangeType_Create}
			continue
		}
		// delete
		if !checkNoFieldOrNil(field, oldMessage) && checkNoFieldOrNil(field, incrMessage) {
			oldMessage.ClearField(field)
			change.FieldChanges[field.GetNumber()] = &ChangeMeta{Type: ChangeType_Delete}
			continue
		}

		if field.IsMap() {
			mc := mergeAndDiffMap(oldMessage, incrMessage, field)
			if mc.Type != ChangeType_Unchange {
				change.FieldChanges[field.GetNumber()] = mc
			}
		} else if field.IsRepeated() {
			rc := mergeAndDiffRepeated(oldMessage, incrMessage, field)
			if rc.Type != ChangeType_Unchange {
				change.FieldChanges[field.GetNumber()] = rc
			}
		} else if field.GetMessageType() != nil && !strings.HasPrefix(field.GetMessageType().GetFullyQualifiedName(), "google.protobuf.") {
			mc := mergeAndDiffMessage(oldMessage.GetField(field).(*dynamic.Message), incrMessage.GetField(field).(*dynamic.Message))
			if mc.Type != ChangeType_Unchange {
				change.FieldChanges[field.GetNumber()] = mc
			}
		} else {
			diff := false
			if field.GetMessageType() != nil && strings.HasPrefix(field.GetMessageType().GetFullyQualifiedName(), "google.protobuf.") {
				diff = !proto.Equal(oldMessage.GetField(field).(proto.Message), incrMessage.GetField(field).(proto.Message))
			} else {
				ov := oldMessage.GetField(field)
				nv := incrMessage.GetField(field)
				diff = ov != nv
			}
			if diff {
				change.FieldChanges[field.GetNumber()] = &ChangeMeta{Type: ChangeType_Update}
				oldMessage.SetField(field, incrMessage.GetField(field))
			}
		}
	}
	if len(change.FieldChanges) > 0 {
		change.Type = ChangeType_Update
	} else {
		change.FieldChanges = nil
	}
	return change
}

func mergeAndDiffMap(oldMessage, incrMessage *dynamic.Message, mapField *desc.FieldDescriptor) *ChangeMeta {
	result := &ChangeMeta{
		Type:      ChangeType_Unchange,
		MapString: map[string]*ChangeMeta{},
		MapInt32:  map[int32]*ChangeMeta{},
		MapInt64:  map[int64]*ChangeMeta{},
		MapBool:   map[bool]*ChangeMeta{},
	}
	oldMessage.ForEachMapFieldEntry(mapField, func(key, val interface{}) bool {
		var c *ChangeMeta
		if nv, ok := incrMessage.GetField(mapField).(map[interface{}]interface{})[key]; !ok {
			// deleted
			oldMessage.RemoveMapField(mapField, key)
			c = &ChangeMeta{Type: ChangeType_Delete}
		} else {
			// updated
			if mapField.GetMapValueType().GetMessageType() != nil {
				c = mergeAndDiffMessage(val.(*dynamic.Message), nv.(*dynamic.Message))
			} else {
				if nv != val {
					oldMessage.PutMapField(mapField, key, nv)
					c = &ChangeMeta{Type: ChangeType_Update}
				}
			}
		}
		if c != nil && c.Type != ChangeType_Unchange {
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
			c := &ChangeMeta{Type: ChangeType_Create}
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
	if len(result.MapBool) > 0 || len(result.MapInt32) > 0 || len(result.MapInt64) > 0 || len(result.MapString) > 0 {
		result.Type = ChangeType_Update
	}
	if len(result.MapBool) == 0 {
		result.MapBool = nil
	}
	if len(result.MapInt64) == 0 {
		result.MapInt64 = nil
	}
	if len(result.MapInt32) == 0 {
		result.MapInt32 = nil
	}
	if len(result.MapString) == 0 {
		result.MapString = nil
	}
	return result
}

func mergeAndDiffRepeated(oldMessage, incrMessage *dynamic.Message, repeatedField *desc.FieldDescriptor) *ChangeMeta {
	result := &ChangeMeta{Type: ChangeType_Unchange}
	ov := oldMessage.GetField(repeatedField).([]interface{})
	nv := incrMessage.GetField(repeatedField).([]interface{})
	if len(ov) != len(nv) {
		result.Type = ChangeType_Update
	} else {
		for idx, ovItem := range ov {
			nvItem := nv[idx]
			if repeatedField.GetMessageType() != nil {
				c := mergeAndDiffMessage(ovItem.(*dynamic.Message), nvItem.(*dynamic.Message))
				if c.Type != ChangeType_Unchange {
					result.Type = ChangeType_Update
					break
				}
			} else {
				if ovItem != nvItem {
					result.Type = ChangeType_Update
					break
				}
			}
		}
	}
	if result.Type == ChangeType_Update {
		oldMessage.SetField(repeatedField, incrMessage.GetField(repeatedField))
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

func getIncrementalMessage(fullMessage, incrementalMessage *dynamic.Message, change *ChangeMeta) {
	for fieldIdx, fieldChange := range change.FieldChanges {
		field := fullMessage.GetMessageDescriptor().FindFieldByNumber(fieldIdx)
		if fieldChange.Type == ChangeType_Create {
			incrementalMessage.SetField(field, fullMessage.GetField(field))
			continue
		} else if fieldChange.Type == ChangeType_Delete {
			incrementalMessage.ClearField(field)
			continue
		}
		if field.IsMap() {
			getIncrementalMap(fullMessage, incrementalMessage, field, fieldChange)
		} else if field.IsRepeated() {
			incrementalMessage.SetField(field, fullMessage.GetField(field))
		} else if field.GetMessageType() == nil || strings.HasPrefix(field.GetMessageType().GetFullyQualifiedName(), "google.protobuf.") {
			incrementalMessage.SetField(field, fullMessage.GetField(field))
		} else {
			var msg = incrementalMessage.GetField(field).(*dynamic.Message)
			if msg == nil {
				msg = dynamic.NewMessage(field.GetMessageType())
			}
			getIncrementalMessage(fullMessage.GetField(field).(*dynamic.Message), msg, fieldChange)
			if bts, _ := msg.Marshal(); len(bts) > 0 {
				incrementalMessage.SetField(field, msg)
			}
		}
	}
}

func getIncrementalMap(fullMessage *dynamic.Message, incrMessage *dynamic.Message, field *desc.FieldDescriptor, fieldChange *ChangeMeta) {
	mapChange := map[interface{}]*ChangeMeta{}
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
		if change.Type == ChangeType_Create {
			incrMessage.PutMapField(field, key, fullMessage.GetMapField(field, key))
		} else if change.Type == ChangeType_Delete {
			incrMessage.RemoveMapField(field, key)
		} else if change.Type == ChangeType_Update {
			if field.GetMapValueType().GetMessageType() != nil {
				var incrMapValue = incrMessage.GetMapField(field, key)
				if incrMessage.GetMapField(field, key) == nil {
					incrMapValue = dynamic.NewMessage(field.GetMapValueType().GetMessageType())
				}
				getIncrementalMessage(fullMessage.GetMapField(field, key).(*dynamic.Message), incrMapValue.(*dynamic.Message), change)
				if bts, _ := incrMapValue.(*dynamic.Message).Marshal(); len(bts) > 0 {
					incrMessage.PutMapField(field, key, incrMapValue)
				}
			} else {
				incrMessage.PutMapField(field, key, fullMessage.GetMapField(field, key))
			}
		}
	}
	if !checkNoFieldOrNil(field, incrMessage) {
		if len(incrMessage.GetField(field).(map[interface{}]interface{})) == 0 {
			incrMessage.ClearField(field)
		}
	}
}

func getIncrementalChange(source, increment *ChangeMeta) {
	if increment.Type == ChangeType_Unchange {
		return
	}
	if increment.Type == ChangeType_Create || increment.Type == ChangeType_Delete {
		source.FieldChanges = nil
		source.MapBool = nil
		source.MapInt32 = nil
		source.MapInt64 = nil
		source.MapString = nil
		source.Type = increment.Type
		return
	}
	source.Type = ChangeType_Update
	if increment.FieldChanges != nil {
		for fieldIdx, change := range increment.FieldChanges {
			if oldChange, exists := source.FieldChanges[fieldIdx]; exists {
				getIncrementalChange(oldChange, change)
			} else {
				if source.FieldChanges == nil {
					source.FieldChanges = map[int32]*ChangeMeta{}
				}
				source.FieldChanges[fieldIdx] = change
			}
		}
	}
	if increment.MapBool != nil {
		for fieldIdx, change := range increment.MapBool {
			if oldChange, exists := source.MapBool[fieldIdx]; exists {
				getIncrementalChange(oldChange, change)
			} else {
				if source.MapBool == nil {
					source.MapBool = map[bool]*ChangeMeta{}
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
					source.MapInt32 = map[int32]*ChangeMeta{}
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
					source.MapInt64 = map[int64]*ChangeMeta{}
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
					source.MapString = map[string]*ChangeMeta{}
				}
				source.MapString[fieldIdx] = change
			}
		}
	}
}
