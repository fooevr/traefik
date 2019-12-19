package cache

//
//import (
//	"github.com/jhump/protoreflect/desc"
//	"github.com/jhump/protoreflect/dynamic"
//	"log"
//	"reflect"
//)
//
//type changeType int
//
//const (
//	ChangeType_Create   = 0
//	ChangeType_Updated  = 1
//	ChangeType_Delete   = 2
//	ChangeType_NoChange = 2
//)
//
//type change struct {
//	messageFieldChanges  map[int32]*change
//	mapFieldChanges      map[interface{}]interface{}
//	repeatedFieldChanges map[interface{}]interface{}
//	changeType           changeType
//}
//
//func mergeMessage(oldMessage, newMessage *dynamic.Message) (changed bool, changeDesc *change) {
//	changeDesc = &change{messageFieldChanges: map[int32]*change{}}
//	for _, field := range oldMessage.GetMessageDescriptor().GetFields() {
//		// check if both old and new message field not hasfield or nil
//		if checkNoFieldOrNil(field, oldMessage) && checkNoFieldOrNil(field, newMessage) {
//			continue
//		}
//		// check if delete field
//		if !checkNoFieldOrNil(field, oldMessage) && checkNoFieldOrNil(field, newMessage) {
//			changeDesc.messageFieldChanges[field.GetNumber()] = &change{changeType: ChangeType_Updated}
//			oldMessage.ClearField(field)
//			continue
//		}
//		// check if create field
//		if checkNoFieldOrNil(field, oldMessage) && !checkNoFieldOrNil(field, newMessage) {
//			changeDesc.messageFieldChanges[field.GetNumber()] = &change{changeType: ChangeType_Create}
//			oldMessage.SetField(field, newMessage.GetField(field))
//			continue
//		}
//
//		if field.IsMap() {
//			b, change := mergeMap(oldMessage, newMessage, field)
//			if b {
//				changeDesc.messageFieldChanges[field.GetNumber()] = change
//			}
//		} else if field.IsRepeated() {
//			b, change := mergeRepeated(oldMessage, newMessage, field)
//			if b {
//				changeDesc.messageFieldChanges[field.GetNumber()] = change
//			}
//		} else if field.GetMessageType() != nil {
//			b, change := mergeMessage(oldMessage, newMessage)
//			if b {
//				changeDesc.messageFieldChanges[field.GetNumber()] = change
//			}
//		} else {
//			oldValue := oldMessage.GetField(field)
//			newValue := newMessage.GetField(field)
//			if oldValue != newValue {
//				oldMessage.SetField(field, newValue)
//				changeDesc.messageFieldChanges[field.GetNumber()] = &change{changeType: ChangeType_Updated}
//			}
//		}
//	}
//	return len(changeDesc.messageFieldChanges) > 0, changeDesc
//}
//
//func mergeMap(oldMessage, newMessage *dynamic.Message, mapField *desc.FieldDescriptor) (changed bool, changeDesc *change) {
//	changeDesc = &change{mapFieldChanges: map[interface{}]interface{}{}}
//	oldMessage.ForEachMapFieldEntry(mapField, func(key, val interface{}) bool {
//		// if delete
//		newValue, err := newMessage.TryGetMapField(mapField, key)
//		if err != nil {
//			changeDesc.mapFieldChanges[key] = &change{changeType: ChangeType_Delete}
//			oldMessage.RemoveMapField(mapField, key)
//			return true
//		}
//		// if updated
//		if mapField.GetMessageType() != nil {
//			b, change := mergeMessage(val.(*dynamic.Message), newValue.(*dynamic.Message))
//			if b {
//				changeDesc.mapFieldChanges[key] = change
//			}
//		} else {
//			if val != newValue {
//				oldMessage.PutMapField(mapField, key, newValue)
//				changeDesc.mapFieldChanges[key] = &change{changeType: ChangeType_Updated}
//			}
//		}
//
//		return true
//	})
//	newMessage.ForEachMapFieldEntry(mapField, func(key, val interface{}) bool {
//		_, err := oldMessage.TryGetMapField(mapField, key)
//		if err != nil {
//			changeDesc.mapFieldChanges[key] = &change{changeType: ChangeType_Create}
//			oldMessage.PutMapField(mapField, key, val)
//		}
//		return true
//	})
//	return len(changeDesc.mapFieldChanges) > 0, changeDesc
//}
//
//func mergeRepeated(oldMessage, newMessage *dynamic.Message, repeatedField *desc.FieldDescriptor) (changed bool, changeDesc *change) {
//	// TODO: 数组版本化支持
//	oldV := oldMessage.GetField(repeatedField).([]interface{})
//	newV := newMessage.GetField(repeatedField).([]interface{})
//	if len(oldV) != len(newV) {
//		return true, nil
//	}
//	for idx, oldItem := range oldV {
//		newItem := newV[idx]
//		if repeatedField.GetMessageType() != nil {
//			b, _ := mergeMessage(newItem.(*dynamic.Message), oldItem.(*dynamic.Message))
//			if b {
//				return true, nil
//			}
//		} else if newItem != oldItem {
//			return true, nil
//		}
//	}
//	return false, nil
//}
//
//func checkNoFieldOrNil1(field *desc.FieldDescriptor, message *dynamic.Message) bool {
//	if !message.HasField(field) {
//		return true
//	}
//	v := message.GetField(field)
//	return v == nil || ((reflect.ValueOf(v).Kind() == reflect.Ptr ||
//		reflect.ValueOf(v).Kind() == reflect.Map ||
//		reflect.ValueOf(v).Kind() == reflect.Slice) && reflect.ValueOf(v).IsNil())
//}
//
//func changePartMessage(fullMessage *dynamic.Message, targetMessage *dynamic.Message, changeDesc *change, resultDesc *change) (data *dynamic.Message, incrChange *change) {
//	for fieldIndex, change := range changeDesc.messageFieldChanges {
//		field := fullMessage.GetMessageDescriptor().FindFieldByNumber(fieldIndex)
//		if field == nil {
//			log.Panic("未获取到缓存中的列索引")
//		}
//		if field.IsMap() {
//			changePartMap(fullMessage.GetField(field).(*dynamic.Message), targetMessage.GetField(field).(*dynamic.Message), change)
//		} else if field.IsRepeated() {
//			changePartRepeated(fullMessage.GetField(field).(*dynamic.Message), targetMessage.GetField(field).(*dynamic.Message), change)
//		} else if field.GetMessageType() != nil {
//			changePartMessage(fullMessage.GetField(field).(*dynamic.Message), targetMessage.GetField(field).(*dynamic.Message), change)
//		} else {
//			if change.changeType == ChangeType_Create || change.changeType == ChangeType_Updated {
//				targetMessage.SetField(field, fullMessage.GetField(field))
//			} else if change.changeType == ChangeType_Delete {
//				targetMessage.ClearField(field)
//			}
//		}
//	}
//	return targetMessage
//}
//
//func changePartRepeated(message *dynamic.Message, message2 *dynamic.Message, c *change) {
//
//}
//
//func changePartMap(message *dynamic.Message, message2 *dynamic.Message, c *change) {
//
//}
