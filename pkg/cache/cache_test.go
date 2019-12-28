package cache

import (
	"encoding/base64"
	"fmt"
	test "github.com/containous/traefik/v2/pkg/cache/proto/declare"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/golang/protobuf/ptypes/wrappers"
	"github.com/imroc/biu"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	assert "github.com/stretchr/testify/assert"
	"strings"
	"testing"
	"time"
)

func TestVersionCache(t *testing.T) {
	empty := &test.TestMessage{}
	emptyD := toDynamic(empty)

	desc := emptyD.GetMessageDescriptor()
	v1 := time.Now().Unix()
	t.Run("创建消息", func(t *testing.T) {
		v1p := &test.TestMessage{}
		v1 = time.Now().Unix()
		d := toDynamic(v1p)
		CacheManager.SetVersionCache("a", v1, d, d.GetMessageDescriptor(), 300000)
		// assert create
		msg, ct, cd, nv, hit := CacheManager.GetVersionCache("a", 0)
		assert.True(t, hit, "命中")
		assert.Equal(t, ct, ChangeType_Create, "类型")
		assert.Nil(t, cd, "描述")
		assert.Equal(t, nv, v1, "版本")
		assert.True(t, dynamic.Equal(d, msg), "结果")
	})
	time.Sleep(time.Second * 1)
	t.Run("删除消息", func(t *testing.T) {
		v := time.Now().Unix()
		CacheManager.SetVersionCache("a", v, nil, desc, 300000)
		msg, ct, cd, nv, hit := CacheManager.GetVersionCache("a", 0)
		assert.True(t, hit, "命中")
		assert.Equal(t, ct, ChangeType_Delete, "类型")
		assert.Nil(t, cd, "描述")
		assert.Equal(t, nv, v, "版本")
		assert.Nil(t, msg, "结果")
	})
	time.Sleep(time.Second * 1)
	v2 := time.Now().Unix()
	t.Run("重建消息", func(t *testing.T) {
		m := &test.TestMessage{}
		d := toDynamic(m)
		v2 = time.Now().Unix()
		CacheManager.SetVersionCache("a", v2, d, desc, 300000)

		t.Run("叠加版本", func(t *testing.T) {
			msg, ct, cd, nv, hit := CacheManager.GetVersionCache("a", v1)
			assert.True(t, hit, "命中")
			assert.Equal(t, ct, ChangeType_Create, "类型")
			assert.Nil(t, cd, "描述")
			assert.Equal(t, nv, v2, "版本")
			assert.True(t, dynamic.Equal(msg, d), "结果")
		})

		t.Run("全量版本", func(t *testing.T) {
			msg, ct, cd, nv, hit := CacheManager.GetVersionCache("a", 0)
			assert.True(t, hit, "命中")
			assert.Equal(t, ct, ChangeType_Create, "类型")
			assert.Nil(t, cd, "描述")
			assert.Equal(t, nv, v2, "版本")
			assert.True(t, dynamic.Equal(msg, d), "结果")
		})
	})

	time.Sleep(time.Second * 1)
	t.Run("新增值字段", func(t *testing.T) {
		m := &test.TestMessage{Value: "foo"}
		d := toDynamic(m)
		v := time.Now().Unix()
		CacheManager.SetVersionCache("a", v, d, desc, 300000)

		t.Run("叠加版本", func(t *testing.T) {
			msg, ct, cd, nv, hit := CacheManager.GetVersionCache("a", v1)
			assert.True(t, hit, "命中")
			assert.Equal(t, ct, ChangeType_Create, "类型")
			assert.Nil(t, cd, "描述")
			assert.Equal(t, nv, v, "版本")
			assert.True(t, dynamic.Equal(msg, d), "结果")
		})

		t.Run("仅变更", func(t *testing.T) {
			msg, ct, cd, nv, hit := CacheManager.GetVersionCache("a", v2)
			assert.True(t, hit, "命中")
			assert.Equal(t, ChangeType_Update, ct, "类型")
			assert.NotNil(t, cd, "描述")
			assert.Equal(t, "[01000000 00000000]", biu.BytesToBinaryString(cd.FieldTags), "字段变更标记")
			assert.Equal(t, "[00000000]", biu.BytesToBinaryString(cd.ChangeTags), "字段描述标记")
			assert.Equal(t, 0, len(cd.FieldsChangeDescs), "描述")
			assert.Nil(t, cd.MapBool, "BoolMap")
			assert.Nil(t, cd.MapInt32, "Int32Map")
			assert.Nil(t, cd.MapInt64, "Int64Map")
			assert.Nil(t, cd.MapString, "StringMap")
			assert.Equal(t, nv, v, "版本")
			assert.True(t, dynamic.Equal(msg, d), "结果")
		})

		t.Run("全量版本", func(t *testing.T) {
			msg, ct, cd, nv, hit := CacheManager.GetVersionCache("a", 0)
			assert.True(t, hit, "命中")
			assert.Equal(t, ct, ChangeType_Create, "类型")
			assert.Nil(t, cd, "描述")
			assert.Equal(t, nv, v, "版本")
			assert.True(t, dynamic.Equal(msg, d), "结果")
		})
	})
	time.Sleep(time.Second * 1)

	t.Run("更新值字段", func(t *testing.T) {
		m := &test.TestMessage{Value: "bar"}
		d := toDynamic(m)
		v := time.Now().Unix()
		CacheManager.SetVersionCache("a", v, d, desc, 300000)

		t.Run("叠加版本", func(t *testing.T) {
			msg, ct, cd, nv, hit := CacheManager.GetVersionCache("a", v1)
			assert.True(t, hit, "命中")
			assert.Equal(t, ct, ChangeType_Create, "类型")
			assert.Nil(t, cd, "描述")
			assert.Equal(t, nv, v, "版本")
			assert.True(t, dynamic.Equal(msg, d), "结果")
		})

		t.Run("仅变更", func(t *testing.T) {
			msg, ct, cd, nv, hit := CacheManager.GetVersionCache("a", v2)
			assert.True(t, hit, "命中")
			assert.Equal(t, ChangeType_Update, ct, "类型")
			assert.NotNil(t, cd, "描述")
			assert.Equal(t, "[10000000 00000000]", biu.BytesToBinaryString(cd.FieldTags), "字段变更标记")
			assert.Equal(t, "[00000000]", biu.BytesToBinaryString(cd.ChangeTags), "字段描述标记")
			assert.Equal(t, 0, len(cd.FieldsChangeDescs), "描述")
			assert.Nil(t, cd.MapBool, "BoolMap")
			assert.Nil(t, cd.MapInt32, "Int32Map")
			assert.Nil(t, cd.MapInt64, "Int64Map")
			assert.Nil(t, cd.MapString, "StringMap")
			assert.Equal(t, nv, v, "版本")
			assert.Equal(t, "bar", msg.GetFieldByNumber(1).(string), "变更内容")
			assert.True(t, dynamic.Equal(msg, d), "结果")
		})

		t.Run("全量版本", func(t *testing.T) {
			msg, ct, cd, nv, hit := CacheManager.GetVersionCache("a", 0)
			assert.True(t, hit, "命中")
			assert.Equal(t, ct, ChangeType_Create, "类型")
			assert.Nil(t, cd, "描述")
			assert.Equal(t, nv, v, "版本")
			assert.True(t, dynamic.Equal(msg, d), "结果")
		})
	})
	time.Sleep(time.Second * 1)

	t.Run("删除值字段", func(t *testing.T) {
		m := &test.TestMessage{}
		d := toDynamic(m)
		v := time.Now().Unix()
		CacheManager.SetVersionCache("a", v, d, desc, 300000)

		t.Run("叠加版本", func(t *testing.T) {
			msg, ct, cd, nv, hit := CacheManager.GetVersionCache("a", v1)
			assert.True(t, hit, "命中")
			assert.Equal(t, ct, ChangeType_Create, "类型")
			assert.Nil(t, cd, "描述")
			assert.Equal(t, nv, v, "版本")
			assert.True(t, dynamic.Equal(msg, d), "结果")
		})

		t.Run("仅变更", func(t *testing.T) {
			msg, ct, cd, nv, hit := CacheManager.GetVersionCache("a", v2)
			assert.True(t, hit, "命中")
			assert.Equal(t, ChangeType_Update, ct, "类型")
			assert.NotNil(t, cd, "描述")
			assert.Equal(t, "[11000000 00000000]", biu.BytesToBinaryString(cd.FieldTags), "字段变更标记")
			assert.Equal(t, "[00000000]", biu.BytesToBinaryString(cd.ChangeTags), "字段描述标记")
			assert.Equal(t, 0, len(cd.FieldsChangeDescs), "描述")
			assert.Nil(t, cd.MapBool, "BoolMap")
			assert.Nil(t, cd.MapInt32, "Int32Map")
			assert.Nil(t, cd.MapInt64, "Int64Map")
			assert.Nil(t, cd.MapString, "StringMap")
			assert.Equal(t, nv, v, "版本")
			assert.Equal(t, "", msg.GetFieldByNumber(1).(string), "变更内容")
			assert.True(t, dynamic.Equal(msg, d), "结果")
		})

		t.Run("全量版本", func(t *testing.T) {
			msg, ct, cd, nv, hit := CacheManager.GetVersionCache("a", 0)
			assert.True(t, hit, "命中")
			assert.Equal(t, ct, ChangeType_Create, "类型")
			assert.Nil(t, cd, "描述")
			assert.Equal(t, nv, v, "版本")
			assert.True(t, dynamic.Equal(msg, d), "结果")
		})
	})

	time.Sleep(time.Second * 1)
	// 重置值字段
	t.Run("重置值字段", func(t *testing.T) {
		m := &test.TestMessage{Value: "foo-bar"}
		d := toDynamic(m)
		v := time.Now().Unix()
		CacheManager.SetVersionCache("a", v, d, desc, 300000)

		t.Run("叠加版本", func(t *testing.T) {
			msg, ct, cd, nv, hit := CacheManager.GetVersionCache("a", v1)
			assert.True(t, hit, "命中")
			assert.Equal(t, ct, ChangeType_Create, "类型")
			assert.Nil(t, cd, "描述")
			assert.Equal(t, nv, v, "版本")
			assert.True(t, dynamic.Equal(msg, d), "结果")
		})

		t.Run("仅变更", func(t *testing.T) {
			msg, ct, cd, nv, hit := CacheManager.GetVersionCache("a", v2)
			assert.True(t, hit, "命中")
			assert.Equal(t, ChangeType_Update, ct, "类型")
			assert.NotNil(t, cd, "描述")
			assert.Equal(t, "[01000000 00000000]", biu.BytesToBinaryString(cd.FieldTags), "字段变更标记")
			assert.Equal(t, "[00000000]", biu.BytesToBinaryString(cd.ChangeTags), "字段描述标记")
			assert.Equal(t, 0, len(cd.FieldsChangeDescs), "描述")
			assert.Nil(t, cd.MapBool, "BoolMap")
			assert.Nil(t, cd.MapInt32, "Int32Map")
			assert.Nil(t, cd.MapInt64, "Int64Map")
			assert.Nil(t, cd.MapString, "StringMap")
			assert.Equal(t, nv, v, "版本")
			assert.Equal(t, "foo-bar", msg.GetFieldByNumber(1).(string), "变更内容")
			assert.True(t, dynamic.Equal(msg, d), "结果")
		})

		t.Run("全量版本", func(t *testing.T) {
			msg, ct, cd, nv, hit := CacheManager.GetVersionCache("a", 0)
			assert.True(t, hit, "命中")
			assert.Equal(t, ct, ChangeType_Create, "类型")
			assert.Nil(t, cd, "描述")
			assert.Equal(t, nv, v, "版本")
			assert.True(t, dynamic.Equal(msg, d), "结果")
		})
	})
	time.Sleep(time.Second * 1)
	CacheManager.SetVersionCache("a", 0, nil, desc, 300000)

	t.Run("新增可空字段", func(t *testing.T) {
		p := &test.TestMessage{Nullable: &wrappers.Int32Value{Value: 123}}
		v1 = time.Now().Unix()
		d := toDynamic(p)
		CacheManager.SetVersionCache("a", v1, d, d.GetMessageDescriptor(), 300000)

		t.Run("无变更", func(t *testing.T) {
			msg, ct, cd, nv, hit := CacheManager.GetVersionCache("a", v1)
			assert.True(t, dynamic.Equal(msg, emptyD), "结果")
			assert.Equal(t, ct, ChangeType_Unchange, "类型")
			assert.Nil(t, cd, "描述")
			assert.Equal(t, nv, v1, "新版本")
			assert.True(t, hit, "命中")
		})

		t.Run("新增", func(t *testing.T) {
			msg, ct, cd, nv, hit := CacheManager.GetVersionCache("a", 0)
			assert.True(t, hit, "命中")
			assert.Equal(t, ct, ChangeType_Create, "类型")
			assert.Nil(t, cd, "描述")
			assert.Equal(t, nv, v1, "版本")
			assert.True(t, dynamic.Equal(d, msg), "结果")
		})
	})
	time.Sleep(time.Second)
	t.Run("更新可空字段", func(t *testing.T) {
		p := &test.TestMessage{Nullable: &wrappers.Int32Value{Value: 321}}
		v := time.Now().Unix()
		d := toDynamic(p)
		CacheManager.SetVersionCache("a", v, d, d.GetMessageDescriptor(), 300000)
		t.Run("全量", func(t *testing.T) {
			msg, ct, cd, nv, hit := CacheManager.GetVersionCache("a", 0)
			assert.True(t, dynamic.Equal(msg, d), "结果")
			assert.Equal(t, ct, ChangeType_Create, "类型")
			assert.Nil(t, cd, "描述")
			assert.Equal(t, nv, v, "版本")
			assert.True(t, hit, "命中")
		})
		t.Run("增量", func(t *testing.T) {
			msg, ct, cd, nv, hit := CacheManager.GetVersionCache("a", v1)
			assert.True(t, dynamic.Equal(msg, d), "结果")
			assert.Equal(t, ct, ChangeType_Update, "类型")
			assert.Equal(t, biu.BytesToBinaryString(cd.FieldTags), "[00100000 00000000]", "变更类型标示")
			assert.Equal(t, biu.BytesToBinaryString(cd.ChangeTags), "[00000000]", "变更描述标示")
			assert.Zero(t, len(cd.FieldsChangeDescs), "变更描述标示")
			assert.Equal(t, nv, v, "版本")
			assert.True(t, hit, "命中")
		})
	})

	// 删除可空字段

	// 重置可空字段

	// 新增消息字段

	// 更新消息中值字段

	// 更新消息中可空字段

	// 新增消息中value map字段

	// 更新消息中value map字段

	// 删除消息中value map字段

	// 重置消息中value map字段

	// 新增消息中value repeated字段

	// 更新消息中value repeated字段

	// 删除消息中value repeated字段

	// 重置消息中value repeated字段

	// 新增消息中message map字段

	// 更新消息中message map字段

	// 删除消息中message map字段

	// 重置消息中message map字段

	// 新增消息中message repeated字段

	// 更新消息中message repeated字段

	// 删除消息中message repeated字段

	// 重置消息中message repeated字段

	// 删除消息字段

	// 新增消息字段

	// 新增value map字段

	// 更新value map字段

	// 删除value map字段

	// 重置value map字段

	// 新增value repeated字段

	// 更新value repeated字段

	// 删除value repeated字段

	// 重置value repeated字段

	// 新增message map字段

	// 更新message map字段

	// 删除message map字段

	// 重置message map字段

	// 新增message repeated字段

	// 更新message repeated字段

	// 删除message repeated字段

	// 重置message repeated字段

	// 多次更新命中

	// 多次更新未命中
}

func TestExpire(t *testing.T) {

}

func msgEqual(source, target *dynamic.Message) bool {
	for _, field := range source.GetMessageDescriptor().GetFields() {
		if field.IsMap() {
			if !mapEqual(source, target, field) {
				return false
			}
		} else if field.IsRepeated() {
			if !repeatedEqual(source, target, field) {
				return false
			}
		} else if field.GetMessageType() != nil {
			if strings.HasPrefix(field.GetMessageType().GetFullyQualifiedName(), "google.protobuf.") {
				if !proto.Equal(source.GetField(field).(proto.Message), target.GetField(field).(proto.Message)) {
					return false
				}
			} else {
				if !msgEqual(source.GetField(field).(*dynamic.Message), target.GetField(field).(*dynamic.Message)) {
					return false
				}
			}
		} else {
			if source.GetField(field) != target.GetField(field) {
				return false
			}
		}
	}
	return true
}

func mapEqual(source, target *dynamic.Message, mapField *desc.FieldDescriptor) bool {
	diff := false
	source.ForEachMapFieldEntry(mapField, func(key, val interface{}) bool {
		v, err := target.TryGetMapField(mapField, key)
		if err != nil {
			diff = true
			return false
		}
		if mapField.GetMapValueType().GetMessageType() != nil {
			if !msgEqual(val.(*dynamic.Message), v.(*dynamic.Message)) {
				diff = true
				return false
			}
		} else {
			if val != v {
				diff = true
				return false
			}
		}
		return true
	})
	if diff {
		return false
	}

	target.ForEachMapFieldEntry(mapField, func(key, val interface{}) bool {
		v, err := source.TryGetMapField(mapField, key)
		if err != nil {
			diff = true
			return false
		}
		if mapField.GetMapValueType().GetMessageType() != nil {
			if !msgEqual(val.(*dynamic.Message), v.(*dynamic.Message)) {
				diff = true
				return false
			}
		} else {
			if val != v {
				diff = true
				return false
			}
		}
		return true
	})

	if diff {
		return false
	}
	return true
}

func repeatedEqual(source, target *dynamic.Message, repeatedField *desc.FieldDescriptor) bool {
	sr := source.GetField(repeatedField).([]interface{})
	tr := target.GetField(repeatedField).([]interface{})
	if sr == nil && tr == nil {
		return true
	}
	if (sr == nil && tr != nil) || (sr != nil && tr == nil) {
		return false
	}
	if len(sr) != len(tr) {
		return false
	}
	for idx, val := range sr {
		v := tr[idx]
		if repeatedField.GetMessageType() != nil {
			if !msgEqual(val.(*dynamic.Message), v.(*dynamic.Message)) {
				return false
			}
		} else {
			if v != val {
				return false
			}
		}
	}
	return true
}

func toDynamic(msg *test.TestMessage) *dynamic.Message {
	wrapperFs := new(descriptor.FileDescriptorSet)
	wrapperByte, _ := base64.StdEncoding.DecodeString("Cv4DCh5nb29nbGUvcHJvdG9idWYvd3JhcHBlcnMucHJvdG8SD2dvb2dsZS5wcm90b2J1ZiIjCgtEb3VibGVWYWx1ZRIUCgV2YWx1ZRgBIAEoAVIFdmFsdWUiIgoKRmxvYXRWYWx1ZRIUCgV2YWx1ZRgBIAEoAlIFdmFsdWUiIgoKSW50NjRWYWx1ZRIUCgV2YWx1ZRgBIAEoA1IFdmFsdWUiIwoLVUludDY0VmFsdWUSFAoFdmFsdWUYASABKARSBXZhbHVlIiIKCkludDMyVmFsdWUSFAoFdmFsdWUYASABKAVSBXZhbHVlIiMKC1VJbnQzMlZhbHVlEhQKBXZhbHVlGAEgASgNUgV2YWx1ZSIhCglCb29sVmFsdWUSFAoFdmFsdWUYASABKAhSBXZhbHVlIiMKC1N0cmluZ1ZhbHVlEhQKBXZhbHVlGAEgASgJUgV2YWx1ZSIiCgpCeXRlc1ZhbHVlEhQKBXZhbHVlGAEgASgMUgV2YWx1ZUJ8ChNjb20uZ29vZ2xlLnByb3RvYnVmQg1XcmFwcGVyc1Byb3RvUAFaKmdpdGh1Yi5jb20vZ29sYW5nL3Byb3RvYnVmL3B0eXBlcy93cmFwcGVyc/gBAaICA0dQQqoCHkdvb2dsZS5Qcm90b2J1Zi5XZWxsS25vd25UeXBlc2IGcHJvdG8z")
	wrapperFs.XXX_Unmarshal(wrapperByte)

	fs := new(descriptor.FileDescriptorSet)
	descBytes, _ := base64.StdEncoding.DecodeString("CpkHChlkZWNsYXJlL1Rlc3RNZXNzYWdlLnByb3RvEh9jb20udmFyaWZsaWdodC5kYXRhc2VydmljZS50ZXN0Gh5nb29nbGUvcHJvdG9idWYvd3JhcHBlcnMucHJvdG8imwUKC1Rlc3RNZXNzYWdlEhQKBXZhbHVlGAEgASgJUgV2YWx1ZRI3CghudWxsYWJsZRgCIAEoCzIbLmdvb2dsZS5wcm90b2J1Zi5JbnQzMlZhbHVlUghudWxsYWJsZRI+CgNtc2cYAyABKAsyLC5jb20udmFyaWZsaWdodC5kYXRhc2VydmljZS50ZXN0LlRlc3RNZXNzYWdlUgNtc2cSVgoIbWFwVmFsdWUYBCADKAsyOi5jb20udmFyaWZsaWdodC5kYXRhc2VydmljZS50ZXN0LlRlc3RNZXNzYWdlLk1hcFZhbHVlRW50cnlSCG1hcFZhbHVlElAKBm1hcE1zZxgFIAMoCzI4LmNvbS52YXJpZmxpZ2h0LmRhdGFzZXJ2aWNlLnRlc3QuVGVzdE1lc3NhZ2UuTWFwTXNnRW50cnlSBm1hcE1zZxIkCg1yZXBlYXRlZFZhbHVlGAogAygJUg1yZXBlYXRlZFZhbHVlEk8KC3JlcGVhdGVkTXNnGOcHIAMoCzIsLmNvbS52YXJpZmxpZ2h0LmRhdGFzZXJ2aWNlLnRlc3QuVGVzdE1lc3NhZ2VSC3JlcGVhdGVkTXNnEjYKAWgY6AcgASgLMicuY29tLnZhcmlmbGlnaHQuZGF0YXNlcnZpY2UudGVzdC5IZWhlZGFSAWgaOwoNTWFwVmFsdWVFbnRyeRIQCgNrZXkYASABKAlSA2tleRIUCgV2YWx1ZRgCIAEoBVIFdmFsdWU6AjgBGmcKC01hcE1zZ0VudHJ5EhAKA2tleRgBIAEoCVIDa2V5EkIKBXZhbHVlGAIgASgLMiwuY29tLnZhcmlmbGlnaHQuZGF0YXNlcnZpY2UudGVzdC5UZXN0TWVzc2FnZVIFdmFsdWU6AjgBIhgKBkhlaGVkYRIOCgJoZRgBIAEoCVICaGVCewolY29tLnZhcmlmbGlnaHQuZGF0YXNlcnZpY2UuY2FjaGUudGVzdFAAWh92YXJpZmxpZ2h0LmNvbS9kYXRhc2VydmljZS90ZXN0+AEBogIDRFNDqgIlY29tLnZhcmlmbGlnaHQuZGF0YXNlcnZpY2UuY2FjaGUudGVzdGIGcHJvdG8zCtUIChRzeXMvQ2hhbmdlRGVzYy5wcm90bxIeY29tLnZhcmlmbGlnaHQuZGF0YXNlcnZpY2Uuc3lzIqQHCgpDaGFuZ2VEZXNjEhwKCWZpZWxkVGFncxgBIAEoDFIJZmllbGRUYWdzEh4KCmNoYW5nZVRhZ3MYAiABKAxSCmNoYW5nZVRhZ3MSWAoRZmllbGRzQ2hhbmdlRGVzY3MYAyADKAsyKi5jb20udmFyaWZsaWdodC5kYXRhc2VydmljZS5zeXMuQ2hhbmdlRGVzY1IRZmllbGRzQ2hhbmdlRGVzY3MSVQoJbWFwX2ludDMyGAQgAygLMjguY29tLnZhcmlmbGlnaHQuZGF0YXNlcnZpY2Uuc3lzLkNoYW5nZURlc2MuTWFwSW50MzJFbnRyeVIIbWFwSW50MzISVQoJbWFwX2ludDY0GAUgAygLMjguY29tLnZhcmlmbGlnaHQuZGF0YXNlcnZpY2Uuc3lzLkNoYW5nZURlc2MuTWFwSW50NjRFbnRyeVIIbWFwSW50NjQSUgoIbWFwX2Jvb2wYBiADKAsyNy5jb20udmFyaWZsaWdodC5kYXRhc2VydmljZS5zeXMuQ2hhbmdlRGVzYy5NYXBCb29sRW50cnlSB21hcEJvb2wSWAoKbWFwX3N0cmluZxgHIAMoCzI5LmNvbS52YXJpZmxpZ2h0LmRhdGFzZXJ2aWNlLnN5cy5DaGFuZ2VEZXNjLk1hcFN0cmluZ0VudHJ5UgltYXBTdHJpbmcaZwoNTWFwSW50MzJFbnRyeRIQCgNrZXkYASABKAVSA2tleRJACgV2YWx1ZRgCIAEoCzIqLmNvbS52YXJpZmxpZ2h0LmRhdGFzZXJ2aWNlLnN5cy5DaGFuZ2VEZXNjUgV2YWx1ZToCOAEaZwoNTWFwSW50NjRFbnRyeRIQCgNrZXkYASABKANSA2tleRJACgV2YWx1ZRgCIAEoCzIqLmNvbS52YXJpZmxpZ2h0LmRhdGFzZXJ2aWNlLnN5cy5DaGFuZ2VEZXNjUgV2YWx1ZToCOAEaZgoMTWFwQm9vbEVudHJ5EhAKA2tleRgBIAEoCFIDa2V5EkAKBXZhbHVlGAIgASgLMiouY29tLnZhcmlmbGlnaHQuZGF0YXNlcnZpY2Uuc3lzLkNoYW5nZURlc2NSBXZhbHVlOgI4ARpoCg5NYXBTdHJpbmdFbnRyeRIQCgNrZXkYASABKAlSA2tleRJACgV2YWx1ZRgCIAEoCzIqLmNvbS52YXJpZmxpZ2h0LmRhdGFzZXJ2aWNlLnN5cy5DaGFuZ2VEZXNjUgV2YWx1ZToCOAFCbgoeY29tLnZhcmlmbGlnaHQuZGF0YXNlcnZpY2Uvc3lzUABaHnZhcmlmbGlnaHQuY29tL2RhdGFzZXJ2aWNlL3N5c/gBAaICBURTU1lTqgIeY29tLnZhcmlmbGlnaHQuZGF0YXNlcnZpY2Uuc3lzYgZwcm90bzM=")
	err := fs.XXX_Unmarshal(descBytes)
	if err != nil {
		fmt.Println("can't parse desc")
	}

	fs.File = append(fs.File, wrapperFs.File...)
	files, err := desc.CreateFileDescriptorsFromSet(fs)
	if err != nil {
		fmt.Println("can't parse desc")
	}
	messages := map[string]*desc.MessageDescriptor{}
	for _, file := range files {
		for _, msgType := range file.GetMessageTypes() {
			messages[msgType.GetFullyQualifiedName()] = msgType
		}
	}

	//bts, _ := proto.Marshal(&test.TestMessage{Nullable:&wrappers.Int32Value{Value:123}, Msg:&test.TestMessage{Value:"asdf"}, H:&test.Heheda{He:"aaa"}})
	bts, _ := proto.Marshal(msg)
	result := dynamic.NewMessage(messages["com.variflight.dataservice.test.TestMessage"])
	result.Unmarshal(bts)
	return result
}
