package kvstore

import (
	"crypto/sha512"
	"encoding/base64"
	"encoding/gob"
	"github.com/sdh21/dstore/utils"
	"log"
	"strconv"
)

func lookup(cell *AnyValue, keys []string) (*AnyValue, bool) {
	target := cell
	for _, key := range keys {
		// target is always AnyValue
		switch target.Type {
		case ValueType_HashMap:
			m, found := target.Map[key]
			if !found {
				return nil, false
			}
			target = m
		case ValueType_List:
			idx, err := strconv.Atoi(key)
			if err != nil {
				return nil, false
			}
			arr := target.List
			if idx >= len(arr) {
				return nil, false
			}
			target = arr[idx]
		default:
			return nil, false
		}
	}
	return target, true
}

func OpCreateCollection(collectionId string) *AnyOp {
	return &AnyOp{
		Type:                    OpType_CreateCollection,
		CollectionId:            collectionId,
		DocumentVersionExpected: -1,
		FieldVersionExpected:    -1,
		CreateOption:            nil,
	}
}

func OpCreateDocument(collectionId string, documentId string) *AnyOp {
	return &AnyOp{
		Type:                    OpType_CreateDocument,
		CollectionId:            collectionId,
		DocumentId:              documentId,
		DocumentVersionExpected: -1,
		FieldVersionExpected:    -1,
		CreateOption:            nil,
	}
}

func (x *AnyOp) parseLookupKey() *AnyOp {
	if len(x.LookupKey) < 2 {
		panic("collection id and document id must be specified")
	}
	x.CollectionId = x.LookupKey[0]
	x.DocumentId = x.LookupKey[1]
	x.LookupKey = x.LookupKey[2:]
	return x
}

func OpGet(lookupKey []string, alias string) *AnyOp {
	return (&AnyOp{
		Type:                    OpType_Get,
		LookupKey:               lookupKey,
		DocumentVersionExpected: -1,
		FieldVersionExpected:    -1,
		Alias:                   alias,
	}).parseLookupKey()
}

func (x *AnyOp) OpSetExpectedVersion(version int64) *AnyOp {
	x.FieldVersionExpected = version
	return x
}

func (x *AnyOp) OpSetOnFailOption(option OnFailOption) *AnyOp {
	x.OnFail = option
	return x
}

func (x *AnyOp) OpSetCreateOption(option *CreateOption) *AnyOp {
	x.CreateOption = option
	return x
}

func applyGet(op *AnyOp, cell *AnyValue, result *TransactionResult) {
	target, ok := lookup(cell, op.LookupKey)
	if !ok {
		result.Values[op.Alias] = &AliasValue{
			IsNull: true,
			Value:  nil,
		}
	} else {
		result.Values[op.Alias] = &AliasValue{
			IsNull: false,
			Value:  target,
		}
	}
}

// To create/replace new keys/values in map,
// it is recommended to use OpMapStore.
// To set a value in a list, use OpPut,
// since OpAppend can only append to a list.
// OpPut does not check the original ValueType.
// It will overwrite the whole original value, including
// version, type, etc.
// The keys must exist. OpPut does not create new keys/values.
// A new table's first AnyValue is set to map[string]*AnyValue{}.
// Usage example,
// If Key ["Map1", "List1", "0"] already exists,
// OpPut{Key: ["Map1", "List1", "0"], Value: String(...)},  Replace 0->... with 0->string in the above list
func OpPut(lookupKey []string, value interface{}) *AnyOp {
	return (&AnyOp{
		Type:                    OpType_Put,
		LookupKey:               lookupKey,
		Value:                   MakeAnyValue(value),
		DocumentVersionExpected: -1,
		FieldVersionExpected:    -1,
	}).parseLookupKey()
}

func applyPut(op *AnyOp, cell *AnyValue, result *TransactionResult) {
	target, ok := lookup(cell, op.LookupKey)
	if !ok {
		result.Status = TransactionResult_TypeAssertionFailed
		return
	}
	if op.FieldVersionExpected != -1 && target.Version != op.FieldVersionExpected {
		result.Status = TransactionResult_NotExpectedFieldVersion
		return
	}

	// shallow copy is enough
	op.Value.ShallowCopy(target)
}

func OpListAppend(lookupKey []string, value interface{}) *AnyOp {
	return (&AnyOp{
		Type:                    OpType_ListAppend,
		LookupKey:               lookupKey,
		Value:                   MakeAnyValue(value),
		DocumentVersionExpected: -1,
		FieldVersionExpected:    -1,
	}).parseLookupKey()
}

func applyListAppend(op *AnyOp, cell *AnyValue, result *TransactionResult) {
	target, ok := lookup(cell, op.LookupKey)
	if !ok {
		result.Status = TransactionResult_TypeAssertionFailed
		return
	}
	if op.FieldVersionExpected != -1 && target.Version != op.FieldVersionExpected {
		result.Status = TransactionResult_NotExpectedFieldVersion
		return
	}
	// target should be list
	if target.Type != ValueType_List {
		result.Status = TransactionResult_TypeAssertionFailed
		return
	}
	if target.List == nil {
		target.List = []*AnyValue{}
	}
	target.List = append(target.List, op.Value)
	target.Version++
}

func OpMapStore(lookupKey []string, key string, value interface{}) *AnyOp {
	return (&AnyOp{
		Type:                    OpType_MapStore,
		LookupKey:               lookupKey,
		MapKey:                  key,
		Value:                   MakeAnyValue(value),
		DocumentVersionExpected: -1,
		FieldVersionExpected:    -1,
	}).parseLookupKey()
}

func applyMapStore(op *AnyOp, cell *AnyValue, result *TransactionResult) {
	target, ok := lookup(cell, op.LookupKey)
	if !ok {
		result.Status = TransactionResult_TypeAssertionFailed
		return
	}
	if op.FieldVersionExpected != -1 && target.Version != op.FieldVersionExpected {
		result.Status = TransactionResult_NotExpectedFieldVersion
		return
	}
	// target should be map
	if target.Type != ValueType_HashMap {
		result.Status = TransactionResult_TypeAssertionFailed
		return
	}
	if target.Map == nil {
		target.Map = map[string]*AnyValue{}
	}
	target.Map[op.MapKey] = op.Value
	target.Version++
}

func OpMapDelete(lookupKey []string, key string) *AnyOp {
	return (&AnyOp{
		Type:                    OpType_MapDelete,
		LookupKey:               lookupKey,
		MapKey:                  key,
		DocumentVersionExpected: -1,
		FieldVersionExpected:    -1,
	}).parseLookupKey()
}

func applyMapDelete(op *AnyOp, cell *AnyValue, result *TransactionResult) {
	target, ok := lookup(cell, op.LookupKey)
	if !ok {
		result.Status = TransactionResult_TypeAssertionFailed
		return
	}
	if op.FieldVersionExpected != -1 && target.Version != op.FieldVersionExpected {
		result.Status = TransactionResult_NotExpectedFieldVersion
		return
	}
	// target should be map
	if target.Type != ValueType_HashMap {
		result.Status = TransactionResult_TypeAssertionFailed
		return
	}
	if target.Map == nil {
		target.Map = map[string]*AnyValue{}
	}
	delete(target.Map, op.MapKey)
	target.Version++
}

// Check whether Comparer(Values[AliasA], ValueB) is true.
// If false, transaction can be aborted/rollback/continued
// according to user settings.
// User can use an OpGet to create the AliasA to compare.
func applyCheck(op *AnyOp, cell *AnyValue, result *TransactionResult) {
	log.Fatalf("not implemented Op")
}

func OpDoStrHash(lookupKey []string, key string, value string, alias string) *AnyOp {
	return (&AnyOp{
		Type:                    OpType_DoStrHash,
		LookupKey:               lookupKey,
		MapKey:                  key,
		Value:                   MakeAnyValue(value),
		Alias:                   alias,
		DocumentVersionExpected: -1,
		FieldVersionExpected:    -1,
	}).parseLookupKey()
}

func applyDoStrHash(op *AnyOp, cell *AnyValue, result *TransactionResult) {
	target, ok := lookup(cell, op.LookupKey)
	if !ok {
		result.Status = TransactionResult_TypeAssertionFailed
		return
	}
	if op.FieldVersionExpected != -1 && target.Version != op.FieldVersionExpected {
		result.Status = TransactionResult_NotExpectedFieldVersion
		return
	}
	// target should be map
	if target.Type != ValueType_HashMap {
		result.Status = TransactionResult_TypeAssertionFailed
		return
	}
	if target.Map == nil {
		target.Map = map[string]*AnyValue{}
	}
	old := target.Map[op.MapKey]
	target.Map[op.MapKey] = &AnyValue{Type: ValueType_String, Str: doStrHash(old.Str, op.Value.Str)}
	result.Values[op.Alias] = &AliasValue{IsNull: false, Value: old}
	target.Version++
}

func doStrHash(old string, new string) string {
	digest := sha512.Sum512([]byte(old + new))
	return base64.StdEncoding.EncodeToString(digest[:])
}

// Apply an AnyOp
func (x *AnyOp) Apply(cell *AnyValue, result *TransactionResult) error {
	// If a previous op failed, do not continue
	if result.Status != TransactionResult_OK {
		return nil
	}
	switch x.Type {
	case OpType_Get:
		applyGet(x, cell, result)
	case OpType_Put:
		applyPut(x, cell, result)
	case OpType_MapStore:
		applyMapStore(x, cell, result)
	case OpType_MapDelete:
		applyMapDelete(x, cell, result)
	case OpType_Check:
		applyCheck(x, cell, result)
	case OpType_ListAppend:
		applyListAppend(x, cell, result)
	case OpType_DoStrHash:

	default:
		utils.Error("no op named %v", x.Type)
	}
	return nil
}

func init() {
	gob.Register(&AnyOp{})
	gob.Register(&TransactionResult{})
	gob.Register(&CollectionState{})
	gob.Register(&AnyValue{})
	gob.Register(map[string]*AnyValue{})
	gob.Register([]*AnyValue{})
}

func MakeAnyValue(value interface{}) *AnyValue {
	switch value.(type) {
	case string:
		return &AnyValue{Type: ValueType_String, Str: value.(string)}
	case int64:
		return &AnyValue{Type: ValueType_Number, Num: value.(int64)}
	case int:
		return &AnyValue{Type: ValueType_Number, Num: (int64)(value.(int))}
	case []byte:
		return &AnyValue{Type: ValueType_Binary, Bin: value.([]byte)}
	case map[string]*AnyValue:
		return &AnyValue{Type: ValueType_HashMap, Map: value.(map[string]*AnyValue)}
	case []*AnyValue:
		return &AnyValue{Type: ValueType_List, List: value.([]*AnyValue)}
	default:
		utils.Error("MakeAnyValue: invalid value")
	}
	return nil
}
