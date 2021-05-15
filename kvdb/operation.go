package kvdb

import (
	"encoding/gob"
	"github.com/sdh21/dstore/utils"
	"log"
	"strconv"
)

func lookup(state *TableState, keys []string) (*AnyValue, bool) {
	target := state.Data
	for _, key := range keys {
		// target is always AnyValue
		switch target.Type {
		case "Map":
			m, found := target.Map[key]
			if !found {
				return nil, false
			}
			target = m
		case "List":
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

func OpGet(lookupKey []string, alias string) *AnyOp {
	return &AnyOp{
		Type:                 "Get",
		LookupKey:            lookupKey,
		FieldVersionExpected: -1,
		Alias:                alias,
	}
}

func (x *AnyOp) OpSetTableOption(option CreateTableOption, errIfExists bool) *AnyOp {
	x.TableOps = &TableOp{
		Option:      option,
		ErrIfExists: errIfExists,
	}
	return x
}

func (x *AnyOp) OpSetExpectedVersion(version int64) *AnyOp {
	x.FieldVersionExpected = version
	return x
}

func (x *AnyOp) OpSetOnFailOption(option OnFailOption) *AnyOp {
	x.OnFail = option
	return x
}

func applyGet(op *AnyOp, state *TableState, result *TransactionResult) {
	target, ok := lookup(state, op.LookupKey)
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

func copyAnyValue(dst *AnyValue, src *AnyValue) {
	dst.Version = src.Version
	dst.Map = src.Map
	dst.Type = src.Type
	dst.List = src.List
	dst.Bin = src.Bin
	dst.Num = src.Num
	dst.Str = src.Str
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
	return &AnyOp{
		Type:                 "Put",
		LookupKey:            lookupKey,
		Value:                MakeAnyValue(value),
		FieldVersionExpected: -1,
	}
}

func applyPut(op *AnyOp, state *TableState, result *TransactionResult) {
	target, ok := lookup(state, op.LookupKey)
	if !ok {
		result.Status = TransactionResult_TypeAssertionFailed
		return
	}
	if op.FieldVersionExpected != -1 && target.Version != op.FieldVersionExpected {
		result.Status = TransactionResult_NotExpectedValueVersion
		return
	}
	copyAnyValue(target, op.Value)
}

// Append to a list.
func OpAppend(lookupKey []string, value interface{}) *AnyOp {
	return &AnyOp{
		Type:                 "Append",
		LookupKey:            lookupKey,
		Value:                MakeAnyValue(value),
		FieldVersionExpected: -1,
	}
}

func applyAppend(op *AnyOp, state *TableState, result *TransactionResult) {
	target, ok := lookup(state, op.LookupKey)
	if !ok {
		result.Status = TransactionResult_TypeAssertionFailed
		return
	}
	if op.FieldVersionExpected != -1 && target.Version != op.FieldVersionExpected {
		result.Status = TransactionResult_NotExpectedValueVersion
		return
	}
	// target should be list
	if target.Type != "List" {
		result.Status = TransactionResult_TypeAssertionFailed
		return
	}
	if target.List == nil {
		target.List = []*AnyValue{}
	}
	target.List = append(target.List, op.Value)
	target.Version++
}

// Store a <k,v> in the map.
func OpMapStore(lookupKey []string, key string, value interface{}) *AnyOp {
	return &AnyOp{
		Type:                 "MapStore",
		LookupKey:            lookupKey,
		Key:                  key,
		Value:                MakeAnyValue(value),
		FieldVersionExpected: -1,
	}
}

func applyMapStore(op *AnyOp, state *TableState, result *TransactionResult) {
	target, ok := lookup(state, op.LookupKey)
	if !ok {
		result.Status = TransactionResult_TypeAssertionFailed
		return
	}
	if op.FieldVersionExpected != -1 && target.Version != op.FieldVersionExpected {
		result.Status = TransactionResult_NotExpectedValueVersion
		return
	}
	// target should be map
	if target.Type != "Map" {
		result.Status = TransactionResult_TypeAssertionFailed
		return
	}
	if target.Map == nil {
		target.Map = map[string]*AnyValue{}
	}
	target.Map[op.Key] = op.Value
	target.Version++
}

func OpMapDelete(lookupKey []string, key string) *AnyOp {
	return &AnyOp{
		Type:                 "MapStore",
		LookupKey:            lookupKey,
		Key:                  key,
		FieldVersionExpected: -1,
	}
}

func applyMapDelete(op *AnyOp, state *TableState, result *TransactionResult) {
	target, ok := lookup(state, op.LookupKey)
	if !ok {
		result.Status = TransactionResult_TypeAssertionFailed
		return
	}
	if op.FieldVersionExpected != -1 && target.Version != op.FieldVersionExpected {
		result.Status = TransactionResult_NotExpectedValueVersion
		return
	}
	// target should be map
	if target.Type != "Map" {
		result.Status = TransactionResult_TypeAssertionFailed
		return
	}
	if target.Map == nil {
		target.Map = map[string]*AnyValue{}
	}
	delete(target.Map, op.Key)
	target.Version++
}

// Check whether Comparer(Values[AliasA], ValueB) is true.
// If false, transaction can be aborted/rollback/continued
// according to user settings.
// User can use an OpGet to create the AliasA to compare.
func applyCheck(op *AnyOp, state *TableState, result *TransactionResult) {
	log.Fatalf("not implemented Op")
}

// for test only
func applyPutHash(op *AnyOp, state *TableState, result *TransactionResult) {
	if state.Data.Map == nil {
		state.Data.Map = map[string]*AnyValue{}
	}
	m := state.Data.Map
	if m[op.Key] == nil {
		m[op.Key] = &AnyValue{}
	}
	oldValue := m[op.Key].Str
	newValue := strconv.Itoa(int(hash(oldValue + op.Value.Str)))
	m[op.Key].Str = newValue
	m[op.Key].Version++

	result.Values[op.Key] = &AliasValue{
		IsNull: false,
		Value:  &AnyValue{Type: "Str", Str: oldValue},
	}
}

// Apply an AnyOp to TableState
func (x *AnyOp) Apply(state *TableState, result *TransactionResult) error {
	// If a previous op failed, do not continue
	if result.Status != TransactionResult_OK {
		return nil
	}
	switch x.Type {
	case "Get":
		applyGet(x, state, result)
	case "Put":
		applyPut(x, state, result)
	case "PutHash":
		applyPutHash(x, state, result)
	case "Check":
		applyCheck(x, state, result)
	case "Append":
		applyAppend(x, state, result)
	case "MapStore":
		applyMapStore(x, state, result)
	case "MapDelete":
		applyMapDelete(x, state, result)
	default:
		utils.Error("no op named %v", x.Type)
	}
	return nil
}

func init() {
	gob.Register(&OpWrapper{})
	gob.Register(&AnyOp{})
	gob.Register(&TransactionResult{})
	gob.Register(&TableState{})
	gob.Register(&AnyValue{})
	gob.Register(map[string]*AnyValue{})
	gob.Register([]*AnyValue{})
}

func MakeAnyValue(value interface{}) *AnyValue {
	switch value.(type) {
	case string:
		return &AnyValue{Type: "Str", Str: value.(string)}
	case int64:
		return &AnyValue{Type: "Int", Num: value.(int64)}
	case int:
		return &AnyValue{Type: "Int", Num: (int64)(value.(int))}
	case []byte:
		return &AnyValue{Type: "Bin", Bin: value.([]byte)}
	case map[string]*AnyValue:
		return &AnyValue{Type: "Map", Map: value.(map[string]*AnyValue)}
	case []*AnyValue:
		return &AnyValue{Type: "List", List: value.([]*AnyValue)}
	default:
		utils.Error("MakeAnyValue: invalid value")
	}
	return nil
}
