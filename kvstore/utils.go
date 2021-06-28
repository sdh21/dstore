package kvstore

func (x *AnyValue) Clone() *AnyValue {
	v := &AnyValue{}
	v.Type = x.Type
	if x.Type == ValueType_String {
		v.Str = x.Str
	} else if x.Type == ValueType_Number {
		v.Num = x.Num
	} else if x.Type == ValueType_List {
		v.List = make([]*AnyValue, len(x.List))
		for i := range x.List {
			v.List[i] = x.List[i].Clone()
		}
	} else if x.Type == ValueType_HashMap {
		v.Map = map[string]*AnyValue{}
		for i, val := range x.Map {
			v.Map[i] = val.Clone()
		}
	}
	return v
}

func (x *AnyValue) ShallowCopy(dst *AnyValue) {
	dst.Type = x.Type
	if x.Type == ValueType_String {
		dst.Str = x.Str
	} else if x.Type == ValueType_Number {
		dst.Num = x.Num
	} else if x.Type == ValueType_List {
		dst.List = x.List
	} else if x.Type == ValueType_HashMap {
		dst.Map = x.Map
	}
}
