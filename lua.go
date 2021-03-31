package kafka

import "github.com/edunx/lua"

func LuaInjectApi(L *lua.LState , parent *lua.LTable) {
	kfk  := &lua.UserKV{}
	kfk.Set("producer" , lua.NewGFunction( createProducerUserData ))
	L.SetField(parent, "kafka" , kfk)
}
