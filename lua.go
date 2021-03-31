package kafka

import "github.com/edunx/lua"

func LuaInjectApi(L *lua.LState , parent *lua.UserKV) {
	kfk  := &lua.UserKV{}
	kfk.Set("producer" , lua.NewGFunction( createProducerUserData ))
	parent.Set("kafka" , kfk)
}
