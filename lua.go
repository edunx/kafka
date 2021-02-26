package kafka

import "github.com/edunx/lua"

func LuaInjectApi(L *lua.LState , parent *lua.LTable) {
	kTab := L.CreateTable(0 , 1)
	LuaInjectProducerApi(L , kTab)

	L.SetField(parent, "kafka" , kTab)
}
