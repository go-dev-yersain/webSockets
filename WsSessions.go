package main

import (
	"gopkg.in/melody"
	"sync"
)

func NewWsSession()*WsSessions{
	return &WsSessions{
		wsMap:make(map[string]*melody.Session),
	}
}

type WsSessions struct {
	mx    sync.RWMutex
	wsMap map[string]*melody.Session
}

func(ws *WsSessions) Size() int{
	ws.mx.RLock()
	defer ws.mx.RUnlock()
	return len(ws.wsMap)
}

func (ws *WsSessions) Load(key string) (*melody.Session, bool) {
	ws.mx.RLock()
	defer ws.mx.RUnlock()
	val, ok := ws.wsMap[key]
	return val, ok
}

func (ws *WsSessions) Store(key string, value *melody.Session) {
	ws.mx.Lock()
	defer ws.mx.Unlock()
	ws.wsMap[key] = value
}
