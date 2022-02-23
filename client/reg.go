package client

import (
	"fmt"
	"sync"
)

func toKey(topic, event, ref string) string {
	return fmt.Sprintf("KEY:%s:%s:%s:", topic, event, ref)
}

type regCenter struct {
	sync.RWMutex
	regs map[string][]*Puller
}

func newRegCenter() *regCenter {
	return &regCenter{
		regs: make(map[string][]*Puller),
	}
}

func (center *regCenter) register(key string) *Puller {
	puller := &Puller{
		center: center,
		key:    key,
		ch:     make(chan *Message, pullerMsgSize),
	}

	center.Lock()
	defer center.Unlock()

	center.regs[key] = append(center.regs[key], puller)

	return puller
}

func (center *regCenter) unregister(puller *Puller) {
	center.Lock()
	defer center.Unlock()

	pullers := center.regs[puller.key]
	for i, _puller := range pullers {
		if _puller != puller {
			continue
		}
		pullers[i] = pullers[len(pullers)-1]
		center.regs[puller.key] = pullers[:len(pullers)-1]
		return
	}
}
