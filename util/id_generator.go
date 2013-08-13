package util

import (
	"fmt"
	"log"
)

// IdGenerator stores stores arbitrary values under unique uint32 indices,
// and generates such indices in range min <= id <= max
type IdGenerator struct {
	min, max uint32
	id       uint32
	hold     map[uint32]interface{}
}

// Init generater with minimum and maximum values
func (i *IdGenerator) Init(min, max uint32) {
	if max <= min {
		log.Panicf("idGenerater: min %d should be less than max %d", i.min, i.max)
	}
	i.max, i.min = max, min
	i.id = min
	i.hold = make(map[uint32]interface{})
}

// Init generater with minimum and maximum values
func (i *IdGenerator) DefInit() {
	i.Init(1, ^uint32(0)-1)
}

// Next returns next unused id. If whole range is busy, then error is returned
func (i *IdGenerator) Next() (id uint32, err error) {
	if i.max == 0 {
		log.Panic("Attempt to use uninitialized IdGenerator")
	}
	id = i.id
	tested := id
	_, ok := i.hold[id]
	for ok {
		if id == i.max {
			id = i.min
		}
		id++
		if id == tested {
			err = fmt.Errorf("All id between %d and %d are used", i.min, i.max)
			return
		}
		_, ok = i.hold[i.id]
	}
	i.id = id + 1
	return
}

func (i *IdGenerator) Set(id uint32, o interface{}) {
	if i.max == 0 {
		log.Panic("Attempt to use uninitialized IdGenerator")
	}
	if _, ok := i.hold[id]; ok {
		log.Panicf("IdGenerator: attempt to set duplicate id %d", id)
	}
	i.hold[id] = o
}

func (i *IdGenerator) Remove(id uint32) (res interface{}) {
	if i.max == 0 {
		log.Panic("Attempt to use uninitialized IdGenerator")
	}
	res = i.hold[id]
	delete(i.hold, id)
	return
}

func (i *IdGenerator) Get(id uint32) (res interface{}) {
	if i.max == 0 {
		log.Panic("Attempt to use uninitialized IdGenerator")
	}
	res = i.hold[id]
	return
}

func (i *IdGenerator) Size() int {
	if i.max == 0 {
		log.Panic("Attempt to use uninitialized IdGenerator")
	}
	return len(i.hold)
}

func (i *IdGenerator) Holded() (r []interface{}) {
	r = make([]interface{}, len(i.hold))
	j := 0
	for _, v := range(i.hold) {
		r[j] = v
		j++
	}
	return
}
