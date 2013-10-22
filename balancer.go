package iproto

type BalancerPoint struct {
	SimplePoint
	children []EndPoint
}

func (b *BalancerPoint) Init() {
	b.SimplePoint.Init(b)
}

func (b *BalancerPoint) AddChild(ch EndPoint) {
	b.children = append(b.children, ch)
	if b.Runned() {
		ch.Run(b.b.ch)
	}
}

func (b *BalancerPoint) Loop() {
	for _, ch := range b.children {
		b.RunChild(ch)
	}
	<-b.ExitChan()
	for _, ch := range b.children {
		ch.Stop()
	}
}
