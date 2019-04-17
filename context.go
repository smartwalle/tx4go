package tx4go

type Context struct {
	keys map[string]interface{}
}

func (this *Context) Get(key string) interface{} {
	return this.keys[key]
}

func (this *Context) Set(key string, value interface{}) {
	this.keys[key] = value
}

func (this *Context) Del(key string) {
	delete(this.keys, key)
}
