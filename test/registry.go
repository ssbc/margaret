package test

var NewLogFuncs map[string]NewLogFunc

func init() {
	NewLogFuncs = map[string]NewLogFunc{}
}

func Register(name string, f NewLogFunc) {
	NewLogFuncs[name] = f
}
