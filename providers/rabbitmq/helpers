package rabbitmq

// Func is current function name provider,
// like `__FUNCTION__` of PHP.
func currentFunc() string {
	pc, _, _, _ := runtime.Caller(depthOfFunctionCaller)
	fn := runtime.FuncForPC(pc)
	elems := strings.Split(fn.Name(), ".")
	return elems[len(elems)-1]
}
