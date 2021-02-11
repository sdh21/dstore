package utils

import "fmt"

func Debug(format string, a ...interface{}) (n int) {
	//n, _ = fmt.Printf(format, a...)
	return
}

func ProposeInfo(format string, a ...interface{}) (n int) {
	//n, _ = fmt.Printf(format + "\n", a...)
	return
}

func ProposePhaseInfo(format string, a ...interface{}) (n int) {
	//n, _ = fmt.Printf(format + "\n", a...)
	return
}

func ProposeNetworkError(format string, a ...interface{}) (n int) {
	n, _ = fmt.Printf(format+"\n", a...)
	return
}

func HeartBeatError(format string, a ...interface{}) (n int) {
	//n, _ = fmt.Printf(format + "\n", a...)
	return
}

func Info(format string, a ...interface{}) (n int) {
	//n, _ = fmt.Printf(format + "\n", a...)
	return
}

func Warning(format string, a ...interface{}) (n int) {
	n, _ = fmt.Printf(format+"\n", a...)
	return
}

func Error(format string, a ...interface{}) (n int) {
	n, _ = fmt.Printf(format+"\n", a...)
	return
}
