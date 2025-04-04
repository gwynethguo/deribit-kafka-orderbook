package logging

import (
	"log"
	"os"
	"runtime"
	"strings"
)

func WriteLogsToFile() *os.File {
	_, filename, _, ok := runtime.Caller(1)
	if !ok {
		log.Fatalln("Could not retrieve caller information")
	}
	parts := strings.Split(filename, "/")
	logFile, err := os.OpenFile(strings.Replace(parts[len(parts)-1], ".go", ".log", 1), os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		log.Fatal(err)
	}

	log.SetOutput(logFile)

	return logFile
}
