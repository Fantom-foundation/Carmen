package mpt

import (
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"runtime"
)

func init() {
	port := 6060
	fmt.Printf("Starting diagnostic server at port http://localhost:%d\n", port)
	fmt.Printf("(see https://pkg.go.dev/net/http/pprof#hdr-Usage_examples for usage examples)\n")
	fmt.Printf("Block and mutex sampling rate is set to 100%% for diagnostics, which may impact overall performance\n")
	go func() {
		addr := fmt.Sprintf("localhost:%d", port)
		log.Println(http.ListenAndServe(addr, nil))
	}()
	runtime.SetBlockProfileRate(1)
	runtime.SetMutexProfileFraction(1)
}
