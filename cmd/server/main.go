package main

import (
	"log"

	"cw3/internal/app"
)

func main() {
	application, err := app.New()
	if err != nil {
		log.Fatalf("init app failed: %v", err)
	}

	if err := application.Run(); err != nil {
		log.Fatalf("app run failed: %v", err)
	}

	log.Println("app exited normally")
}
