package helpers

import (
	"log"
	"os"
)

func PrintDbg(s string) {
	if os.Getenv("IDBDS_DEBUG") != "" {
		log.Println("[DEBUG] ", s)
	}
}

func PrintInfo(s string) {
	log.Println("[INFO] ", s)
}

func PrintWarn(s string) {
	log.Println("[WARNING] ", s)
}

func PrintErr(s string) {
	log.Println("[ERROR] ", s)
}

func PrintFatal(s string) {
	log.Fatal("[ERROR] ", s)
}
