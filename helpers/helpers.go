package helpers

import (
	"log"
	"os"
)

// PrintDbg prints the debug message if the "IDBDS_DEBUG" environment variable is set.
//
// s is the string to be printed.
func PrintDbg(s string) {
	if os.Getenv("IDBDS_DEBUG") != "" {
		log.Println("[DEBUG] ", s)
	}
}

// PrintInfo prints the given string as an information message.
//
// s is the string to be printed.
func PrintInfo(s string) {
	log.Println("[INFO] ", s)
}

// PrintWarn prints a warning message to the log.
//
// s is the string to be printed.
func PrintWarn(s string) {
	log.Println("[WARNING] ", s)
}

// PrintErr prints an error message to the log.
//
// s is the string to be printed.
func PrintErr(s string) {
	log.Println("[ERROR] ", s)
}

// rintErr prints an error message to the log and exits.
//
// s is the string to be printed.
func PrintFatal(s string) {
	log.Fatal("[ERROR] ", s)
}
