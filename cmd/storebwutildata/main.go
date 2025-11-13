// Copyright 2024 by Marko Punnar <marko[AT]aretaja.org>
// Use of this source code is governed by a Apache License 2.0 that can be found in the LICENSE file.

// storebwutildata is external application for calculating and storing bandwidth usage data of eth ports to InfluxDB metrics

package main

import (
	"log"

	"github.com/aretaja/idbdownsampler/app"
	"github.com/aretaja/idbdownsampler/helpers"
)

// Version of release
const version string = "v0.0.6"

// main is the entry point of the program.
//
// No parameters.
// No return values.
func main() {
	log.SetFlags(log.Ldate | log.Lmicroseconds)

	helpers.PrintInfo("start storebwutildata")
	helpers.PrintDbg("initializing app")

	a := &app.App{
		Version: version,
	}

	a.Initialize()
	helpers.PrintDbg("app initialized")

	helpers.PrintDbg("running app")
	a.StoreBwData()
}
