package main

import (
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"bigcartel/dolosse/app"
	"bigcartel/dolosse/err_utils"

	"github.com/pkg/profile"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/siddontang/go-log/log"
)

func main() {
	app := app.NewApp(false, os.Args[1:])

	app.InitState(false)

	var p *profile.Profile
	if *app.Config.RunProfile {
		p = profile.Start(profile.CPUProfile, profile.ProfilePath("."), profile.NoShutdownHook).(*profile.Profile)
	}

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-ch
		log.Infoln("Exiting...")

		if *app.Config.RunProfile && p != nil {
			p.Stop()
		}

		app.Shutdown()
		time.Sleep(2 * time.Second)

		os.Exit(1)
	}()

	go func() {
		log.Infoln("Now listening at :3003 for prometheus")
		err_utils.Must(http.ListenAndServe(":3003", promhttp.Handler()))
	}()

	app.StartSync()
}
