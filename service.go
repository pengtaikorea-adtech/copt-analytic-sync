package main

import (
	"log"

	"github.com/robfig/cron/v3"
)

type Service struct {
	crontab *cron.Cron
}

type CronJob struct {
	Schedule string
	Handler  func()
}

// Run implements cron job interface
func (job CronJob) Run() {
	job.Handler()
}

var Tasks = []CronJob{}

func NewService() Service {
	settings := GetConfigure(ConfigPath)
	srv := Service{
		crontab: cron.New(cron.WithSeconds()),
	}
	log.Print(" >> Service created")

	srv.crontab.AddFunc(settings.Schedule, RunTransferTables)

	// for _, t := range Tasks {
	// 	srv.crontab.AddJob(t.Schedule, t)
	// }

	return srv
}

func (srv Service) Start() {
	srv.crontab.Start()
}

func (srv Service) Stop() {
	srv.crontab.Stop()
}
