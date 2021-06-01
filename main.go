package main

import (
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sys/windows/svc"
	"golang.org/x/sys/windows/svc/debug"
	"golang.org/x/sys/windows/svc/mgr"
)

const (
	ServiceName        = "ptkCronSync"
	ServiceDisplayName = "PTK AnalyticMart CronSync"
)

// WindowsService to run
type WinService struct {
	service Service
}

func (ws *WinService) execPath() string {
	path, err := filepath.Abs(os.Args[0])
	errorCheck(err, -9, "can not specify the path", path)
	return path

}

func (ws *WinService) proceedRunning(s chan<- svc.Status) {
	// send pending start signal
	s <- svc.Status{
		State: svc.StartPending,
	}

	ws.service = NewService()

	// start crontab service
	ws.service.Start()

	// send running signal
	s <- svc.Status{
		State:   svc.Running,
		Accepts: svc.AcceptStop | svc.AcceptShutdown,
	}
}

func (ws *WinService) proceedStopped(s chan<- svc.Status) {
	s <- svc.Status{
		State: svc.StopPending,
	}
	ws.service.Stop()

	s <- svc.Status{
		State:   svc.Stopped,
		Accepts: svc.AcceptShutdown,
	}
}

func (ws *WinService) Execute(args []string, r <-chan svc.ChangeRequest, s chan<- svc.Status) (bool, uint32) {
	log.Print("try to start the service...")
	ws.proceedRunning(s)

	log.Print("the service has started successfully")
	for {
		req := <-r
		switch req.Cmd {
		case svc.Stop, svc.Shutdown:
			ws.proceedStopped(s)
			goto stop
		default:
			time.Sleep(1 * time.Millisecond)

		}
	}

stop:
	return true, 0
}

func errorCheck(err error, exitCode int, msgs ...string) {
	if err != nil {
		log.Fatal(err)
		if 0 < len(msgs) {
			// print log messages
			for _, m := range msgs {
				log.Fatal(m)
			}
		}
		os.Exit(exitCode)
	}
}

func Run(modeDebug bool) error {
	if modeDebug {
		return debug.Run(ServiceName, &WinService{})
	} else {
		return svc.Run(ServiceName, &WinService{})
	}
}

// install the service
func installTheService(manager *mgr.Mgr) {
	ws := &WinService{}
	conf := mgr.Config{
		DisplayName: ServiceDisplayName,
	}
	srv, err := manager.CreateService(ServiceName, ws.execPath(), conf, "is", "auto-started")
	errorCheck(err, -1, "can not create the service")
	defer srv.Close()
}

// uninstall the service
func uninstallTheService(manager *mgr.Mgr) {
	srv, err := manager.OpenService(ServiceName)
	errorCheck(err, -1, "can not open service")
	err = srv.Delete()
	errorCheck(err, -1, "can not remove the service")
}

func startTheService(service *mgr.Service) {
	err := service.Start("is", "manual-started")
	errorCheck(err, -1, "can not start the service")
}

func stopTheService(service *mgr.Service) {

	state, err := service.Control(svc.Stop)
	for retries := 10; 0 < retries; retries -= 1 {
		errorCheck(err, -2, "service control error")
		// update
		state, err = service.Query()
		errorCheck(err, -2, "service state unreachable")
		if state.State == svc.Stopped {
			return
		}
		// time delay
		time.Sleep(200 * time.Millisecond)
	}

	if state.State != svc.Stopped {
		log.Fatal("could not stop the service")
	}
}

func controlService(cmd string) {
	manager, err := mgr.Connect()
	errorCheck(err, -1, "can not connect the Service Manager")
	defer manager.Disconnect()

	if strings.ToLower(cmd) == "install" {
		installTheService(manager)
	} else {
		// not install
		service, err := manager.OpenService(ServiceName)
		errorCheck(err, -2, "Can not open the service")
		defer service.Close()
		switch strings.ToLower(cmd) {
		case "path":
			ws := &WinService{}
			log.Printf("binPath= `%s`", ws.execPath())
		case "config":
			cf, err := service.Config()
			errorCheck(err, -9)
			log.Print(cf)
		case "uninstall":
			uninstallTheService(manager)
		case "debug":
			err = Run(true)
			errorCheck(err, -1, "debug run failed")
		case "start":
			// start
			startTheService(service)
		case "stop":
			stopTheService(service)
		case "restart":
			stopTheService(service)
			startTheService(service)
		case "transfer":
			RunTransferTables()
			RunTransferViews()
		case "tables":
			RunTransferTables()
		case "views":
			RunTransferViews()
		default:
			log.Fatalf("invalid command : %s", cmd)
			log.Fatal("Command must be in one of (debug | install | uninstall | start | stop | restart)")
			return
		}
	}

}

func main() {
	if modeService, _ := svc.IsWindowsService(); modeService {
		// service run mode
		Run(false)
	} else {
		controlService(os.Args[1])
	}
}
