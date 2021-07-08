package service

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"github.com/dennis-tra/nebula-crawler/pkg/crawl"

	"github.com/libp2p/go-libp2p-core/peer"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/dennis-tra/nebula-crawler/pkg/config"
	"github.com/dennis-tra/nebula-crawler/pkg/models"
)

// knownErrors contains a list of known errors. Property key + string to match for
var knownErrors = map[string]string{
	models.FailureTypeIoTimeout:               "i/o timeout",
	models.FailureTypeConnectionRefused:       "connection refused",
	models.FailureTypeProtocolNotSupported:    "protocol not supported",
	models.FailureTypePeerIDMismatch:          "peer id mismatch",
	models.FailureTypeNoRouteToHost:           "no route to Host",
	models.FailureTypeNetworkUnreachable:      "network is unreachable",
	models.FailureTypeNoGoodAddresses:         "no good addresses",
	models.FailureTypeContextDeadlineExceeded: "context deadline exceeded",
	models.FailureTypeNoPublicIP:              "no public IP address",
	models.FailureTypeMaxDialAttemptsExceeded: "max dial attempts exceeded",
}

type BaseScheduler struct { // Service represents an entity that runs in a
	// separate go routine and where its lifecycle
	// needs to be handled externally.
	*Service

	// The libp2p node that's used to interact with the network. This one is also passed to all workers.
	Host host.Host

	// The database handle
	Dbh *sql.DB

	// The configuration of timeouts etc.
	Config *config.Config

	// The Queue instance acting as a thread safe buffer.
	Queue *Queue

	// A map from peer.ID to peer.AddrInfo to indicate if a peer was put in the Queue, so
	// we don't put it there again.
	inJobQueue sync.Map

	// The Queue that the workers publish their dial results on
	resultsQueue chan Result

	// The list of worker node references.
	workers sync.Map

	scheduler Scheduler
}

type Scheduler interface {
	NewWorker(host.Host, *config.Config) (*BaseWorker, error)
	Setup() error
	HandleResult(Result) (*BaseWorker, error)
	Shutdown() error
}

func NewBaseScheduler(ctx context.Context, dbh *sql.DB, scheduler *Scheduler) (*BaseScheduler, error) {
	conf, err := config.FromContext(ctx)
	if err != nil {
		return nil, err
	}

	// Set the timeout for dialing peers
	ctx = network.WithDialPeerTimeout(ctx, conf.DialTimeout)

	// Force direct dials will prevent swarm to run into dial backoff errors. It also prevents proxied connections.
	ctx = network.WithForceDirectDial(ctx, "prevent backoff")

	// Initialize a single libp2p node that's shared between all workers.
	priv, _, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
	if err != nil {
		return nil, err
	}

	h, err := libp2p.New(ctx, libp2p.Identity(priv), libp2p.NoListenAddrs)
	if err != nil {
		return nil, err
	}

	m := &BaseScheduler{
		Service:      New("scheduler"),
		Host:         h,
		Dbh:          dbh,
		Config:       conf,
		inJobQueue:   sync.Map{},
		Queue:        NewQueue(),
		resultsQueue: make(chan Result),
		workers:      sync.Map{},
		scheduler:    scheduler,
	}

	return m, nil
}

func (s *BaseScheduler) StartScheduling() error {
	s.ServiceStarted()
	defer s.ServiceStopped()

	// Start all workers
	if err := s.startWorkers(); err != nil {
		return errors.Wrap(err, "start workers")
	}

	if err := s.scheduler.Setup(); err != nil {
		return errors.Wrap(err, "setup scheduler")
	}

	// Async handle the results from workers
	go s.handleResults()

	// Monitor the database and schedule dial jobs
	// s.monitorDatabase()

	// release all resources
	s.cleanup()

	log.WithFields(log.Fields{
		"inJobQueue":      s.Queue.Length(),
		"monitorDuration": time.Now().Sub(s.StartTime).String(),
	}).Infoln("Finished monitoring")

	return nil
}

func (s *BaseScheduler) startWorkers() error {
	for i := 0; i < s.Config.MonitorWorkerCount; i++ {
		w, err := s.scheduler.NewWorker(s.Host, s.Config)
		if err != nil {
			return err
		}
		s.workers.Store(i, w)
		go w.Start(s.Queue.Consume(), s.resultsQueue)
	}
	return nil
}

func (s *BaseScheduler) handleResults() {
	for result := range s.resultsQueue {
		logEntry := log.WithFields(log.Fields{
			"workerID": result.WorkerID,
			"targetID": result.PeerIDPretty(),
			"alive":    result.Error == nil,
		})
		if result.Error != nil {
			logEntry = logEntry.WithError(result.Error())
		}
		logEntry.Infoln("Handling result from worker", result.WorkerID)

		// Update maps
		s.inJobQueue.Delete(result.PeerID())

		s.scheduler.HandleResult(result)

		logEntry.WithFields(map[string]interface{}{
			"inCrawlQueue": s.Queue.Length(),
			"crawled":      len(s.crawled),
		}).Infoln("Handled crawl result from worker", cr.WorkerID)
	}
}

func (s *BaseScheduler) ScheduleJob(pi peer.AddrInfo) {
	s.inJobQueue.Store(pi.ID, pi)
	s.Queue.push(&pi)
}

func (s *BaseScheduler) cleanup() {
	s.Queue.Close()
	s.shutdownWorkers()
	close(s.resultsQueue)
}

// shutdownWorkers sends shutdown signals to all workers and blocks until all have shut down.
func (s *BaseScheduler) shutdownWorkers() {
	var wg sync.WaitGroup
	s.workers.Range(func(_, worker interface{}) bool {
		w := worker.(BaseWorker)
		wg.Add(1)
		go func(w BaseWorker) {
			w.Shutdown()
			wg.Done()
		}(w)
		return true
	})
	wg.Wait()
}
