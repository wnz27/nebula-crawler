package service

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/dennis-tra/nebula-crawler/pkg/config"
	"github.com/dennis-tra/nebula-crawler/pkg/db"
	"github.com/dennis-tra/nebula-crawler/pkg/metrics"
	"github.com/dennis-tra/nebula-crawler/pkg/models"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	ma "github.com/multiformats/go-multiaddr"
	log "github.com/sirupsen/logrus"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
	"go.uber.org/atomic"
)

type Scheduler struct { // Service represents an entity that runs in a
	// separate go routine and where its lifecycle
	// needs to be handled externally.
	*Service

	// The libp2p node that's used to interact with the network. This one is also passed to all workers.
	host host.Host

	// The database handle
	dbh *sql.DB

	// The configuration of timeouts etc.
	config *config.Config

	// The queue of peer.AddrInfo's that need to be dialed to.
	jobQueue chan peer.AddrInfo

	// A map from peer.ID to peer.AddrInfo to indicate if a peer was put in the queue, so
	// we don't put it there again.
	inJobQueue sync.Map

	// The number of peers in the ping queue.
	inJobQueueCount atomic.Uint32

	// The queue that the workers publish their dial results on
	resultsQueue chan interface{}

	// The list of worker node references.
	workers sync.Map

	workerGen NewWorker
}

type NewWorker func(host.Host, *sql.DB, *config.Config) (Worker, error)

type Worker interface {
	Start(<-chan peer.AddrInfo, chan<- interface{})
	Shutdown()
}

func NewScheduler(ctx context.Context, dbh *sql.DB, workerGen NewWorker) (*Scheduler, error) {
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

	m := &Scheduler{
		Service:      New("scheduler"),
		host:         h,
		dbh:          dbh,
		config:       conf,
		inJobQueue:   sync.Map{},
		jobQueue:     make(chan peer.AddrInfo),
		resultsQueue: make(chan interface{}),
		workers:      sync.Map{},
		workerGen:    workerGen,
	}

	return m, nil
}

func (s *Scheduler) StartScheduling() error {
	s.ServiceStarted()
	defer s.ServiceStopped()

	// Start all workers
	if err := s.startWorkers(); err != nil {
		return errors.Wrap(err, "start workers")
	}

	// Async handle the results from workers
	go s.handleResults()

	// Monitor the database and schedule dial jobs
	// s.monitorDatabase()

	// release all resources
	s.cleanup()

	log.WithFields(log.Fields{
		"inJobQueue":      s.inJobQueueCount.Load(),
		"monitorDuration": time.Now().Sub(s.StartTime).String(),
	}).Infoln("Finished monitoring")

	return nil
}

func (s *Scheduler) startWorkers() error {
	for i := 0; i < s.config.MonitorWorkerCount; i++ {
		w, err := s.workerGen(s.host, s.dbh, s.config)
		if err != nil {
			return err
		}
		s.workers.Store(i, w)
		go w.Start(s.jobQueue, s.resultsQueue)
	}
	return nil
}

func (s *Scheduler) handleResults() {
	for dr := range s.resultsQueue {
		logEntry := log.WithFields(log.Fields{
			"workerID": dr.WorkerID,
			"targetID": dr.Peer.ID.Pretty()[:16],
			"alive":    dr.Error == nil,
		})
		if dr.Error != nil {
			logEntry = logEntry.WithError(dr.Error)
		}
		logEntry.Infoln("Handling dial result from worker", dr.WorkerID)

		// Update maps
		s.inJobQueue.Delete(dr.Peer.ID)
		stats.Record(s.ServiceContext(), metrics.PeersToDialCount.M(float64(s.inJobQueueCount.Dec())))

		errKey := "unknown"
		for key, errStr := range knownErrors {
			if strings.Contains(dr.Error.Error(), errStr) {
				errKey = key
				break
			}
		}

		if ctx, err := tag.New(s.ServiceContext(), tag.Upsert(metrics.KeyError, errKey)); err == nil {
			stats.Record(ctx, metrics.PeersToDialErrorsCount.M(1))
		}

		var err error
		if dr.Error == nil {
			err = db.UpsertSessionSuccess(s.dbh, dr.Peer.ID.Pretty())
		} else {
			err = db.UpsertSessionErrorTS(s.dbh, dr.Peer.ID.Pretty(), dr.FirstFailedDial)
		}

		if err != nil {
			logEntry.WithError(err).Warn("Could not update session record")
		}
	}
}

func (s *Scheduler) EnqueueJob(pi peer.AddrInfo) {
	// Schedule job for peer
	go func() {
		if s.IsStarted() {
			s.jobQueue <- pi
		}
	}()
}

func (s *Scheduler) cleanup() {
	s.drainJobQueue()
	close(s.jobQueue)
	s.shutdownWorkers()
	close(s.resultsQueue)
}

// drainJobQueue reads all entries from crawlQueue and discards them.
func (s *Scheduler) drainJobQueue() {
	for {
		select {
		case pi := <-s.jobQueue:
			log.WithField("targetID", pi.ID.Pretty()[:16]).Debugln("Drained peer")
		default:
			return
		}
	}
}

// shutdownWorkers sends shutdown signals to all workers and blocks until all have shut down.
func (s *Scheduler) shutdownWorkers() {
	var wg sync.WaitGroup
	s.workers.Range(func(_, worker interface{}) bool {
		w := worker.(Worker)
		wg.Add(1)
		go func(w Worker) {
			w.Shutdown()
			wg.Done()
		}(w)
		return true
	})
	wg.Wait()
}
