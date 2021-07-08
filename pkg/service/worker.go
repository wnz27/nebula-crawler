package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"go.uber.org/atomic"

	"github.com/dennis-tra/nebula-crawler/pkg/models"
	"github.com/libp2p/go-libp2p-core/peer"
)

var workerID = atomic.NewInt32(0)

type BaseResult struct {
	// The worker who derived this result
	Worker string

	// The crawled peer
	Peer peer.AddrInfo

	// Any error that has occurred during the crawl
	Err error
}

func NewBaseResult(workerID string, pi peer.AddrInfo) *BaseResult {
	return &BaseResult{
		Worker: workerID,
		Peer:   pi,
	}
}

type Result interface {
	WorkerID() string
	PeerID() peer.ID
	PeerIDPretty() string
	ErrKey() string
	Error() error
}

func (r *BaseResult) WorkerID() string {
	return r.Worker
}

func (r *BaseResult) ErrKey() string {
	for key, errStr := range knownErrors {
		if strings.Contains(r.Err.Error(), errStr) {
			return key
		}
	}
	return models.FailureTypeUnknown
}

func (r *BaseResult) PeerIDPretty() string {
	return r.Peer.ID.Pretty()[:16]
}

func (r *BaseResult) PeerID() peer.ID {
	return r.Peer.ID
}

func (r *BaseResult) Error() error {
	return r.Err
}

// BaseWorker encapsulates a libp2p Host that crawls the network.
type BaseWorker struct {
	*Service
	worker Worker
}

type Worker interface {
	HandleJob(context.Context, peer.AddrInfo) Result
}

func NewBaseWorker(worker Worker) *BaseWorker {
	return &BaseWorker{
		Service: New(fmt.Sprintf("worker-%02d", workerID.Inc())),
		worker:  worker,
	}
}

func (w *BaseWorker) Start(dialQueue <-chan *peer.AddrInfo, resultsQueue chan<- Result) {
	w.ServiceStarted()
	defer w.ServiceStopped()

	for pi := range dialQueue {
		start := time.Now()

		logEntry := log.WithFields(log.Fields{
			"workerID": w.Identifier(),
			"targetID": pi.ID.Pretty()[:16],
		})
		logEntry.Debugln("Handling peer")

		res := w.worker.HandleJob(w.ServiceContext(), *pi)

		select {
		case resultsQueue <- res:
		case <-w.SigShutdown():
			return
		}

		logEntry.WithFields(log.Fields{
			"duration": time.Since(start),
			"alive":    res.Error == nil,
		}).Debugln("Handled peer")
	}
}
