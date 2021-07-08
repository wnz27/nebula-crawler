package crawl

import (
	"context"
	"database/sql"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p-core/host"

	"github.com/libp2p/go-libp2p-core/network"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/volatiletech/sqlboiler/v4/boil"
	"go.opencensus.io/stats"

	"github.com/dennis-tra/nebula-crawler/pkg/config"
	"github.com/dennis-tra/nebula-crawler/pkg/db"
	"github.com/dennis-tra/nebula-crawler/pkg/metrics"
	"github.com/dennis-tra/nebula-crawler/pkg/models"
	"github.com/dennis-tra/nebula-crawler/pkg/service"
)

const agentVersionRegexPattern = `\/?go-ipfs\/(?P<core>\d+\.\d+\.\d+)-?(?P<prerelease>\w+)?\/(?P<commit>\w+)?`

var agentVersionRegex = regexp.MustCompile(agentVersionRegexPattern)

// The Scheduler handles the scheduling and managing of workers that crawl the network
// as well as handling the crawl the results by persisting them in the database.
type Scheduler struct {
	*service.BaseScheduler

	// A map from peer.ID to peer.AddrInfo to indicate if a peer has already been crawled
	// in the past, so we don't put in the crawl queue again.
	crawled map[peer.ID]peer.AddrInfo

	// A map of agent versions and their occurrences that happened during the crawl.
	agentVersion map[string]int

	// A map of protocols and their occurrences that happened during the crawl.
	protocols map[string]int

	// A map of errors that happened during the crawl.
	errors map[string]int
}

// NewScheduler initializes a new libp2p host and scheduler instance.
func NewScheduler(ctx context.Context, dbh *sql.DB) (*Scheduler, error) {
	s := &Scheduler{
		crawled:      map[peer.ID]peer.AddrInfo{},
		agentVersion: map[string]int{},
		protocols:    map[string]int{},
		errors:       map[string]int{},
	}

	bs, err := service.NewBaseScheduler(ctx, dbh, s)
	if err != nil {
		return nil, errors.Wrap(err, "new scheduler")
	}
	s.BaseScheduler = bs

	return p, nil
}

func (s *Scheduler) NewWorker(host host.Host, conf *config.Config) (service.Worker, error) {
	return NewWorker(host, conf)
}

func (s *Scheduler) HandleResult(result service.Result) (*service.BaseWorker, error) {
	res := result.(Result)

	// update maps
	s.crawled[res.Peer.ID] = res.Peer
	stats.Record(s.ServiceContext(), metrics.CrawledPeersCount.M(1))

	stats.Record(s.ServiceContext(), metrics.PeersToCrawlCount.M(float64(len(s.inCrawlQueue))))

	// persist session information
	if err := s.upsertCrawlResult(res.Peer.ID, res.Peer.Addrs, res.Err); err != nil {
		log.WithError(err).Warnln("Could not update peer")
	}

	// track agent versions
	s.agentVersion[res.Agent] += 1

	// track seen protocols
	for _, p := range res.Protocols {
		s.protocols[p] += 1
	}

	// track error or schedule new crawls
	if res.Err != nil {

		if res.ErrKey() == models.FailureTypeUnknown {
			logEntry = logEntry.WithError(res.Error)
		}
		logEntry.Debugln("Error crawling peer")
	} else {
		for _, pi := range res.Neighbors {
			_, inCrawlQueue := s.inCrawlQueue[pi.ID]
			_, crawled := s.crawled[pi.ID]
			if !inCrawlQueue && !crawled {
				s.scheduleCrawl(pi)
			}
		}
	}

	if len(s.inCrawlQueue) == 0 || (s.config.CrawlLimit > 0 && len(s.crawled) >= s.config.CrawlLimit) {
		go s.Shutdown()
	}
	return nil
}

func (s *Scheduler) Setup() error {
	bootstrap, err := s.Config.BootstrapAddrInfos()
	if err != nil {
		return err
	}

	// Fill the queue with bootstrap nodes
	for _, b := range bootstrap {
		s.ScheduleJob(b)
	}
	return nil
}

func (s *Scheduler) Shutdown() error {
	defer func() {
		log.WithFields(log.Fields{
			"crawledPeers":    len(s.crawled),
			"crawlDuration":   time.Now().Sub(s.StartTime).String(),
			"dialablePeers":   len(s.crawled) - s.TotalErrors(),
			"undialablePeers": s.TotalErrors(),
		}).Infoln("Finished crawl")
	}()

	if s.Dbh != nil {
		crawl, err := s.persistCrawl(context.Background())
		if err != nil {
			return errors.Wrap(err, "persist crawl")
		}

		if err := s.persistPeerProperties(context.Background(), crawl.ID); err != nil {
			return errors.Wrap(err, "persist peer properties")
		}
	}
	return nil
}

// upsertCrawlResult inserts the given peer with its multi addresses in the database and
// upserts its currently active session
func (s *Scheduler) upsertCrawlResult(peerID peer.ID, maddrs []ma.Multiaddr, dialErr error) error {
	// Check if we're in a dry-run
	if s.Dbh == nil {
		return nil
	}

	startUpsert := time.Now()
	if dialErr == nil {
		if err := db.UpsertPeer(s.ServiceContext(), s.Dbh, peerID.Pretty(), maddrs); err != nil {
			return errors.Wrap(err, "upsert peer")
		}
		if err := db.UpsertSessionSuccess(s.Dbh, peerID.Pretty()); err != nil {
			return errors.Wrap(err, "upsert session success")
		}
	} else if dialErr != s.ServiceContext().Err() {
		if err := db.UpsertSessionError(s.Dbh, peerID.Pretty()); err != nil {
			return errors.Wrap(err, "upsert session error")
		}
	}
	stats.Record(s.ServiceContext(), metrics.CrawledUpsertDuration.M(millisSince(startUpsert)))
	return nil
}

// schedule crawl takes the address information and inserts it in the crawl queue in a separate
// go routine so we don't block the results handler. Buffered channels won't work here as there could
// be thousands of peers waiting to be crawled, so we spawn a separate go routine each time.
func (s *Scheduler) scheduleCrawl(pi peer.AddrInfo) {
	s.inCrawlQueue[pi.ID] = pi
	stats.Record(s.ServiceContext(), metrics.PeersToCrawlCount.M(float64(len(s.inCrawlQueue))))
	go func() {
		if s.IsStarted() {
			s.crawlQueue <- pi
		}
	}()
}

// persistCrawl writes crawl statistics to the database.
func (s *Scheduler) persistCrawl(ctx context.Context) (*models.Crawl, error) {
	log.Infoln("Persisting crawl result...")

	crawl := &models.Crawl{
		StartedAt:       s.StartTime,
		FinishedAt:      time.Now(),
		CrawledPeers:    len(s.crawled),
		DialablePeers:   len(s.crawled) - s.TotalErrors(),
		UndialablePeers: s.TotalErrors(),
	}

	return crawl, crawl.Insert(ctx, s.Dbh, boil.Infer())
}

// persistPeerProperties writes peer property statistics to the database.
func (s *Scheduler) persistPeerProperties(ctx context.Context, crawlID int) error {
	log.Infoln("Persisting peer properties...")

	// Extract full and core agent versions. Core agent versions are just strings like 0.8.0 or 0.5.0
	// The full agent versions have much more information e.g., /go-ipfs/0.4.21-dev/789dab3
	avFull := map[string]int{}
	avCore := map[string]int{}
	for version, count := range s.agentVersion {
		avFull[version] += count
		matches := agentVersionRegex.FindStringSubmatch(version)
		if matches != nil {
			avCore[matches[1]] += count
		}
	}

	txn, err := s.Dbh.BeginTx(ctx, nil)
	if err != nil {
		return errors.New("start txn")
	}

	for property, valuesMap := range map[string]map[string]int{
		"agent_version":      avFull,
		"agent_version_core": avCore,
		"protocol":           s.protocols,
		"error":              s.errors,
	} {
		for value, count := range valuesMap {
			pp := &models.PeerProperty{
				Property: property,
				Value:    value,
				Count:    count,
				CrawlID:  crawlID,
			}
			err := pp.Insert(ctx, txn, boil.Infer())
			if err != nil {
				log.WithError(err).WithFields(log.Fields{
					"property": property,
					"value":    value,
				}).Warnln("Could not insert peer property txn")
				continue
			}
		}
	}

	return txn.Commit()
}

// TotalErrors counts the total amount of errors - equivalent to undialable peers during this crawl.
func (s *Scheduler) TotalErrors() int {
	sum := 0
	for _, count := range s.errors {
		sum += count
	}
	return sum
}
