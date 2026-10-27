//go:build !nowebrtc

package torrent

import (
	"context"
	"errors"
	"fmt"
	"net"
	netHttp "net/http"
	"net/url"
	"sync"

	"github.com/anacrolix/log"
	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v4"

	"github.com/anacrolix/torrent/tracker"
	httpTracker "github.com/anacrolix/torrent/tracker/http"
	"github.com/anacrolix/torrent/webtorrent"
)

type websocketTrackerStatus struct {
	url url.URL
	tc  *webtorrent.TrackerClient
}

func (me websocketTrackerStatus) statusLine() string {
	return fmt.Sprintf("%+v", me.tc.Stats())
}

func (me websocketTrackerStatus) URL() *url.URL {
	return &me.url
}

func (me websocketTrackerStatus) Stop() {
}

type refCountedWebtorrentTrackerClient struct {
	webtorrent.TrackerClient
	refCount int
}

type websocketTrackers struct {
	PeerId                     [20]byte
	Logger                     log.Logger
	GetAnnounceRequest         func(event tracker.AnnounceEvent, infoHash [20]byte) (tracker.AnnounceRequest, error)
	OnConn                     func(webtorrent.DataChannelConn, webtorrent.DataChannelContext)
	mu                         sync.Mutex
	clients                    map[string]*refCountedWebtorrentTrackerClient
	Proxy                      httpTracker.ProxyFunc
	DialContext                func(ctx context.Context, network, addr string) (net.Conn, error)
	WebsocketTrackerHttpHeader func() netHttp.Header
	ICEServers                 []ICEServer
	callbacks                  *Callbacks
}

func (me *websocketTrackers) Get(url string, infoHash [20]byte) (*webtorrent.TrackerClient, func()) {
	me.mu.Lock()
	defer me.mu.Unlock()
	value, ok := me.clients[url]
	if !ok {
		dialer := &websocket.Dialer{
			Proxy:            me.Proxy,
			NetDialContext:   me.DialContext,
			HandshakeTimeout: websocket.DefaultDialer.HandshakeTimeout,
		}
		value = &refCountedWebtorrentTrackerClient{
			TrackerClient: webtorrent.TrackerClient{
				Dialer:             dialer,
				Url:                url,
				GetAnnounceRequest: me.GetAnnounceRequest,
				PeerId:             me.PeerId,
				OnConn:             me.OnConn,
				Logger: me.Logger.WithText(
					func(m log.Msg) string {
						return fmt.Sprintf("tracker client for %q: %v", url, m)
					},
				),
				WebsocketTrackerHttpHeader: me.WebsocketTrackerHttpHeader,
				ICEServers:                 me.ICEServers,
				OnConnected: func(err error) {
					for _, cb := range me.callbacks.StatusUpdated {
						cb(StatusUpdatedEvent{
							Event: TrackerConnected,
							Url:   url,
							Error: err,
						})
					}
				},
				OnDisconnected: func(err error) {
					for _, cb := range me.callbacks.StatusUpdated {
						cb(StatusUpdatedEvent{
							Event: TrackerDisconnected,
							Url:   url,
							Error: err,
						})
					}
				},
				OnAnnounceSuccessful: func(ih string) {
					for _, cb := range me.callbacks.StatusUpdated {
						cb(StatusUpdatedEvent{
							Event:    TrackerAnnounceSuccessful,
							Url:      url,
							InfoHash: ih,
						})
					}
				},
				OnAnnounceError: func(ih string, err error) {
					for _, cb := range me.callbacks.StatusUpdated {
						cb(StatusUpdatedEvent{
							Event:    TrackerAnnounceError,
							Url:      url,
							Error:    err,
							InfoHash: ih,
						})
					}
				},
			},
		}
		value.TrackerClient.Start(func(err error) {
			if err != nil {
				me.Logger.Printf("error running tracker client for %q: %v", url, err)
			}
		})
		if me.clients == nil {
			me.clients = make(map[string]*refCountedWebtorrentTrackerClient)
		}
		me.clients[url] = value
	}
	value.refCount++
	return &value.TrackerClient, func() {
		me.mu.Lock()
		defer me.mu.Unlock()
		value.TrackerClient.CloseOffersForInfohash(infoHash)
		value.refCount--
		if value.refCount == 0 {
			value.TrackerClient.Close()
			delete(me.clients, url)
		}
	}
}

func (cl *Client) initWebsocketTrackers() {
	cl.websocketTrackers = websocketTrackers{
		PeerId: cl.peerID,
		Logger: cl.logger.WithNames("websocketTrackers"),
		GetAnnounceRequest: func(
			event tracker.AnnounceEvent, infoHash [20]byte,
		) (
			tracker.AnnounceRequest, error,
		) {
			cl.lock()
			defer cl.unlock()
			t, ok := cl.torrentsByShortHash[infoHash]
			if !ok {
				return tracker.AnnounceRequest{}, errors.New("torrent not tracked by client")
			}
			return t.announceRequest(event, infoHash), nil
		},
		Proxy:                      cl.config.HTTPProxy,
		WebsocketTrackerHttpHeader: cl.config.WebsocketTrackerHttpHeader,
		ICEServers:                 cl.ICEServers(),
		DialContext:                cl.config.TrackerDialContext,
		callbacks:                  &cl.config.Callbacks,
		OnConn: func(dc webtorrent.DataChannelConn, dcc webtorrent.DataChannelContext) {
			cl.lock()
			defer cl.unlock()
			t, ok := cl.torrentsByShortHash[dcc.InfoHash]
			if !ok {
				cl.logger.WithDefaultLevel(log.Warning).Printf(
					"got webrtc conn for unloaded torrent with infohash %x",
					dcc.InfoHash,
				)
				dc.Close()
				return
			}
			go t.onWebRtcConn(dc, dcc)
		},
	}
}

func (t *Torrent) startWebsocketAnnouncer(u url.URL, shortInfohash [20]byte) torrentTrackerAnnouncer {
	wtc, release := t.cl.websocketTrackers.Get(u.String(), shortInfohash)
	// This needs to run before the Torrent is dropped from the Client, to prevent a new
	// webtorrent.TrackerClient for the same info hash before the old one is cleaned up.
	t.onClose = append(t.onClose, release)
	wst := websocketTrackerStatus{u, wtc}
	go func() {
		err := wtc.Announce(tracker.Started, shortInfohash)
		if err != nil {
			level := log.Warning
			if t.closed.IsSet() {
				level = log.Debug
			}
			t.logger.Levelf(level, "error doing initial announce to %q: %v", u.String(), err)
		}
	}()
	return wst
}

type webRtcStatsReports map[string]webrtc.StatsReport

func (t *Torrent) GetWebRtcPeerConnStats() map[string]webRtcStatsReports {
	stats := make(map[string]webRtcStatsReports)
	trackersMap := t.cl.websocketTrackers.clients
	for i, trackerClient := range trackersMap {
		ts := trackerClient.RtcPeerConnStats()
		stats[i] = ts
	}
	return stats
}
