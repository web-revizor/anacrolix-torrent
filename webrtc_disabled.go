//go:build nowebrtc

// Stand-ins used when building with -tags nowebrtc: no pion/webrtc and no webtorrent client are
// linked, websocket (ws / wss) trackers are ignored and WebRTC peers are never created.

package torrent

import "net/url"

// ICEServer mirrors the fields of webrtc.ICEServer that are meaningful without WebRTC. It is not
// used for anything when the nowebrtc tag is set.
type ICEServer struct {
	URLs       []string
	Username   string
	Credential any
}

type websocketTrackers struct{}

func (cl *Client) initWebsocketTrackers() {}

func (cl *Client) ICEServers() []ICEServer { return nil }

func (t *Torrent) startWebsocketAnnouncer(u url.URL, shortInfohash [20]byte) torrentTrackerAnnouncer {
	return nil
}

type webRtcStatsReports map[string]any

func (t *Torrent) GetWebRtcPeerConnStats() map[string]webRtcStatsReports {
	return nil
}
