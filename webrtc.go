//go:build !nowebrtc

package torrent

import (
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"

	"github.com/anacrolix/log"
	"github.com/anacrolix/missinggo/v2"
	"github.com/pion/webrtc/v4"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/anacrolix/torrent/webtorrent"
)

const webrtcNetwork = "webrtc"

// ICEServer describes a STUN / TURN server for WebRTC. With the nowebrtc build tag it is a
// stand-in struct with the same fields.
type ICEServer = webrtc.ICEServer

type webrtcNetConn struct {
	io.ReadWriteCloser
	webtorrent.DataChannelContext
}

type webrtcNetAddr struct {
	*webrtc.ICECandidate
}

var _ net.Addr = webrtcNetAddr{}

func (webrtcNetAddr) Network() string {
	// Now that we have the ICE candidate, we can tell if it's over udp or tcp. But should we use
	// that for the network?
	return webrtcNetwork
}

func (me webrtcNetAddr) String() string {
	return net.JoinHostPort(me.Address, strconv.FormatUint(uint64(me.Port), 10))
}

func (me webrtcNetConn) LocalAddr() net.Addr {
	// I'm not sure if this evolves over time. It might also be unavailable if the PeerConnection is
	// closed or closes itself. The same concern applies to RemoteAddr.
	pair, err := me.DataChannelContext.GetSelectedIceCandidatePair()
	if err != nil {
		panic(err)
	}
	return webrtcNetAddr{pair.Local}
}

func (me webrtcNetConn) RemoteAddr() net.Addr {
	// See comments on LocalAddr.
	pair, err := me.DataChannelContext.GetSelectedIceCandidatePair()
	if err != nil {
		panic(err)
	}
	return webrtcNetAddr{pair.Remote}
}

// Do we need these for WebRTC connections exposed as net.Conns? Can we set them somewhere inside
// PeerConnection or on the channel or some transport?

func (w webrtcNetConn) SetDeadline(t time.Time) error {
	w.Span.AddEvent("SetDeadline", trace.WithAttributes(attribute.String("time", t.String())))
	return nil
}

func (w webrtcNetConn) SetReadDeadline(t time.Time) error {
	w.Span.AddEvent("SetReadDeadline", trace.WithAttributes(attribute.String("time", t.String())))
	return nil
}

func (w webrtcNetConn) SetWriteDeadline(t time.Time) error {
	w.Span.AddEvent("SetWriteDeadline", trace.WithAttributes(attribute.String("time", t.String())))
	return nil
}

func (w webrtcNetConn) Read(b []byte) (n int, err error) {
	_, span := otel.Tracer(tracerName).Start(w.Context, "Read")
	defer span.End()
	span.SetAttributes(attribute.Int("buf_len", len(b)))
	n, err = w.ReadWriteCloser.Read(b)
	span.RecordError(err)
	span.SetAttributes(attribute.Int("bytes_read", n))
	return
}

func (cl *Client) ICEServers() []ICEServer {
	var ICEServers []ICEServer
	if cl.config.ICEServerList != nil {
		ICEServers = cl.config.ICEServerList
	} else if cl.config.ICEServers != nil {
		ICEServers = []ICEServer{{URLs: cl.config.ICEServers}}
	}
	return ICEServers
}

func (t *Torrent) onWebRtcConn(
	c webtorrent.DataChannelConn,
	dcc webtorrent.DataChannelContext,
) {
	defer c.Close()
	netConn := webrtcNetConn{
		ReadWriteCloser:    c,
		DataChannelContext: dcc,
	}
	peerRemoteAddr := netConn.RemoteAddr()
	//t.logger.Levelf(log.Critical, "onWebRtcConn remote addr: %v", peerRemoteAddr)
	if t.cl.badPeerAddr(peerRemoteAddr) {
		return
	}
	localAddrIpPort := missinggo.IpPortFromNetAddr(netConn.LocalAddr())

	pc, err := t.cl.initiateProtocolHandshakes(
		context.Background(),
		netConn,
		t,
		false,
		newConnectionOpts{
			outgoing:        dcc.LocalOffered,
			remoteAddr:      peerRemoteAddr,
			localPublicAddr: localAddrIpPort,
			network:         webrtcNetwork,
			connString:      fmt.Sprintf("webrtc offer_id %x: %v", dcc.OfferId, regularNetConnPeerConnConnString(netConn)),
		},
	)
	if err != nil {
		t.logger.WithDefaultLevel(log.Error).Printf("error in handshaking webrtc connection: %v", err)
		return
	}
	if dcc.LocalOffered {
		pc.Discovery = PeerSourceTracker
	} else {
		pc.Discovery = PeerSourceIncoming
	}
	pc.conn.SetWriteDeadline(time.Time{})
	t.cl.lock()
	defer t.cl.unlock()
	err = t.runHandshookConn(pc)
	if err != nil {
		t.logger.WithDefaultLevel(log.Debug).Printf("error running handshook webrtc conn: %v", err)
	}
}
