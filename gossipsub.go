package pubsub

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"math/rand"
	"slices"
	"sort"
	"sync/atomic"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"

	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/core/record"
	"github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoremem"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
	"google.golang.org/protobuf/proto"
)

const (
	// GossipSubID_v10 is the protocol ID for version 1.0.0 of the GossipSub protocol.
	// It is advertised along with GossipSubID_v11 and GossipSubID_v12 for backwards compatibility.
	GossipSubID_v10 = protocol.ID("/meshsub/1.0.0")

	// GossipSubID_v11 is the protocol ID for version 1.1.0 of the GossipSub protocol.
	// It is advertised along with GossipSubID_v12 for backwards compatibility.
	// See the spec for details about how v1.1.0 compares to v1.0.0:
	// https://github.com/libp2p/specs/blob/master/pubsub/gossipsub/gossipsub-v1.1.md
	GossipSubID_v11 = protocol.ID("/meshsub/1.1.0")

	// GossipSubID_v12 is the protocol ID for version 1.2.0 of the GossipSub protocol.
	// See the spec for details about how v1.2.0 compares to v1.1.0:
	// https://github.com/libp2p/specs/blob/master/pubsub/gossipsub/gossipsub-v1.2.md
	GossipSubID_v12 = protocol.ID("/meshsub/1.2.0")

	// GossipSubID_v13 is the protocol ID for version 1.3.0 of the GossipSub
	// protocol. It adds the extensions control message.
	GossipSubID_v13 = protocol.ID("/meshsub/1.3.0")
)

// Defines the default gossipsub parameters.
var (
	GossipSubD                                = 6
	GossipSubDlo                              = 5
	GossipSubDhi                              = 12
	GossipSubDscore                           = 4
	GossipSubDout                             = 2
	GossipSubHistoryLength                    = 5
	GossipSubHistoryGossip                    = 3
	GossipSubDlazy                            = 6
	GossipSubGossipFactor                     = 0.25
	GossipSubGossipRetransmission             = 3
	GossipSubHeartbeatInitialDelay            = 100 * time.Millisecond
	GossipSubHeartbeatInterval                = 1 * time.Second
	GossipSubFanoutTTL                        = 60 * time.Second
	GossipSubPrunePeers                       = 16
	GossipSubPruneBackoff                     = time.Minute
	GossipSubUnsubscribeBackoff               = 10 * time.Second
	GossipSubConnectors                       = 8
	GossipSubMaxPendingConnections            = 128
	GossipSubConnectionTimeout                = 30 * time.Second
	GossipSubDirectConnectTicks        uint64 = 300
	GossipSubDirectConnectInitialDelay        = time.Second
	GossipSubOpportunisticGraftTicks   uint64 = 60
	GossipSubOpportunisticGraftPeers          = 2
	GossipSubGraftFloodThreshold              = 10 * time.Second
	GossipSubMaxIHaveLength                   = 5000
	GossipSubMaxIHaveMessages                 = 10
	GossipSubMaxIDontWantLength               = 10
	GossipSubMaxIDontWantMessages             = 1000
	GossipSubIWantFollowupTime                = 3 * time.Second
	GossipSubIDontWantMessageThreshold        = 1024 // 1KB
	GossipSubIDontWantMessageTTL              = 3    // 3 heartbeats
)

type checksum struct {
	payload [32]byte
	length  uint8
}

// GossipSubParams defines all the gossipsub specific parameters.
type GossipSubParams struct {
	// overlay parameters.

	// D sets the optimal degree for a GossipSub topic mesh. For example, if D == 6,
	// each peer will want to have about six peers in their mesh for each topic they're subscribed to.
	// D should be set somewhere between Dlo and Dhi.
	D int

	// Dlo sets the lower bound on the number of peers we keep in a GossipSub topic mesh.
	// If we have fewer than Dlo peers, we will attempt to graft some more into the mesh at
	// the next heartbeat.
	Dlo int

	// Dhi sets the upper bound on the number of peers we keep in a GossipSub topic mesh.
	// If we have more than Dhi peers, we will select some to prune from the mesh at the next heartbeat.
	Dhi int

	// Dscore affects how peers are selected when pruning a mesh due to over subscription.
	// At least Dscore of the retained peers will be high-scoring, while the remainder are
	// chosen randomly.
	Dscore int

	// Dout sets the quota for the number of outbound connections to maintain in a topic mesh.
	// When the mesh is pruned due to over subscription, we make sure that we have outbound connections
	// to at least Dout of the survivor peers. This prevents sybil attackers from overwhelming
	// our mesh with incoming connections.
	//
	// Dout must be set below Dlo, and must not exceed D / 2.
	Dout int

	// gossip parameters

	// HistoryLength controls the size of the message cache used for gossip.
	// The message cache will remember messages for HistoryLength heartbeats.
	HistoryLength int

	// HistoryGossip controls how many cached message ids we will advertise in
	// IHAVE gossip messages. When asked for our seen message IDs, we will return
	// only those from the most recent HistoryGossip heartbeats. The slack between
	// HistoryGossip and HistoryLength allows us to avoid advertising messages
	// that will be expired by the time they're requested.
	//
	// HistoryGossip must be less than or equal to HistoryLength to
	// avoid a runtime panic.
	HistoryGossip int

	// Dlazy affects how many peers we will emit gossip to at each heartbeat.
	// We will send gossip to at least Dlazy peers outside our mesh. The actual
	// number may be more, depending on GossipFactor and how many peers we're
	// connected to.
	Dlazy int

	// GossipFactor affects how many peers we will emit gossip to at each heartbeat.
	// We will send gossip to GossipFactor * (total number of non-mesh peers), or
	// Dlazy, whichever is greater.
	GossipFactor float64

	// GossipRetransmission controls how many times we will allow a peer to request
	// the same message id through IWANT gossip before we start ignoring them. This is designed
	// to prevent peers from spamming us with requests and wasting our resources.
	GossipRetransmission int

	// heartbeat interval

	// HeartbeatInitialDelay is the short delay before the heartbeat timer begins
	// after the router is initialized.
	HeartbeatInitialDelay time.Duration

	// HeartbeatInterval controls the time between heartbeats.
	HeartbeatInterval time.Duration

	// SlowHeartbeatWarning is the duration threshold for heartbeat processing before emitting
	// a warning; this would be indicative of an overloaded peer.
	SlowHeartbeatWarning float64

	// FanoutTTL controls how long we keep track of the fanout state. If it's been
	// FanoutTTL since we've published to a topic that we're not subscribed to,
	// we'll delete the fanout map for that topic.
	FanoutTTL time.Duration

	// PrunePeers controls the number of peers to include in prune Peer eXchange.
	// When we prune a peer that's eligible for PX (has a good score, etc), we will try to
	// send them signed peer records for up to PrunePeers other peers that we
	// know of.
	PrunePeers int

	// PruneBackoff controls the backoff time for pruned peers. This is how long
	// a peer must wait before attempting to graft into our mesh again after being pruned.
	// When pruning a peer, we send them our value of PruneBackoff so they know
	// the minimum time to wait. Peers running older versions may not send a backoff time,
	// so if we receive a prune message without one, we will wait at least PruneBackoff
	// before attempting to re-graft.
	PruneBackoff time.Duration

	// UnsubscribeBackoff controls the backoff time to use when unsuscribing
	// from a topic. A peer should not resubscribe to this topic before this
	// duration.
	UnsubscribeBackoff time.Duration

	// Connectors controls the number of active connection attempts for peers obtained through PX.
	Connectors int

	// MaxPendingConnections sets the maximum number of pending connections for peers attempted through px.
	MaxPendingConnections int

	// ConnectionTimeout controls the timeout for connection attempts.
	ConnectionTimeout time.Duration

	// DirectConnectTicks is the number of heartbeat ticks for attempting to reconnect direct peers
	// that are not currently connected.
	DirectConnectTicks uint64

	// DirectConnectInitialDelay is the initial delay before opening connections to direct peers
	DirectConnectInitialDelay time.Duration

	// OpportunisticGraftTicks is the number of heartbeat ticks for attempting to improve the mesh
	// with opportunistic grafting. Every OpportunisticGraftTicks we will attempt to select some
	// high-scoring mesh peers to replace lower-scoring ones, if the median score of our mesh peers falls
	// below a threshold (see https://godoc.org/github.com/libp2p/go-libp2p-pubsub#PeerScoreThresholds).
	OpportunisticGraftTicks uint64

	// OpportunisticGraftPeers is the number of peers to opportunistically graft.
	OpportunisticGraftPeers int

	// If a GRAFT comes before GraftFloodThreshold has elapsed since the last PRUNE,
	// then there is an extra score penalty applied to the peer through P7.
	GraftFloodThreshold time.Duration

	// MaxIHaveLength is the maximum number of messages to include in an IHAVE message.
	// Also controls the maximum number of IHAVE ids we will accept and request with IWANT from a
	// peer within a heartbeat, to protect from IHAVE floods. You should adjust this value from the
	// default if your system is pushing more than 5000 messages in HistoryGossip heartbeats;
	// with the defaults this is 1666 messages/s.
	MaxIHaveLength int

	// MaxIHaveMessages is the maximum number of IHAVE messages to accept from a peer within a heartbeat.
	MaxIHaveMessages int

	// MaxIDontWantLength is the maximum number of messages to include in an IDONTWANT message. Also controls
	// the maximum number of IDONTWANT ids we will accept to protect against IDONTWANT floods. This value
	// should be adjusted if your system anticipates a larger amount than specified per heartbeat.
	MaxIDontWantLength int
	// MaxIDontWantMessages is the maximum number of IDONTWANT messages to accept from a peer within a heartbeat.
	MaxIDontWantMessages int

	// Time to wait for a message requested through IWANT following an IHAVE advertisement.
	// If the message is not received within this window, a broken promise is declared and
	// the router may apply bahavioural penalties.
	IWantFollowupTime time.Duration

	// IDONTWANT is only sent for messages larger than the threshold. This should be greater than
	// D_high * the size of the message id. Otherwise, the attacker can do the amplication attack by sending
	// small messages while the receiver replies back with larger IDONTWANT messages.
	IDontWantMessageThreshold int

	// IDONTWANT is cleared when it's older than the TTL.
	IDontWantMessageTTL int
}

func (params *GossipSubParams) validate() error {
	if !(params.HistoryGossip <= params.HistoryLength) {
		return fmt.Errorf("param HistoryGossip=%d must be less than or equal to HistoryLength=%d", params.HistoryGossip, params.HistoryLength)
	}

	if !(params.Dscore <= params.Dhi) {
		return fmt.Errorf("param Dscore=%d must be lower than or equal to  Dhi=%d", params.Dscore, params.Dhi)
	}

	// Bootstrappers set D=D_lo=D_hi=D_out=0
	// See https://github.com/libp2p/specs/blob/master/pubsub/gossipsub/gossipsub-v1.1.md#recommendations-for-network-operators
	if params.D == 0 && params.Dlo == 0 && params.Dhi == 0 && params.Dout == 0 {
		return nil
	}

	if !(params.Dlo <= params.D && params.D <= params.Dhi) {
		return fmt.Errorf("param D=%d must be between Dlo=%d and Dhi=%d", params.D, params.Dlo, params.Dhi)
	}

	if !(params.Dscore <= params.Dhi) {
		return fmt.Errorf("param Dscore=%d must be less than Dhi=%d", params.Dscore, params.Dhi)
	}

	if !(params.Dout < params.Dlo && params.Dout < (params.D/2)) {
		return fmt.Errorf("param Dout=%d must be less than Dlo=%d and Dout must not exceed D=%d / 2", params.Dout, params.Dlo, params.D)
	}

	return nil
}

// NewGossipSub returns a new PubSub object using the default GossipSubRouter as the router.
func NewGossipSub(ctx context.Context, h host.Host, opts ...Option) (*PubSub, error) {
	rt := DefaultGossipSubRouter(h)
	opts = append(opts, WithRawTracer(rt.tagTracer))
	return NewGossipSubWithRouter(ctx, h, rt, opts...)
}

// NewGossipSubWithRouter returns a new PubSub object using the given router.
func NewGossipSubWithRouter(ctx context.Context, h host.Host, rt PubSubRouter, opts ...Option) (*PubSub, error) {
	return NewPubSub(ctx, h, rt, opts...)
}

// DefaultGossipSubRouter returns a new GossipSubRouter with default parameters.
func DefaultGossipSubRouter(h host.Host) *GossipSubRouter {
	params := DefaultGossipSubParams()
	rt := &GossipSubRouter{
		peers:           make(map[peer.ID]protocol.ID),
		mesh:            make(map[string]map[peer.ID]struct{}),
		fanout:          make(map[string]map[peer.ID]struct{}),
		lastpub:         make(map[string]int64),
		gossip:          make(map[peer.ID][]*pb.ControlIHave),
		control:         make(map[peer.ID]*pb.ControlMessage),
		backoff:         make(map[string]map[peer.ID]time.Time),
		peerhave:        make(map[peer.ID]int),
		peerdontwant:    make(map[peer.ID]int),
		unwanted:        make(map[peer.ID]map[checksum]int),
		iasked:          make(map[peer.ID]int),
		outbound:        make(map[peer.ID]bool),
		connect:         make(chan connectInfo, params.MaxPendingConnections),
		cab:             pstoremem.NewAddrBook(),
		mcache:          NewMessageCache(params.HistoryGossip, params.HistoryLength),
		protos:          GossipSubDefaultProtocols,
		feature:         GossipSubDefaultFeatures,
		tagTracer:       newTagTracer(h.ConnManager()),
		params:          params,
		reducePXRecords: defaultPXRecordReducer,
	}

	rt.extensions = newExtensionsState(PeerExtensions{}, func(p peer.ID) {
		if rt.score != nil {
			rt.score.AddPenalty(p, 10)
		}
	}, rt.sendRPC)

	return rt
}

// DefaultGossipSubParams returns the default gossip sub parameters
// as a config.
func DefaultGossipSubParams() GossipSubParams {
	return GossipSubParams{
		D:                         GossipSubD,
		Dlo:                       GossipSubDlo,
		Dhi:                       GossipSubDhi,
		Dscore:                    GossipSubDscore,
		Dout:                      GossipSubDout,
		HistoryLength:             GossipSubHistoryLength,
		HistoryGossip:             GossipSubHistoryGossip,
		Dlazy:                     GossipSubDlazy,
		GossipFactor:              GossipSubGossipFactor,
		GossipRetransmission:      GossipSubGossipRetransmission,
		HeartbeatInitialDelay:     GossipSubHeartbeatInitialDelay,
		HeartbeatInterval:         GossipSubHeartbeatInterval,
		FanoutTTL:                 GossipSubFanoutTTL,
		PrunePeers:                GossipSubPrunePeers,
		PruneBackoff:              GossipSubPruneBackoff,
		UnsubscribeBackoff:        GossipSubUnsubscribeBackoff,
		Connectors:                GossipSubConnectors,
		MaxPendingConnections:     GossipSubMaxPendingConnections,
		ConnectionTimeout:         GossipSubConnectionTimeout,
		DirectConnectTicks:        GossipSubDirectConnectTicks,
		DirectConnectInitialDelay: GossipSubDirectConnectInitialDelay,
		OpportunisticGraftTicks:   GossipSubOpportunisticGraftTicks,
		OpportunisticGraftPeers:   GossipSubOpportunisticGraftPeers,
		GraftFloodThreshold:       GossipSubGraftFloodThreshold,
		MaxIHaveLength:            GossipSubMaxIHaveLength,
		MaxIHaveMessages:          GossipSubMaxIHaveMessages,
		MaxIDontWantLength:        GossipSubMaxIDontWantLength,
		MaxIDontWantMessages:      GossipSubMaxIDontWantMessages,
		IWantFollowupTime:         GossipSubIWantFollowupTime,
		IDontWantMessageThreshold: GossipSubIDontWantMessageThreshold,
		IDontWantMessageTTL:       GossipSubIDontWantMessageTTL,
		SlowHeartbeatWarning:      0.1,
	}
}

// WithPeerScore is a gossipsub router option that enables peer scoring.
func WithPeerScore(params *PeerScoreParams, thresholds *PeerScoreThresholds) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("pubsub router is not gossipsub")
		}

		// sanity check: validate the score parameters
		err := params.validate()
		if err != nil {
			return err
		}

		// sanity check: validate the threshold values
		err = thresholds.validate()
		if err != nil {
			return err
		}

		gs.score = newPeerScore(params, ps.logger)
		gs.gossipThreshold = thresholds.GossipThreshold
		gs.publishThreshold = thresholds.PublishThreshold
		gs.graylistThreshold = thresholds.GraylistThreshold
		gs.acceptPXThreshold = thresholds.AcceptPXThreshold
		gs.opportunisticGraftThreshold = thresholds.OpportunisticGraftThreshold

		gs.gossipTracer = newGossipTracer()

		// hook the tracer
		if ps.tracer != nil {
			ps.tracer.raw = append(ps.tracer.raw, gs.score, gs.gossipTracer)
		} else {
			ps.tracer = &pubsubTracer{
				raw:   []RawTracer{gs.score, gs.gossipTracer},
				pid:   ps.host.ID(),
				idGen: ps.idGen,
			}
		}

		return nil
	}
}

// WithFloodPublish is a gossipsub router option that enables flood publishing.
// When this is enabled, published messages are forwarded to all peers with score >=
// to publishThreshold
func WithFloodPublish(floodPublish bool) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("pubsub router is not gossipsub")
		}

		gs.floodPublish = floodPublish

		return nil
	}
}

// WithPeerExchange is a gossipsub router option that enables Peer eXchange on PRUNE.
// This should generally be enabled in bootstrappers and well connected/trusted nodes
// used for bootstrapping.
func WithPeerExchange(doPX bool) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("pubsub router is not gossipsub")
		}

		gs.doPX = doPX
		return nil
	}
}

type PXRecordReducer func(pxRecords []*pb.PeerInfo, logger *slog.Logger, p peer.ID, addrs []ma.Multiaddr, record *record.Envelope) []*pb.PeerInfo

// WithCustomPXRecordReducer enables custom filtering of PX Records. See
// the implementation of OnlyPublicAddrsOnPeerExchange for an example.
func WithCustomPXRecordReducer(f PXRecordReducer) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("pubsub router is not gossipsub")
		}
		gs.reducePXRecords = f
		return nil
	}
}

// defaultPXRecordReducer serializes the Signed Peer Records within the record envelop
func defaultPXRecordReducer(pxRecords []*pb.PeerInfo, logger *slog.Logger, p peer.ID, addrs []ma.Multiaddr, record *record.Envelope) []*pb.PeerInfo {
	var recordBytes []byte
	if record != nil {
		var err error
		recordBytes, err = record.Marshal()
		if err != nil {
			logger.Warn("error marshaling signed peer record for", "peer", p, "err", err)
			return pxRecords
		}
	}
	return append(pxRecords, &pb.PeerInfo{PeerID: []byte(p), SignedPeerRecord: recordBytes})
}

// OnlyPublicAddrsOnPeerExchange is a gossipsub router option that defines ensures that only Signed Peer Records with
// public addresses are shared over the gossipsub PeerExchange method
func OnlyPublicAddrsOnPeerExchange() Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("pubsub router is not gossipsub")
		}
		gs.reducePXRecords = publicAddrsPXRecordReducer
		return nil
	}
}

// publicAddrsPXRecordReducer checks if the given record.Envelop includes any public IP address and returns the pb.PeerInfo accordingly.
// If the Signed Peer Record includes at least a public IP, the record is shared,
// If not, we return an empty PeerRecord, at least notify the remote node that there is a peer in that topic.
func publicAddrsPXRecordReducer(pxRecords []*pb.PeerInfo, logger *slog.Logger, p peer.ID, addrs []ma.Multiaddr, record *record.Envelope) []*pb.PeerInfo {
	if !slices.ContainsFunc(addrs, manet.IsPublicAddr) {
		return defaultPXRecordReducer(pxRecords, logger, p, addrs, nil)
	}
	if !checkPubAddrsOnSignedPeerRecord(logger, p, record) {
		logger.Warn("no public address on signed peer record for", "peer", p)
		record = nil
	}
	return defaultPXRecordReducer(pxRecords, logger, p, addrs, record)
}

func checkPubAddrsOnSignedPeerRecord(logger *slog.Logger, p peer.ID, env *record.Envelope) bool {
	if env == nil {
		return false
	}
	rec, err := env.Record()
	if err != nil {
		logger.Warn("error getting peer record for from signed envelop", "peer", p, "err", err)
		return false
	}

	pRec, ok := rec.(*peer.PeerRecord)
	if !ok {
		logger.Warn("unable to convert peer record from envelop")
	}

	return slices.ContainsFunc(pRec.Addrs, manet.IsPublicAddr)
}

// WithDirectPeers is a gossipsub router option that specifies peers with direct
// peering agreements. These peers are connected outside of the mesh, with all (valid)
// message unconditionally forwarded to them. The router will maintain open connections
// to these peers. Note that the peering agreement should be reciprocal with direct peers
// symmetrically configured at both ends.
func WithDirectPeers(pis []peer.AddrInfo) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("pubsub router is not gossipsub")
		}

		direct := make(map[peer.ID]struct{})
		for _, pi := range pis {
			direct[pi.ID] = struct{}{}
			ps.host.Peerstore().AddAddrs(pi.ID, pi.Addrs, peerstore.PermanentAddrTTL)
		}

		gs.direct = direct

		if gs.tagTracer != nil {
			gs.tagTracer.isDirect = func(p peer.ID) bool {
				_, ok := gs.direct[p]
				return ok
			}
		}

		return nil
	}
}

// WithDirectConnectTicks is a gossipsub router option that sets the number of
// heartbeat ticks between attempting to reconnect direct peers that are not
// currently connected. A "tick" is based on the heartbeat interval, which is
// 1s by default. The default value for direct connect ticks is 300.
func WithDirectConnectTicks(t uint64) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("pubsub router is not gossipsub")
		}
		gs.params.DirectConnectTicks = t
		return nil
	}
}

// WithGossipSubParams is a gossip sub router option that allows a custom
// config to be set when instantiating the gossipsub router.
func WithGossipSubParams(cfg GossipSubParams) Option {
	return func(ps *PubSub) error {
		if err := cfg.validate(); err != nil {
			return err
		}
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("pubsub router is not gossipsub")
		}
		// Overwrite current config and associated variables in the router.
		gs.params = cfg
		gs.connect = make(chan connectInfo, cfg.MaxPendingConnections)
		gs.mcache = NewMessageCache(cfg.HistoryGossip, cfg.HistoryLength)

		return nil
	}
}

// GossipSubRouter is a router that implements the gossipsub protocol.
// For each topic we have joined, we maintain an overlay through which
// messages flow; this is the mesh map.
// For each topic we publish to without joining, we maintain a list of peers
// to use for injecting our messages in the overlay with stable routes; this
// is the fanout map. Fanout peer lists are expired if we don't publish any
// messages to their topic for GossipSubFanoutTTL.
type GossipSubRouter struct {
	p          *PubSub
	logger     *slog.Logger
	peers      map[peer.ID]protocol.ID // peer protocols
	extensions *extensionsState

	direct         map[peer.ID]struct{}                         // direct peers
	mesh           map[string]map[peer.ID]struct{}              // topic meshes
	fanout         map[string]map[peer.ID]struct{}              // topic fanout
	lastpub        map[string]int64                             // last publish time for fanout topics
	gossip         map[peer.ID][]*pb.ControlIHave               // pending gossip
	control        map[peer.ID]*pb.ControlMessage               // pending control messages
	peerhave       map[peer.ID]int                              // number of IHAVEs received from peer in the last heartbeat
	peerdontwant   map[peer.ID]int                              // number of IDONTWANTs received from peer in the last heartbeat
	unwanted       map[peer.ID]map[checksum]int                 // TTL of the message ids peers don't want
	phaseForward   *phaseForwarding                             // optional push-pull phase transition on the eager-push path
	groupPush      *groupPush                                   // optional group-aware push suppression over phase forwarding
	linkFilter     func(local, remote peer.ID, mid string) bool // optional per-(message, link) activation predicate
	linkEnforce    bool                                         // bilateral link filter: verify inbound traffic against the predicate
	linkViolations *atomic.Int64                                // optional violation counter, application-supplied
	iwantWindow    time.Duration                                // optional one-outstanding-IWANT-per-id window
	iwantAsked     map[string]time.Time                         // message ids with an IWANT in flight, by request time
	iwantPromised  map[string]map[peer.ID]time.Time             // commitment enforcement: open promises per id — every peer asked, and when
	parkK          int                                          // broken promises before a peer is parked (0 = enforcement off)
	parkTTL        time.Duration                                // how long a parked peer's announcements are ineligible
	parkPromise    time.Duration                                // proactive promise deadline; break + refund fire here
	parkBreaks     map[peer.ID]int                              // broken-promise counts
	parkedUntil    map[peer.ID]time.Time                        // parked peers by expiry
	parkBreaksCtr  *atomic.Int64                                // exposure counters, may be nil
	parksCtr       *atomic.Int64
	iwantHedge     time.Duration             // optional: permit a 2nd IWANT after this elapsed
	iwantCount     map[string]int            // outstanding IWANTs per id, for the hedge cap
	adaptHedge     *adaptiveHedge            // optional: two-controller adaptive hedge (delay + AIMD rate)
	tailK          int                       // optional tail hedge: outstanding IWANTs per id allowed once the node is in the tail (0 = off)
	tailOn         bool                      // set on the router goroutine by EnterTailHedge
	reqLedger      map[string]*RequestRecord // passive request observation (requestobs.go); nil when off
	reqUnasked     int                       // first arrivals of ids never asked for
	tailExtraCtr   *atomic.Int64             // hedged (extra) asks issued, may be nil
	// the tail hedge's bounds and schedule (tailbounds.go); zero bounds and no schedule = the hedge as first built
	tailBoundID, tailBoundGroup, tailBoundPeer                   int
	tailAsks                                                     map[string]int  // asks per id over its life, first and retries included
	tailExtraUsed                                                int             // extra asks issued in this group
	tailPeerExtra                                                map[peer.ID]int // extra asks per serving peer in this group
	tailSchedule                                                 bool
	tailH                                                        int
	tailMissing                                                  int
	tailRefusedID, tailRefusedGroup, tailRefusedPeer, tailTopUps *atomic.Int64
	nqg                                                          *nqgConfig                       // optional: (N,q,g) request kernel
	nqgAsked                                                     map[string]map[peer.ID]time.Time // per-mid asked peers and times, for (N,q,g)
	rttPeer                                                      map[peer.ID]*peerQuantile        // per-peer response-time quantile (warms across payloads)
	rttGlobal                                                    *peerQuantile                    // global prior for cold peers
	iwantBudgetMax                                               int                              // optional PR-625-style per-heartbeat IWANT budget per id
	iwantBudget                                                  map[string]int                   // remaining budget by message id
	rng                                                          *rand.Rand                       // optional seeded shuffle source (deterministic mode)
	withholdServe                                                bool                             // F1 injection: never/late IWANT service
	withholdDelay                                                time.Duration                    // 0 = silent, >0 = serve whole after delay
	withheldServes                                               *atomic.Int64                    // verified-exposure counter, may be nil
	withholdSelect                                               func(mid string) bool            // injection: withhold only the ids the predicate accepts (adversaries.go)
	withholdPrefix                                               int                              // injection: count-ordered timing, 0 = off
	withholdPrefixFast                                           bool
	withholdAsked                                                int
	hearsay                                                      bool           // injection: re-announce every id heard in IHAVE
	hearsayWidth                                                 int            // peers per hearsay announcement, 0 = every peer of the topic
	ihaveObs                                                     *IHaveRPCStats // announce-rate observation (ihaveobs.go); nil when off
	coalesceControl                                              bool           // fold control-only RPCs into the one already queued (controlcoalesce.go)
	coalesced                                                    *atomic.Int64
	peerhaveIHave                                                map[peer.ID]int // of peerhave, the RPCs that carried an IHAVE
	hearsaid                                                     map[string]struct{}
	hearsayCtr                                                   *atomic.Int64
	relaySilent                                                  bool                             // injection: forward nothing received from others
	relaySkipped                                                 *atomic.Int64                    // messages not forwarded under relay silence
	idwSpoof                                                     bool                             // injection: claim to hold every id heard in IHAVE
	idwSpoofed                                                   *atomic.Int64                    // spoofed id claims sent
	lossClass                                                    uint8                            // F2a injection: which class is shed at admission
	lossPct                                                      int                              // drop probability, percent
	lossSeed                                                     uint64                           // per-node seed from the harness
	lossDropped                                                  *atomic.Int64                    // verified-exposure counter, may be nil
	phaseHolders                                                 map[string]*phaseHolderEntry     // optional announce-fed holder tracking for phase forwarding
	rarestFirst                                                  bool                             // order IWANT candidates by known announcers, fewest first
	rarityDrain                                                  bool                             // order queued normal-lane RPCs by rarity of their data, least-diffused first
	rarityAnnounce                                               map[string]*phaseHolderEntry     // shared announcer tracking for rarest-first pull and rarity drain
	rarityHold                                                   map[string]*phaseHolderEntry     // delivered-evidence tracking (IDONTWANT-fed): distinct peers known to HOLD a mid
	sendingSolicited                                             bool                             // transient: the RPC being sent answers an IWANT and must not be rarity-ranked
	pullBudgetMax                                                int                              // cap on concurrently outstanding pulled ids; 0 = unbounded
	pullMemory                                                   bool                             // keep offers the discipline could not act on; re-drive on move-on/refund
	redriveCtr                                                   *atomic.Int64                    // pull-memory diagnostics: re-driven asks
	offerTable                                                   map[string]map[peer.ID]time.Time // offer table: unseen id -> announcers and when they offered
	offerTried                                                   map[string]map[peer.ID]time.Time // asks already spent per id (relap on exhaustion)
	offerTopic                                                   map[string]string                // id -> topic, for the request gate at retry time
	offerByPeer                                                  map[peer.ID]int                  // offers recorded per peer, for the per-peer cap
	offerRetryCtr                                                *atomic.Int64                    // offer-table diagnostics: timer-driven retries sent
	pullOutstanding                                              map[string]time.Time             // ids pulled and not yet delivered, expiring on the discipline window
	pullBacklog                                                  []pullCandidate                  // declined candidates (budget or discipline), re-driven as slots free
	requestGate                                                  RequestGate                      // optional application veto on requesting an announced id
	deferred                                                     map[string]*deferredEntry        // declined announcements, replayable when the gate opens
	deferredMax                                                  int                              // cap on deferred entries
	iasked                                                       map[peer.ID]int                  // number of messages we have asked from peer in the last heartbeat
	outbound                                                     map[peer.ID]bool                 // connection direction cache, marks peers with outbound connections
	backoff                                                      map[string]map[peer.ID]time.Time // prune backoff
	connect                                                      chan connectInfo                 // px connection requests
	cab                                                          peerstore.AddrBook

	protos  []protocol.ID
	feature GossipSubFeatureTest

	mcache       *MessageCache
	tracer       *pubsubTracer
	score        *peerScore
	gossipTracer *gossipTracer
	tagTracer    *tagTracer
	gate         *peerGater

	// config for gossipsub parameters
	params GossipSubParams

	// whether PX is enabled; this should be enabled in bootstrappers and other well connected/trusted
	// nodes.
	doPX            bool
	reducePXRecords PXRecordReducer

	// threshold for accepting PX from a peer; this should be positive and limited to scores
	// attainable by bootstrappers and trusted nodes
	acceptPXThreshold float64

	// threshold for peer score to emit/accept gossip
	// If the peer score is below this threshold, we won't emit or accept gossip from the peer.
	// When there is no score, this value is 0.
	gossipThreshold float64

	// flood publish score threshold; we only publish to peers with score >= to the threshold
	// when using flood publishing or the peer is a fanout or floodsub peer.
	publishThreshold float64

	// threshold for peer score before we graylist the peer and silently ignore its RPCs
	graylistThreshold float64

	// threshold for median peer score before triggering opportunistic grafting
	opportunisticGraftThreshold float64

	// whether to use flood publishing
	floodPublish bool

	// number of heartbeats since the beginning of time; this allows us to amortize some resource
	// clean up -- eg backoff clean up.
	heartbeatTicks uint64
}

var _ BatchPublisher = &GossipSubRouter{}

type connectInfo struct {
	p   peer.ID
	spr *record.Envelope
}

func (gs *GossipSubRouter) Protocols() []protocol.ID {
	return gs.protos
}

func (gs *GossipSubRouter) Attach(p *PubSub) {
	gs.p = p
	gs.logger = p.logger
	gs.tracer = p.tracer

	// start the scoring
	gs.score.Start(gs)

	// and the gossip tracing
	gs.gossipTracer.Start(gs)

	// and the tracer for connmgr tags
	gs.tagTracer.Start(gs, p.logger)

	// start using the same msg ID function as PubSub for caching messages.
	gs.mcache.SetMsgIdFn(p.idGen.ID)

	// start the heartbeat
	go gs.heartbeatTimer()

	// start the PX connectors
	for i := 0; i < gs.params.Connectors; i++ {
		go gs.connector()
	}

	// Manage our address book from events emitted by libp2p
	go gs.manageAddrBook()

	// connect to direct peers
	if len(gs.direct) > 0 {
		go func() {
			if gs.params.DirectConnectInitialDelay > 0 {
				time.Sleep(gs.params.DirectConnectInitialDelay)
			}
			for p := range gs.direct {
				gs.connect <- connectInfo{p: p}
			}
		}()
	}
}

func (gs *GossipSubRouter) manageAddrBook() {
	sub, err := gs.p.host.EventBus().Subscribe([]any{
		&event.EvtPeerIdentificationCompleted{},
		&event.EvtPeerConnectednessChanged{},
	})
	if err != nil {
		gs.logger.Error("failed to subscribe to peer identification events", "err", err)
		return
	}
	defer sub.Close()

	for {
		select {
		case <-gs.p.ctx.Done():
			cabCloser, ok := gs.cab.(io.Closer)
			if ok {
				errClose := cabCloser.Close()
				if errClose != nil {
					gs.logger.Warn("failed to close addr book", "err", errClose)
				}
			}
			return
		case ev := <-sub.Out():
			switch ev := ev.(type) {
			case event.EvtPeerIdentificationCompleted:
				if ev.SignedPeerRecord != nil {
					cab, ok := peerstore.GetCertifiedAddrBook(gs.cab)
					if ok {
						ttl := peerstore.RecentlyConnectedAddrTTL
						if gs.p.host.Network().Connectedness(ev.Peer) == network.Connected {
							ttl = peerstore.ConnectedAddrTTL
						}
						_, err := cab.ConsumePeerRecord(ev.SignedPeerRecord, ttl)
						if err != nil {
							gs.logger.Warn("failed to consume signed peer record", "err", err)
						}
					}
				}
			case event.EvtPeerConnectednessChanged:
				if ev.Connectedness != network.Connected {
					gs.cab.UpdateAddrs(ev.Peer, peerstore.ConnectedAddrTTL, peerstore.RecentlyConnectedAddrTTL)
				}
			}
		}
	}
}

func (gs *GossipSubRouter) OnNewIncomingStream(peer.ID, protocol.ID) {}

func (gs *GossipSubRouter) OnClosedIncomingStream(pid peer.ID, proto protocol.ID) {
	if gs.gate != nil {
		gs.gate.OnClosedIncomingStream(pid, proto)
	}
	if gs.feature(GossipSubFeatureExtensions, proto) {
		gs.extensions.OnClosedIncomingStream(pid, proto)
	}
}

func (gs *GossipSubRouter) OnNewOutboundStream(p peer.ID, proto protocol.ID, helloPacket *RPC) *RPC {
	gs.logger.Debug("PEERUP: Add new peer using protocol", "peer", p, "protocol", proto)
	gs.tracer.OnNewOutboundStream(p, proto)
	gs.peers[p] = proto

	// track the connection direction
	outbound := false
	conns := gs.p.host.Network().ConnsToPeer(p)
loop:
	for _, c := range conns {
		stat := c.Stat()

		if stat.Limited {
			continue
		}

		if stat.Direction == network.DirOutbound {
			// only count the connection if it has a pubsub stream
			for _, s := range c.GetStreams() {
				if s.Protocol() == proto {
					outbound = true
					break loop
				}
			}
		}
	}
	gs.outbound[p] = outbound
	if gs.feature(GossipSubFeatureExtensions, proto) {
		helloPacket = gs.extensions.OnNewOutboundStream(p, helloPacket)
	}
	return helloPacket
}

func (gs *GossipSubRouter) OnClosedOutboundStream(p peer.ID) {
	gs.logger.Debug("PEERDOWN: Remove disconnected peer", "peer", p)
	gs.tracer.OnClosedOutboundStream(p)
	if gs.feature(GossipSubFeatureExtensions, gs.peers[p]) {
		gs.extensions.OnClosedOutboundStream(p)
	}
	delete(gs.peers, p)
	for _, peers := range gs.mesh {
		delete(peers, p)
	}
	for _, peers := range gs.fanout {
		delete(peers, p)
	}
	delete(gs.gossip, p)
	delete(gs.control, p)
	delete(gs.outbound, p)
	delete(gs.unwanted, p)
}

func (gs *GossipSubRouter) EnoughPeers(topic string, suggested int) bool {
	// check all peers in the topic
	tmap, ok := gs.p.topics[topic]
	if !ok {
		return false
	}

	fsPeers, gsPeers := 0, 0
	// floodsub peers
	for p := range tmap {
		if !gs.feature(GossipSubFeatureMesh, gs.peers[p]) {
			fsPeers++
		}
	}

	// gossipsub peers
	gsPeers = len(gs.mesh[topic])

	if suggested == 0 {
		suggested = gs.params.Dlo
	}

	if fsPeers+gsPeers >= suggested || gsPeers >= gs.params.Dhi {
		return true
	}

	return false
}

func (gs *GossipSubRouter) AddDirectPeer(pi peer.AddrInfo) {
	if gs.direct == nil {
		gs.direct = make(map[peer.ID]struct{})
	}
	gs.direct[pi.ID] = struct{}{}
	gs.p.host.Peerstore().AddAddrs(pi.ID, pi.Addrs, peerstore.PermanentAddrTTL)
	gs.tagTracer.protectDirect(pi.ID)
}

func (gs *GossipSubRouter) RemoveDirectPeer(p peer.ID) {
	delete(gs.direct, p)
	gs.tagTracer.unprotectDirect(p)
}

func (gs *GossipSubRouter) AcceptFrom(p peer.ID) AcceptStatus {
	_, direct := gs.direct[p]
	if direct {
		return AcceptAll
	}

	if gs.score.Score(p) < gs.graylistThreshold {
		return AcceptNone
	}

	return gs.gate.AcceptFrom(p)
}

// Preprocess sends the IDONTWANT control messages to all the mesh
// peers. They need to be sent right before the validation because they
// should be seen by the peers as soon as possible.
func (gs *GossipSubRouter) Preprocess(from peer.ID, msgs []*Message) {
	tmids := make(map[string][]string)
	for _, msg := range msgs {
		mid := gs.p.idGen.ID(msg)
		gs.noteArrival(mid, from)
		gs.clearIWantBudgetFor(mid)
		// Bilateral link filter, push side: the bytes are already spent, so the message is
		// still used — but an UNSOLICITED full message over an inactive mesh link is
		// attributable, and counted. Solicited data is not a violation: the lazy channel
		// legitimately invites pulls of inactive-link ids from mesh peers, and a served
		// IWANT response arrives here exactly like a push would — the discipline's
		// asked-table is the discriminator (measured: without it, one mid-diffusion
		// heartbeat produced hundreds of false positives per cell on a compliant network).
		// Transient mesh asymmetry around GRAFT/PRUNE remains a rare false-positive source;
		// this stays a counter for attribution, never a punishment.
		if gs.linkEnforce {
			if _, inMesh := gs.mesh[msg.GetTopic()][from]; inMesh && !gs.linkFilter(gs.p.host.ID(), from, mid) {
				if _, asked := gs.iwantAsked[mid]; !asked {
					if gs.linkViolations != nil {
						gs.linkViolations.Add(1)
					}
				}
			}
		}
		if len(msg.GetData()) < gs.params.IDontWantMessageThreshold {
			continue
		}
		topic := msg.GetTopic()
		tmids[topic] = append(tmids[topic], mid)
	}
	for topic, mids := range tmids {
		if len(mids) == 0 {
			continue
		}
		// shuffle the messages got from the RPC envelope
		gs.shuffleStrings(mids)
		// send IDONTWANT to all the mesh peers
		for p := range gs.mesh[topic] {
			if p == from {
				// We don't send IDONTWANT to the peer that sent us the messages
				continue
			}
			if gs.iRequestPartial(topic) && gs.peerSupportsSendingPartial(p, topic) {
				// Don't send IDONTWANT to peers that are using partial messages
				// for this topic
				continue
			}

			// send to only peers that support IDONTWANT
			if gs.feature(GossipSubFeatureIdontwant, gs.peers[p]) {
				sendMids := mids
				if gs.linkFilter != nil {
					// Control follows the active links: a peer that will never be pushed
					// this message over this link has no duplicate to suppress.
					sendMids = make([]string, 0, len(mids))
					for _, mid := range mids {
						if gs.linkFilter(gs.p.host.ID(), p, mid) {
							sendMids = append(sendMids, mid)
						}
					}
					if len(sendMids) == 0 {
						continue
					}
				}
				idontwant := []*pb.ControlIDontWant{{MessageIDs: sendMids}}
				out := rpcWithControl(nil, nil, nil, nil, nil, idontwant)
				gs.sendRPC(p, out, true)
			}
		}
	}
}

func (gs *GossipSubRouter) HandleRPC(rpc *RPC) {
	err := gs.extensions.HandleRPC(rpc)
	if err != nil {
		gs.logger.Warn("error in handling RPC", "from", rpc.from, "err", err)
	}

	ctl := rpc.GetControl()
	if ctl == nil {
		return
	}

	iwant := gs.handleIHave(rpc.from, ctl)
	iWantResponses := gs.handleIWant(rpc.from, ctl)
	prune := gs.handleGraft(rpc.from, ctl)
	gs.handlePrune(rpc.from, ctl)
	gs.handleIDontWant(rpc.from, ctl)

	if len(iwant) == 0 && len(iWantResponses) == 0 && len(prune) == 0 {
		return
	}

	out := rpcWithControl(iWantResponses, nil, iwant, nil, prune, nil)
	// Solicited data outranks rarity: an IWANT response is a direct demand signal, and the
	// rarity drain must never park it behind fresher traffic — a widely-announced message is
	// widely *offered*, which says nothing about the asker, who by asking proved it lacks
	// it. Measured before this exemption: the drain under load served background chatter
	// ahead of requested segments, worsening tails and inflating the server's re-serve load.
	if len(iWantResponses) > 0 {
		gs.sendingSolicited = true
	}
	gs.sendRPC(rpc.from, out, false)
	gs.sendingSolicited = false
}

func (gs *GossipSubRouter) handleIHave(p peer.ID, ctl *pb.ControlMessage) []*pb.ControlIWant {
	// IDONTWANT-spoofing injection: claim to every mesh peer of the topic that this node
	// holds each id it merely heard announced, and request nothing. Against phase forwarding
	// the claims inflate the holder count that decays the push budget, suppressing eager
	// pushes to third parties.
	if gs.idwSpoof {
		gs.spoofIDontWant(ctl)
		return nil
	}
	// Hearsay injection: re-announce what was just announced, before deciding whether to ask.
	if gs.hearsay {
		gs.hearsayAnnounce(p, ctl)
	}
	// we ignore IHAVE gossip from any peer whose score is below the gossip threshold
	score := gs.score.Score(p)
	if score < gs.gossipThreshold {
		gs.logger.Debug("IHAVE: ignoring peer with score below threshold", "peer", p, "score", score)
		return nil
	}

	// Commitment enforcement: a parked peer's announcements are ineligible for pull
	// selection until its park expires.
	if gs.peerParked(p) {
		return nil
	}

	// IHAVE flood protection
	gs.peerhave[p]++
	// Announce-rate observation, beside the counter the limit reads: peerhave counts every
	// control-bearing RPC, so record separately the ones that actually carried an IHAVE.
	if gs.peerhaveIHave != nil && len(ctl.GetIhave()) > 0 {
		gs.peerhaveIHave[p]++
	}
	if gs.peerhave[p] > gs.params.MaxIHaveMessages {
		gs.logger.Debug("IHAVE: peer has advertised too many times within this heartbeat interval; ignoring", "peer", p, "count", gs.peerhave[p])
		return nil
	}
	if gs.iasked[p] >= gs.params.MaxIHaveLength {
		gs.logger.Debug("IHAVE: peer has already advertised too many messages; ignoring", "peer", p, "messageCount", gs.iasked[p])
		return nil
	}

	// iwant is the dedup set, and doubles as the id-to-topic map the request gate needs: an id
	// announced under several topics keeps the first, which is the one that admitted it.
	iwant := make(map[string]string)
	candidates := make([]string, 0, 32)
	for _, ihave := range ctl.GetIhave() {
		topic := ihave.GetTopicID()
		_, ok := gs.mesh[topic]
		if !ok {
			continue
		}

		if !gs.p.peerFilter(p, topic) {
			continue
		}

	checkIwantMsgsLoop:
		for msgIdx, mid := range ihave.GetMessageIDs() {
			// prevent remote peer from sending too many msg_ids on a single IHAVE message
			if msgIdx >= gs.params.MaxIHaveLength {
				gs.logger.Debug("IHAVE: peer has sent IHAVE on topic with too many messages; ignoring remaining msgs", "peer", p, "topic", topic, "messageCount", len(ihave.MessageIDs))
				break checkIwantMsgsLoop
			}

			if gs.p.seenMessage(mid) {
				continue
			}
			// No link-filter check here, deliberately: since heartbeat gossip serves mesh
			// peers their inactive-link ids (emitGossip), an IHAVE for an inactive-link id
			// from a mesh peer is compliant lazy behaviour, and the wire cannot distinguish
			// it from an immediate announce. Enforcement is confined to the data channel,
			// where a push over an inactive link is unambiguous.
			gs.noteAnnouncedHolder(mid, p)
			gs.noteRarityAnnouncer(mid, p)
			gs.recordOffer(mid, topic, p)
			if _, dup := iwant[mid]; dup {
				continue
			}
			iwant[mid] = topic
			candidates = append(candidates, mid)
		}
	}

	if len(candidates) == 0 {
		return nil
	}

	// Decide the per-peer allowance before consulting any request policy. The policies below
	// record state as a side effect of allowing an id, so selecting more ids than we will send
	// and truncating afterwards would leave phantom entries: an id marked as requested that no
	// IWANT ever carried, suppressed until its window expires. Shuffling the candidates here
	// rather than the selected list keeps the anti-gaming property -- a peer cannot steer which
	// ids we ask for by ordering its IHAVE -- while making selection and dispatch the same set.
	allowance := gs.params.MaxIHaveLength - gs.iasked[p]
	if allowance <= 0 {
		return nil
	}
	gs.shuffleStrings(candidates)
	if gs.rarestFirst {
		// Balance the pull plane: spend the allowance on the least-announced ids first —
		// rarest-first over the local announcer estimate, oldest scheduling idea in p2p
		// distribution. The shuffle above still randomizes order *within* a rarity class,
		// so a peer cannot steer the order by how it lists its IHAVE; it can only make its
		// ids look better-diffused (extra announces), never rarer than they are to us.
		sort.SliceStable(candidates, func(a, b int) bool {
			return gs.rarityCount(candidates[a]) < gs.rarityCount(candidates[b])
		})
	}

	// The pull budget caps concurrently outstanding ids across all peers, so which candidate
	// gets a slot becomes a real allocation — the scarcity rarest-first needs. Applied after
	// the sort so both the taken set and the backlog preserve rarity order. Over-budget
	// candidates go to the backlog and are re-driven the moment a delivery frees a slot:
	// waiting for an external re-offer was measured to cost the tail a full heartbeat.
	if gs.pullBudgetMax > 0 {
		remaining := gs.pullBudgetMax - gs.pullOutstandingLive()
		if remaining <= 0 {
			gs.backlogPulls(candidates, iwant, p, nil)
			return nil
		}
		if remaining < allowance {
			allowance = remaining
		}
	}

	iwantlst := gs.selectIWants(candidates, iwant, p, allowance)
	if len(iwantlst) == 0 {
		if gs.pullBudgetMax > 0 || gs.pullMemory {
			gs.backlogPulls(candidates, iwant, p, nil)
		}
		return nil
	}

	gs.logger.Debug("IHAVE: Asking for messages from peer", "asking", len(iwantlst), "candidates", len(candidates), "peer", p)
	gs.iasked[p] += len(iwantlst)
	if gs.pullOutstanding != nil {
		now := time.Now()
		taken := make(map[string]struct{}, len(iwantlst))
		for _, mid := range iwantlst {
			gs.pullOutstanding[mid] = now
			taken[mid] = struct{}{}
		}
		gs.backlogPulls(candidates, iwant, p, taken)
	}

	gs.gossipTracer.AddPromise(p, iwantlst)
	gs.noteIWantSent(p, iwantlst)
	if gs.requestGate != nil {
		// Only the dispatched set is reported, so an application budget is charged for requests
		// that were actually made rather than for ids a later policy refused.
		gs.requestGate.Committed(p, iwantlst)
	}

	return []*pb.ControlIWant{{MessageIDs: iwantlst}}
}

// selectIWants picks at most allowance ids from candidates, consulting the active request
// policy in order.
//
// Selection and dispatch are the same set by construction, and that is the point: each policy
// records state as a side effect of allowing an id, so a caller that selected freely and
// truncated afterwards would leave phantom entries -- ids marked as requested that no IWANT
// ever carried, suppressed until their window expired. Callers must therefore shuffle
// candidates *before* calling this, not the result after.
//
// The application request gate runs FIRST, ahead of every policy, and that ordering is
// load-bearing rather than stylistic. The policies below mutate their ledgers when they allow an
// id; a gate placed after any of them would burn the id on every decline, so an application that
// declines now and wants the same id later -- the block-authority case -- could never come back
// for it. Ahead of them, a decline touches nothing and is retryable. topicOf maps each candidate
// to the topic it was announced under; it may be nil when no gate is installed.
func (gs *GossipSubRouter) selectIWants(candidates []string, topicOf map[string]string, p peer.ID, allowance int) []string {
	if allowance <= 0 {
		return nil
	}
	out := make([]string, 0, min(len(candidates), allowance))
	for _, mid := range candidates {
		if len(out) >= allowance {
			break
		}
		if gs.requestGate != nil && !gs.requestGate.Allow(p, topicOf[mid], mid) {
			gs.noteDeclined(p, topicOf[mid], mid)
			continue
		}
		if gs.nqg != nil {
			// (N,q,g) kernel owns the request decision when enabled.
			if !gs.nqgAllowed(mid, p) {
				continue
			}
			out = append(out, mid)
			continue
		}
		if !gs.iwantAllowed(mid, p) {
			continue
		}
		if !gs.iwantBudgetAllowed(mid) {
			continue
		}
		out = append(out, mid)
	}
	return out
}

func (gs *GossipSubRouter) handleIWant(p peer.ID, ctl *pb.ControlMessage) []*pb.Message {
	// we don't respond to IWANT requests from any peer whose score is below the gossip threshold
	score := gs.score.Score(p)
	if score < gs.gossipThreshold {
		gs.logger.Debug("IWANT: ignoring peer with score below threshold", "peer", p, "score", score)
		return nil
	}

	ihave := make(map[string]*pb.Message)
	for _, iwant := range ctl.GetIwant() {
		for _, mid := range iwant.GetMessageIDs() {
			// Check if that peer has sent IDONTWANT before, if so don't send them the message
			if _, ok := gs.unwanted[p][computeChecksum(mid)]; ok {
				continue
			}

			msg, count, ok := gs.mcache.GetForPeer(mid, p)
			if !ok {
				continue
			}

			if !gs.p.peerFilter(p, msg.GetTopic()) {
				continue
			}

			if count > gs.params.GossipRetransmission {
				gs.logger.Debug("IWANT: Peer has asked for message too many times; ignoring request", "peer", p, "messageID", mid)
				continue
			}

			ihave[mid] = msg.Message
		}
	}

	if len(ihave) == 0 {
		return nil
	}

	gs.logger.Debug("IWANT: Sending messages to peer", "messageCount", len(ihave), "peer", p)

	msgs := make([]*pb.Message, 0, len(ihave))
	mids := make([]string, 0, len(ihave))
	for mid, msg := range ihave {
		msgs = append(msgs, msg)
		mids = append(mids, mid)
	}

	if gs.withholdServe {
		return gs.withholdIWantServe(p, mids, msgs)
	}

	return msgs
}

func (gs *GossipSubRouter) handleGraft(p peer.ID, ctl *pb.ControlMessage) []*pb.ControlPrune {
	var prune []string

	doPX := gs.doPX
	score := gs.score.Score(p)
	now := time.Now()

	for _, graft := range ctl.GetGraft() {
		topic := graft.GetTopicID()

		if !gs.p.peerFilter(p, topic) {
			continue
		}

		peers, ok := gs.mesh[topic]
		if !ok {
			// don't do PX when there is an unknown topic to avoid leaking our peers
			doPX = false
			// spam hardening: ignore GRAFTs for unknown topics
			continue
		}

		// check if it is already in the mesh; if so do nothing (we might have concurrent grafting)
		_, inMesh := peers[p]
		if inMesh {
			continue
		}

		// we don't GRAFT to/from direct peers; complain loudly if this happens
		_, direct := gs.direct[p]
		if direct {
			gs.logger.Warn("GRAFT: ignoring request from direct peer", "peer", p)
			// this is possibly a bug from non-reciprocal configuration; send a PRUNE
			prune = append(prune, topic)
			// but don't PX
			doPX = false
			continue
		}

		// make sure we are not backing off that peer
		expire, backoff := gs.backoff[topic][p]
		if backoff && now.Before(expire) {
			gs.logger.Debug("GRAFT: ignoring backed off peer", "peer", p)
			// add behavioural penalty
			gs.score.AddPenalty(p, 1)
			// no PX
			doPX = false
			// check the flood cutoff -- is the GRAFT coming too fast?
			floodCutoff := expire.Add(gs.params.GraftFloodThreshold - gs.params.PruneBackoff)
			if now.Before(floodCutoff) {
				// extra penalty
				gs.score.AddPenalty(p, 1)
			}
			// refresh the backoff
			gs.addBackoff(p, topic, false)
			prune = append(prune, topic)
			continue
		}

		// check the score
		if score < 0 {
			// we don't GRAFT peers with negative score
			gs.logger.Debug("GRAFT: ignoring peer with negative score", "peer", p, "score", score, "topic", topic)
			// we do send them PRUNE however, because it's a matter of protocol correctness
			prune = append(prune, topic)
			// but we won't PX to them
			doPX = false
			// add/refresh backoff so that we don't reGRAFT too early even if the score decays back up
			gs.addBackoff(p, topic, false)
			continue
		}

		// check the number of mesh peers; if it is at (or over) Dhi, we only accept grafts
		// from peers with outbound connections; this is a defensive check to restrict potential
		// mesh takeover attacks combined with love bombing
		if len(peers) >= gs.params.Dhi && !gs.outbound[p] {
			prune = append(prune, topic)
			gs.addBackoff(p, topic, false)
			continue
		}

		gs.logger.Debug("GRAFT: add mesh link from peer in topic", "peer", p, "topic", topic)
		gs.tracer.Graft(p, topic)
		peers[p] = struct{}{}
	}

	if len(prune) == 0 {
		return nil
	}

	cprune := make([]*pb.ControlPrune, 0, len(prune))
	for _, topic := range prune {
		cprune = append(cprune, gs.makePrune(p, topic, doPX, false))
	}

	return cprune
}

func (gs *GossipSubRouter) handlePrune(p peer.ID, ctl *pb.ControlMessage) {
	score := gs.score.Score(p)

	for _, prune := range ctl.GetPrune() {
		topic := prune.GetTopicID()
		peers, ok := gs.mesh[topic]
		if !ok {
			continue
		}

		gs.logger.Debug("PRUNE: Remove mesh link to peer in topic", "peer", p, "topic", topic)
		gs.tracer.Prune(p, topic)
		delete(peers, p)
		// is there a backoff specified by the peer? if so obey it.
		backoff := prune.GetBackoff()
		if backoff > 0 {
			gs.doAddBackoff(p, topic, time.Duration(backoff)*time.Second)
		} else {
			gs.addBackoff(p, topic, false)
		}

		px := prune.GetPeers()
		if len(px) > 0 {
			// we ignore PX from peers with insufficient score
			if score < gs.acceptPXThreshold {
				gs.logger.Debug("PRUNE: ignoring PX from peer with insufficient score", "peer", p, "score", score, "topic", topic)
				continue
			}

			gs.pxConnect(px)
		}
	}
}

func (gs *GossipSubRouter) handleIDontWant(p peer.ID, ctl *pb.ControlMessage) {
	idontwants := ctl.GetIdontwant()
	if len(idontwants) == 0 {
		return
	}

	// IDONTWANT flood protection
	if gs.peerdontwant[p] >= gs.params.MaxIDontWantMessages {
		gs.logger.Debug("IDONWANT: peer has advertised too many times within this heartbeat interval; ignoring", "peer", p, "count", gs.peerdontwant[p])
		return
	}
	gs.peerdontwant[p]++

	totalUnwantedIds := 0
	// Remember all the unwanted message ids
	var unwanted map[checksum]int
mainIDWLoop:
	for _, idontwant := range idontwants {
		for _, mid := range idontwant.GetMessageIDs() {
			// IDONTWANT flood protection
			if totalUnwantedIds >= gs.params.MaxIDontWantLength {
				gs.logger.Debug("IDONWANT: peer has advertised too many ids within this message; ignoring", "peer", p, "idCount", totalUnwantedIds)
				break mainIDWLoop
			}

			totalUnwantedIds++
			if unwanted == nil {
				unwanted = gs.unwanted[p]
				if unwanted == nil {
					unwanted = make(map[checksum]int)
					gs.unwanted[p] = unwanted
				}
			}
			unwanted[computeChecksum(mid)] = gs.params.IDontWantMessageTTL
			// Group-aware push suppression counts distinct group members a peer has
			// evidenced; IDONTWANT is that evidence, the same channel the per-message
			// decay uses.
			if gs.groupPush != nil {
				gs.groupPush.note(p, mid)
			}
			gs.noteRarityHolder(mid, p)
		}
	}
}

func (gs *GossipSubRouter) addBackoff(p peer.ID, topic string, isUnsubscribe bool) {
	backoff := gs.params.PruneBackoff
	if isUnsubscribe {
		backoff = gs.params.UnsubscribeBackoff
	}
	gs.doAddBackoff(p, topic, backoff)
}

func (gs *GossipSubRouter) doAddBackoff(p peer.ID, topic string, interval time.Duration) {
	backoff, ok := gs.backoff[topic]
	if !ok {
		backoff = make(map[peer.ID]time.Time)
		gs.backoff[topic] = backoff
	}
	expire := time.Now().Add(interval)
	if backoff[p].Before(expire) {
		backoff[p] = expire
	}
}

func (gs *GossipSubRouter) pxConnect(peers []*pb.PeerInfo) {
	if len(peers) > gs.params.PrunePeers {
		gs.shufflePeerInfo(peers)
		peers = peers[:gs.params.PrunePeers]
	}

	toconnect := make([]connectInfo, 0, len(peers))

	for _, pi := range peers {
		p := peer.ID(pi.PeerID)

		_, connected := gs.peers[p]
		if connected {
			continue
		}

		var spr *record.Envelope
		if pi.SignedPeerRecord != nil {
			// the peer sent us a signed record; ensure that it is valid
			envelope, r, err := record.ConsumeEnvelope(pi.SignedPeerRecord, peer.PeerRecordEnvelopeDomain)
			if err != nil {
				gs.logger.Warn("error unmarshalling peer record obtained through px", "err", err)
				continue
			}
			rec, ok := r.(*peer.PeerRecord)
			if !ok {
				gs.logger.Warn("bogus peer record obtained through px: envelope payload is not PeerRecord")
				continue
			}
			if rec.PeerID != p {
				gs.logger.Warn("bogus peer record obtained through px: peer ID doesn't match expected peer", "recordPeerID", rec.PeerID, "expectedPeer", p)
				continue
			}
			spr = envelope
		}

		toconnect = append(toconnect, connectInfo{p, spr})
	}

	if len(toconnect) == 0 {
		return
	}

	for _, ci := range toconnect {
		select {
		case gs.connect <- ci:
		default:
			gs.logger.Debug("ignoring peer connection attempt; too many pending connections")
		}
	}
}

func (gs *GossipSubRouter) connector() {
	for {
		select {
		case ci := <-gs.connect:
			if gs.p.host.Network().Connectedness(ci.p) == network.Connected {
				continue
			}

			gs.logger.Debug("connecting to peer", "peer", ci.p)
			cab, ok := peerstore.GetCertifiedAddrBook(gs.cab)
			if ok && ci.spr != nil {
				_, err := cab.ConsumePeerRecord(ci.spr, peerstore.TempAddrTTL)
				if err != nil {
					gs.logger.Debug("error processing peer record", "err", err)
				}
			}

			ctx, cancel := context.WithTimeout(gs.p.ctx, gs.params.ConnectionTimeout)
			err := gs.p.host.Connect(ctx, peer.AddrInfo{ID: ci.p, Addrs: gs.cab.Addrs(ci.p)})
			cancel()
			if err != nil {
				gs.logger.Debug("error connecting to peer", "peer", ci.p, "err", err)
			}

		case <-gs.p.ctx.Done():
			return
		}
	}
}

func (gs *GossipSubRouter) PublishBatch(messages []*Message, opts *BatchPublishOptions) {
	strategy := opts.Strategy
	for _, msg := range messages {
		msgID := gs.p.idGen.ID(msg)
		for p, rpc := range gs.rpcs(msg) {
			strategy.AddRPC(p, msgID, rpc)
		}
	}

	for p, rpc := range strategy.All() {
		// Re-check IDONTWANT here, not only at plan time above: a batch plans every
		// (peer, message) pair before transmitting any of them, so an IDONTWANT arising from the
		// batch's own first copies cannot prune the remainder.
		//
		// How much this buys depends entirely on the scheduler. RoundRobinMessageIDScheduler
		// drains immediately, so the plan-to-drain window is local computation and nothing can
		// arrive inside it -- measured as no change at all in publisher upload. A *pacing*
		// scheduler, which is the natural choice when the publisher's uplink is the constraint
		// and the reason batch publishing exists, spreads sends over time and opens a real
		// window. RPCScheduler is a pluggable interface, so both cases are reachable.
		//
		// A second window is not closed here: sendRPC enqueues onto a per-peer queue that a
		// different goroutine drains to the wire, and an IDONTWANT arriving during that period
		// should also prune. Checking there would mean reading router-loop state from the write
		// goroutine, which is a data race, so it needs a concurrency change rather than a move.
		if rpc = gs.withoutUnwanted(rpc, p); rpc == nil {
			continue
		}
		gs.sendRPC(p, rpc, false)
	}
}

// withoutUnwanted returns rpc with any message p has sent an IDONTWANT for removed, or nil when
// that leaves nothing to send.
//
// It never mutates the RPC. gs.rpcs yields a single shared *RPC per message to every recipient, so
// filtering in place would edit other peers' sends; the common case -- one message, still wanted --
// returns the original untouched and allocates nothing.
func (gs *GossipSubRouter) withoutUnwanted(rpc *RPC, p peer.ID) *RPC {
	unwanted := gs.unwanted[p]
	if len(unwanted) == 0 || len(rpc.Publish) == 0 {
		return rpc
	}
	drop := 0
	for _, msg := range rpc.Publish {
		if _, no := unwanted[computeChecksum(gs.p.idGen.RawID(msg))]; no {
			drop++
		}
	}
	if drop == 0 {
		return rpc
	}
	if drop == len(rpc.Publish) && len(rpc.Control.GetIhave()) == 0 &&
		len(rpc.Control.GetIwant()) == 0 && len(rpc.Control.GetGraft()) == 0 &&
		len(rpc.Control.GetPrune()) == 0 && len(rpc.Control.GetIdontwant()) == 0 {
		return nil
	}
	kept := make([]*pb.Message, 0, len(rpc.Publish)-drop)
	for _, msg := range rpc.Publish {
		if _, no := unwanted[computeChecksum(gs.p.idGen.RawID(msg))]; !no {
			kept = append(kept, msg)
		}
	}
	out := shallowRPCCopy(rpc)
	out.Publish = kept
	return out
}

// shallowRPCCopy returns a new RPC sharing rpc's field values, so a caller may replace one field
// without mutating an RPC other recipients still hold. Nothing beneath the fields is copied.
//
// The fields are assigned one by one rather than by copying the embedded pb.RPC value, because
// that copy takes protoimpl.MessageState with it: per-instance protobuf state carrying a mutex, a
// size cache computed for the *original* field set, and any unknown fields. The runtime marks it
// DoNotCopy and `go vet` reports it. Nothing has miscomputed a size yet -- proto.Size recomputes
// rather than trusting the cache -- so this is a latent hazard being removed, not a live bug.
//
// TestShallowRPCCopyCarriesEveryField pins the field list by reflection: a field added to pb.RPC
// upstream fails that test rather than being silently dropped from every filtered send.
func shallowRPCCopy(rpc *RPC) *RPC {
	out := &RPC{from: rpc.from}
	out.Subscriptions = rpc.Subscriptions
	out.Publish = rpc.Publish
	out.Control = rpc.Control
	out.Partial = rpc.Partial
	out.TestExtension = rpc.TestExtension
	return out
}

func (gs *GossipSubRouter) Publish(msg *Message) {
	for p, rpc := range gs.rpcs(msg) {
		gs.sendRPC(p, rpc, false)
	}
}

func (gs *GossipSubRouter) rpcs(msg *Message) iter.Seq2[peer.ID, *RPC] {
	return func(yield func(peer.ID, *RPC) bool) {
		gs.mcache.Put(msg)

		from := msg.ReceivedFrom
		topic := msg.GetTopic()

		// Relay-silence injection: the node originates its own publishes but forwards
		// nothing received from others. Announcing (heartbeat IHAVE) and IWANT service are
		// untouched here — stack WithIWantWithholding for the bait-and-blackhole adversary.
		if gs.relaySilent && from != gs.p.host.ID() {
			if gs.relaySkipped != nil {
				gs.relaySkipped.Add(1)
			}
			return
		}

		// (N,q,g): a message from a peer we asked is a claim->arrival sample for that peer.
		if gs.nqg != nil {
			gs.nqgObserve(gs.p.idGen.ID(msg), from)
		}

		tosend := make(map[peer.ID]struct{})
		// announce collects mesh peers that phase forwarding elected to IHAVE instead of push.
		var announce []peer.ID

		// any peers in the topic?
		tmap, ok := gs.p.topics[topic]
		if !ok {
			return
		}

		if gs.floodPublish && from == gs.p.host.ID() {
			for p := range tmap {
				_, direct := gs.direct[p]
				if direct || gs.score.Score(p) >= gs.publishThreshold {
					tosend[p] = struct{}{}
				}
			}
		} else {
			// direct peers
			for p := range gs.direct {
				_, inTopic := tmap[p]
				if inTopic {
					tosend[p] = struct{}{}
				}
			}

			// floodsub peers
			for p := range tmap {
				if !gs.feature(GossipSubFeatureMesh, gs.peers[p]) && gs.score.Score(p) >= gs.publishThreshold {
					tosend[p] = struct{}{}
				}
			}

			// gossipsub peers
			gmap, ok := gs.mesh[topic]
			if !ok {
				// we are not in the mesh for topic, use fanout peers
				gmap = gs.getFanoutPeersForPublishing(topic)
			}

			csum := computeChecksum(gs.p.idGen.ID(msg))
			fmid := gs.p.idGen.ID(msg)
			for p := range gmap {
				// Check if it has already received an IDONTWANT for the message.
				// If so, don't send it to the peer
				if _, ok := gs.unwanted[p][csum]; ok {
					continue
				}
				// Per-(message, link) activation: an inactive link carries neither the
				// push nor the phase announce for this message. Symmetric by contract,
				// so the peer expects nothing on it either.
				if gs.linkFilter != nil && !gs.linkFilter(gs.p.host.ID(), p, fmid) {
					continue
				}
				tosend[p] = struct{}{}
			}

			if gs.phaseForward != nil && gs.phaseForward.match(topic) {
				mid := gs.p.idGen.ID(msg)
				cand := gmap
				if gs.phaseForward.meshless {
					// Meshless: every connected gossipsub-capable topic peer is a candidate,
					// gated the way flood publish gates, and the mesh stops deciding who
					// gets eager copies.
					cand = make(map[peer.ID]struct{}, len(tmap))
					for p := range tmap {
						if !gs.feature(GossipSubFeatureMesh, gs.peers[p]) {
							continue
						}
						if gs.score.Score(p) < gs.publishThreshold {
							continue
						}
						if _, ok := gs.unwanted[p][csum]; ok {
							continue
						}
						if gs.announcedHolder(mid, p) {
							// A peer that announced the message holds it; neither push nor
							// announce is owed.
							continue
						}
						if gs.linkFilter != nil && !gs.linkFilter(gs.p.host.ID(), p, mid) {
							continue
						}
						cand[p] = struct{}{}
						tosend[p] = struct{}{}
					}
				}
				// Group-aware veto ahead of the budget: a peer evidenced to hold enough of
				// this message's group completes without it, so pushing buys nothing. It is
				// announced instead of dropped — evidence approximates, and a peer that is
				// not actually complete must still be able to pull.
				var groupVetoed []peer.ID
				if gs.groupPush != nil {
					for p := range tosend {
						if _, inMesh := cand[p]; !inMesh {
							continue
						}
						if gs.groupPush.complete(p, mid) {
							delete(tosend, p)
							groupVetoed = append(groupVetoed, p)
						}
					}
					if gs.groupPush.vetoes != nil && len(groupVetoed) > 0 {
						gs.groupPush.vetoes.Add(int64(len(groupVetoed)))
					}
				}
				fromSelf := from == gs.p.host.ID()
				announce = gs.phaseForward.trim(gs, mid, csum, cand, tosend, fromSelf)
				if fromSelf && gs.phaseForward.srcMode == PhaseSourceDelay {
					// The source's announce is deferred, not dropped: by the time it fires,
					// first-hop holders have announced too and absorb the pulls.
					gs.phaseForward.scheduleDeferredIHave(gs, topic, mid, announce)
					announce = nil
				}
				// Vetoed peers get the immediate IHAVE under every source policy: the veto
				// must never silently withhold.
				announce = append(announce, groupVetoed...)
			}
		}

		out := rpcWithMessages(msg.Message)
		for pid := range tosend {
			if pid == from || pid == peer.ID(msg.GetFrom()) {
				continue
			}
			if gs.iSupportSendingPartial(topic) && gs.peerRequestsPartial(pid, topic) {
				// The peer requested partial messages. We'll skip sending them full messages
				continue
			}

			if !yield(pid, out) {
				return
			}
		}

		// Phase forwarding: the mesh peers not pushed get an immediate IHAVE, so they can
		// pull with an ordinary IWANT instead of waiting for the heartbeat's gossip.
		if len(announce) > 0 {
			mid := gs.p.idGen.ID(msg)
			for _, pid := range announce {
				if pid == from || pid == peer.ID(msg.GetFrom()) {
					continue
				}
				if gs.iSupportSendingPartial(topic) && gs.peerRequestsPartial(pid, topic) {
					continue
				}
				ihave := rpcWithControl(nil,
					[]*pb.ControlIHave{{TopicID: &topic, MessageIDs: []string{mid}}},
					nil, nil, nil, nil)
				if !yield(pid, ihave) {
					return
				}
			}
		}
	}
}

func (gs *GossipSubRouter) peerSupportsSendingPartial(p peer.ID, topic string) bool {
	if !gs.extensions.myExtensions.PartialMessages {
		return false
	}
	if peerStates, ok := gs.p.topics[topic]; ok && peerStates[p].supportsPartial {
		return true
	}
	// A peer may declare partial interest in a topic it is not subscribed to; see
	// PubSub.announcePartialInterest.
	state, ok := gs.p.partialInterestState(topic, p)

	return ok && state.supportsPartial
}

func (gs *GossipSubRouter) peerRequestsPartial(p peer.ID, topic string) bool {
	if !gs.extensions.myExtensions.PartialMessages {
		return false
	}
	if peerStates, ok := gs.p.topics[topic]; ok && peerStates[p].requestsPartial {
		return true
	}
	state, ok := gs.p.partialInterestState(topic, p)

	return ok && state.requestsPartial
}

func (gs *GossipSubRouter) iSupportSendingPartial(topic string) bool {
	myTopicState := gs.p.myTopics[topic]
	return myTopicState != nil && myTopicState.supportsPartialMessages
}

func (gs *GossipSubRouter) iRequestPartial(topic string) bool {
	myTopicState := gs.p.myTopics[topic]
	return myTopicState != nil && myTopicState.requestPartialMessages
}

func (gs *GossipSubRouter) Join(topic string) {
	gmap, ok := gs.mesh[topic]
	if ok {
		return
	}

	gs.logger.Debug("JOIN topic", "topic", topic)
	gs.tracer.Join(topic)

	gmap, ok = gs.fanout[topic]
	if ok {
		backoff := gs.backoff[topic]
		// these peers have a score above the publish threshold, which may be negative
		// so drop the ones with a negative score
		for p := range gmap {
			_, doBackOff := backoff[p]
			if gs.score.Score(p) < 0 || doBackOff {
				delete(gmap, p)
			}
		}

		if len(gmap) < gs.params.D {
			// we need more peers; eager, as this would get fixed in the next heartbeat
			more := gs.getPeers(topic, gs.params.D-len(gmap), func(p peer.ID) bool {
				// filter our current peers, direct peers, peers we are backing off, and
				// peers with negative scores
				_, inMesh := gmap[p]
				_, direct := gs.direct[p]
				_, doBackOff := backoff[p]
				return !inMesh && !direct && !doBackOff && gs.score.Score(p) >= 0
			})
			for _, p := range more {
				gmap[p] = struct{}{}
			}
		}
		gs.mesh[topic] = gmap
		delete(gs.fanout, topic)
		delete(gs.lastpub, topic)
	} else {
		backoff := gs.backoff[topic]
		peers := gs.getPeers(topic, gs.params.D, func(p peer.ID) bool {
			// filter direct peers, peers we are backing off and peers with negative score
			_, direct := gs.direct[p]
			_, doBackOff := backoff[p]
			return !direct && !doBackOff && gs.score.Score(p) >= 0
		})
		gmap = peerListToMap(peers)
		gs.mesh[topic] = gmap
	}

	for p := range gmap {
		gs.logger.Debug("JOIN: Add mesh link to peer in topic", "peer", p, "topic", topic)
		gs.tracer.Graft(p, topic)
		gs.sendGraft(p, topic)
	}
}

func (gs *GossipSubRouter) Leave(topic string) {
	gmap, ok := gs.mesh[topic]
	if !ok {
		return
	}

	gs.logger.Debug("LEAVE topic", "topic", topic)
	gs.tracer.Leave(topic)

	delete(gs.mesh, topic)

	for p := range gmap {
		gs.logger.Debug("LEAVE: Remove mesh link to peer in topic", "peer", p, "topic", topic)
		gs.tracer.Prune(p, topic)
		gs.sendPrune(p, topic, true)
		// Add a backoff to this peer to prevent us from eagerly
		// re-grafting this peer into our mesh if we rejoin this
		// topic before the backoff period ends.
		gs.addBackoff(p, topic, true)
	}
}

func (gs *GossipSubRouter) sendGraft(p peer.ID, topic string) {
	graft := []*pb.ControlGraft{{TopicID: &topic}}
	out := rpcWithControl(nil, nil, nil, graft, nil, nil)
	gs.sendRPC(p, out, false)
}

func (gs *GossipSubRouter) sendPrune(p peer.ID, topic string, isUnsubscribe bool) {
	prune := []*pb.ControlPrune{gs.makePrune(p, topic, gs.doPX, isUnsubscribe)}
	out := rpcWithControl(nil, nil, nil, nil, prune, nil)
	gs.sendRPC(p, out, false)
}

// sendRPC reports whether `out` was admitted to the peer's send queue.
//
// The result exists for the partial-messages extension, whose application state cannot be
// corrected after the fact. Gossipsub drops a message here on queue pressure and relies on the
// receiver learning of it through the next heartbeat's IHAVE -- a design that works because IHAVE
// is *regenerated* rather than remembered. An application that records "I told this peer X" has no
// such regeneration: it will not say X again, because it believes it already has. Returning
// admission lets such an application record only what actually went out.
//
// Only the fate of `out` is reported. Piggybacked control and gossip are unaffected: they are
// re-queued on drop by doDropRPC, which is the recovery this return value exists to substitute for.
func (gs *GossipSubRouter) sendRPC(p peer.ID, out *RPC, urgent bool) bool {
	q, ok := gs.p.peers[p]
	if !ok {
		// No queue to send to this peer. Nothing to do.
		gs.doDropRPC(out, p, "No send queue for peer. Can't send RPC")
		return false
	}

	// Any pending control messages?
	var controlMessage RPC
	ctl, ok := gs.control[p]
	if ok {
		gs.piggybackControl(p, &controlMessage, ctl)
		delete(gs.control, p)
	}
	ihave, ok := gs.gossip[p]
	if ok {
		gs.piggybackGossip(p, &controlMessage, ihave)
		delete(gs.gossip, p)
	}

	controlSize := proto.Size(&controlMessage.RPC)
	dropIfOversized := func(rpc *RPC) bool {
		if !rpc.exceedsSizeLimits(gs.p.maxMessageSize, gs.p.maxControlMessageSize) {
			return false
		}
		size := proto.Size(&rpc.RPC)
		controlSize := controlRPCSize(rpc)
		gs.doDropRPC(rpc, p, fmt.Sprintf("Dropping oversized RPC. Size: %d, limit: %d. Control size: %d, control limit: %d", size, gs.p.maxMessageSize, controlSize, gs.p.maxControlMessageSize))
		return true
	}
	if controlSize > 0 {
		if !controlMessage.exceedsSizeLimits(gs.p.maxMessageSize, gs.p.maxControlMessageSize) {
			gs.doSendRPC(&controlMessage, p, q, urgent)
		} else {
			for rpc := range controlMessage.split(gs.p.maxMessageSize, gs.p.maxControlMessageSize) {
				if dropIfOversized(rpc) {
					continue
				}
				gs.doSendRPC(rpc, p, q, urgent)
			}
		}
	}

	// If we're below the max message size, go ahead and send
	if !out.exceedsSizeLimits(gs.p.maxMessageSize, gs.p.maxControlMessageSize) {
		return gs.doSendRPC(out, p, q, urgent)
	}

	// Potentially split the RPC into multiple RPCs that are below the max message size.
	// Admitted only if every fragment was: a partially delivered message is not delivered.
	admitted := true
	for rpc := range out.split(gs.p.maxMessageSize, gs.p.maxControlMessageSize) {
		if dropIfOversized(rpc) {
			admitted = false
			continue
		}
		if !gs.doSendRPC(rpc, p, q, urgent) {
			admitted = false
		}
	}

	return admitted
}

func (gs *GossipSubRouter) doDropRPC(rpc *RPC, p peer.ID, reason string) {
	gs.logger.Debug("dropping message to peer", "peer", p, "reason", reason)
	gs.tracer.DropRPC(rpc, p)
	if ctl := rpc.GetControl(); ctl != nil {
		for _, iw := range ctl.GetIwant() {
			gs.noteIWantDropped(p, iw.GetMessageIDs())
		}
	}
	// push control messages that need to be retried
	ctl := rpc.GetControl()
	if ctl != nil {
		gs.pushControl(p, ctl)
	}
}

// doSendRPC reports whether the RPC was admitted to the queue. Injected class loss counts as not
// admitted, which is the honest answer: the RPC will not arrive, and an application that latches
// "sent" on it would be wrong in exactly the way the failure injection is meant to expose.
func (gs *GossipSubRouter) doSendRPC(rpc *RPC, p peer.ID, q *rpcQueue, urgent bool) bool {
	if rpc = gs.applyClassLoss(rpc, p); rpc == nil {
		return false
	}
	// Sized once, here on the router goroutine, before the first queue sees it: the same RPC
	// goes to every peer of a publish, and the queues read the size under their own locks.
	if rpc.size == 0 {
		rpc.size = proto.Size(&rpc.RPC)
	}
	// Control coalescing: fold this RPC into the control-only RPC already waiting for the peer
	// rather than queue a second one. Only control, only the same lane, only when something is
	// already waiting — an idle link is untouched.
	if gs.coalesceControl && controlOnly(rpc) {
		if _, ok := q.coalesceInto(rpc, urgent, gs.p.maxMessageSize, gs.p.maxControlMessageSize); ok {
			if gs.coalesced != nil {
				gs.coalesced.Add(1)
			}
			gs.tracer.SendRPC(rpc, p)
			return true
		}
	}
	var err error
	switch {
	case urgent:
		err = q.UrgentPush(rpc, false)
	case gs.rarityDrain:
		rank := 0
		if !gs.sendingSolicited {
			rank = gs.drainRank(rpc)
		}
		err = q.RankedPush(rpc, rank, false)
	default:
		err = q.Push(rpc, false)
	}
	if err != nil {
		gs.doDropRPC(rpc, p, "queue full")
		return false
	}
	gs.tracer.SendRPC(rpc, p)

	return true
}

func (gs *GossipSubRouter) heartbeatTimer() {
	select {
	case <-time.After(gs.params.HeartbeatInitialDelay):
	case <-gs.p.ctx.Done():
		return
	}
	select {
	case gs.p.eval <- gs.heartbeat:
	case <-gs.p.ctx.Done():
		return
	}

	ticker := time.NewTicker(gs.params.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			select {
			case gs.p.eval <- gs.heartbeat:
			case <-gs.p.ctx.Done():
				return
			}
		case <-gs.p.ctx.Done():
			return
		}
	}
}

func (gs *GossipSubRouter) heartbeat() {
	start := time.Now()
	defer func() {
		if gs.params.SlowHeartbeatWarning > 0 {
			slowWarning := time.Duration(gs.params.SlowHeartbeatWarning * float64(gs.params.HeartbeatInterval))
			if dt := time.Since(start); dt > slowWarning {
				gs.logger.Warn("slow heartbeat", "took", dt)
			}
		}
	}()

	gs.heartbeatTicks++

	tograft := make(map[peer.ID][]string)
	toprune := make(map[peer.ID][]string)
	noPX := make(map[peer.ID]bool)

	// clean up expired backoffs
	gs.clearBackoff()

	// clean up iasked counters
	gs.clearIHaveCounters()

	// clean up IDONTWANT counters
	gs.clearIDontWantCounters()

	// expire outstanding-IWANT records and announce-fed holder entries
	gs.clearIWantAsked()
	gs.resetIWantBudget()
	gs.updateAdaptiveHedge()
	gs.clearNqgAsked()
	gs.clearPhaseHolders()

	// apply IWANT request penalties
	gs.applyIwantPenalties()

	// ensure direct peers are connected
	gs.directConnect()

	// cache scores throughout the heartbeat
	scores := make(map[peer.ID]float64)
	score := func(p peer.ID) float64 {
		s, ok := scores[p]
		if !ok {
			s = gs.score.Score(p)
			scores[p] = s
		}
		return s
	}

	// maintain the mesh for topics we have joined
	for topic, peers := range gs.mesh {
		prunePeer := func(p peer.ID) {
			gs.tracer.Prune(p, topic)
			delete(peers, p)
			gs.addBackoff(p, topic, false)
			topics := toprune[p]
			toprune[p] = append(topics, topic)
		}

		graftPeer := func(p peer.ID) {
			gs.logger.Debug("HEARTBEAT: Add mesh link to peer in topic", "peer", p, "topic", topic)
			gs.tracer.Graft(p, topic)
			peers[p] = struct{}{}
			topics := tograft[p]
			tograft[p] = append(topics, topic)
		}

		// drop all peers with negative score, without PX
		for p := range peers {
			if score(p) < 0 {
				gs.logger.Debug("HEARTBEAT: Prune peer with negative score", "peer", p, "score", score(p), "topic", topic)
				prunePeer(p)
				noPX[p] = true
			}
		}

		// do we have enough peers?
		if l := len(peers); l < gs.params.Dlo {
			backoff := gs.backoff[topic]
			ineed := gs.params.D - l
			plst := gs.getPeers(topic, ineed, func(p peer.ID) bool {
				// filter our current and direct peers, peers we are backing off, and peers with negative score
				_, inMesh := peers[p]
				_, doBackoff := backoff[p]
				_, direct := gs.direct[p]
				return !inMesh && !doBackoff && !direct && score(p) >= 0
			})

			for _, p := range plst {
				graftPeer(p)
			}
		}

		// do we have too many peers?
		if len(peers) >= gs.params.Dhi {
			plst := peerMapToList(peers)

			// sort by score (but shuffle first for the case we don't use the score)
			gs.shufflePeers(plst)
			sort.Slice(plst, func(i, j int) bool {
				return score(plst[i]) > score(plst[j])
			})

			// We keep the first D_score peers by score and the remaining up to D randomly
			// under the constraint that we keep D_out peers in the mesh (if we have that many)
			gs.shufflePeers(plst[gs.params.Dscore:])

			// count the outbound peers we are keeping
			outbound := 0
			for _, p := range plst[:gs.params.D] {
				if gs.outbound[p] {
					outbound++
				}
			}

			// if it's less than D_out, bubble up some outbound peers from the random selection
			if outbound < gs.params.Dout {
				rotate := func(i int) {
					// rotate the plst to the right and put the ith peer in the front
					p := plst[i]
					for j := i; j > 0; j-- {
						plst[j] = plst[j-1]
					}
					plst[0] = p
				}

				// first bubble up all outbound peers already in the selection to the front
				if outbound > 0 {
					ihave := outbound
					for i := 1; i < gs.params.D && ihave > 0; i++ {
						p := plst[i]
						if gs.outbound[p] {
							rotate(i)
							ihave--
						}
					}
				}

				// now bubble up enough outbound peers outside the selection to the front
				ineed := gs.params.Dout - outbound
				for i := gs.params.D; i < len(plst) && ineed > 0; i++ {
					p := plst[i]
					if gs.outbound[p] {
						rotate(i)
						ineed--
					}
				}
			}

			// prune the excess peers
			for _, p := range plst[gs.params.D:] {
				gs.logger.Debug("HEARTBEAT: Remove mesh link to peer in topic", "peer", p, "topic", topic)
				prunePeer(p)
			}
		}

		// do we have enough outboud peers?
		if len(peers) >= gs.params.Dlo {
			// count the outbound peers we have
			outbound := 0
			for p := range peers {
				if gs.outbound[p] {
					outbound++
				}
			}

			// if it's less than D_out, select some peers with outbound connections and graft them
			if outbound < gs.params.Dout {
				ineed := gs.params.Dout - outbound
				backoff := gs.backoff[topic]
				plst := gs.getPeers(topic, ineed, func(p peer.ID) bool {
					// filter our current and direct peers, peers we are backing off, and peers with negative score
					_, inMesh := peers[p]
					_, doBackoff := backoff[p]
					_, direct := gs.direct[p]
					return !inMesh && !doBackoff && !direct && gs.outbound[p] && score(p) >= 0
				})

				for _, p := range plst {
					graftPeer(p)
				}
			}
		}

		// should we try to improve the mesh with opportunistic grafting?
		if gs.heartbeatTicks%gs.params.OpportunisticGraftTicks == 0 && len(peers) > 1 {
			// Opportunistic grafting works as follows: we check the median score of peers in the
			// mesh; if this score is below the opportunisticGraftThreshold, we select a few peers at
			// random with score over the median.
			// The intention is to (slowly) improve an underperforming mesh by introducing good
			// scoring peers that may have been gossiping at us. This allows us to get out of sticky
			// situations where we are stuck with poor peers and also recover from churn of good peers.

			// now compute the median peer score in the mesh
			plst := peerMapToList(peers)
			sort.Slice(plst, func(i, j int) bool {
				return score(plst[i]) < score(plst[j])
			})
			medianIndex := len(peers) / 2
			medianScore := scores[plst[medianIndex]]

			// if the median score is below the threshold, select a better peer (if any) and GRAFT
			if medianScore < gs.opportunisticGraftThreshold {
				backoff := gs.backoff[topic]
				plst = gs.getPeers(topic, gs.params.OpportunisticGraftPeers, func(p peer.ID) bool {
					_, inMesh := peers[p]
					_, doBackoff := backoff[p]
					_, direct := gs.direct[p]
					return !inMesh && !doBackoff && !direct && score(p) > medianScore
				})

				for _, p := range plst {
					gs.logger.Debug("HEARTBEAT: Opportunistically graft peer on topic", "peer", p, "topic", topic)
					graftPeer(p)
				}
			}
		}

		// 2nd arg are mesh peers excluded from gossip. We already push
		// messages to them, so its redundant to gossip IHAVEs.
		gs.emitGossip(topic, peers)
	}

	// expire fanout for topics we haven't published to in a while
	now := time.Now().UnixNano()
	for topic, lastpub := range gs.lastpub {
		if lastpub+int64(gs.params.FanoutTTL) < now {
			delete(gs.fanout, topic)
			delete(gs.lastpub, topic)
		}
	}

	// maintain our fanout for topics we are publishing but we have not joined
	for topic, peers := range gs.fanout {
		// check whether our peers are still in the topic and have a score above the publish threshold
		for p := range peers {
			_, ok := gs.p.topics[topic][p]
			if !ok || score(p) < gs.publishThreshold {
				delete(peers, p)
			}
		}

		// do we need more peers?
		if len(peers) < gs.params.D {
			ineed := gs.params.D - len(peers)
			plst := gs.getPeers(topic, ineed, func(p peer.ID) bool {
				// filter our current and direct peers and peers with score above the publish threshold
				_, inFanout := peers[p]
				_, direct := gs.direct[p]
				return !inFanout && !direct && score(p) >= gs.publishThreshold
			})

			for _, p := range plst {
				peers[p] = struct{}{}
			}
		}

		// 2nd arg are fanout peers excluded from gossip. We already push
		// messages to them, so its redundant to gossip IHAVEs.
		gs.emitGossip(topic, peers)
	}

	// send coalesced GRAFT/PRUNE messages (will piggyback gossip)
	gs.sendGraftPrune(tograft, toprune, noPX)

	// flush all pending gossip that wasn't piggybacked above
	gs.flush()

	// advance the message history window
	gs.mcache.Shift()

	gs.extensions.Heartbeat()
}

func (gs *GossipSubRouter) clearIHaveCounters() {
	// Fold the heartbeat's counts into the observation before they are thrown away: one sample
	// per (peer, heartbeat) pair, which is exactly the quantity MaxIHaveMessages is compared to.
	if gs.ihaveObs != nil {
		for p, n := range gs.peerhave {
			gs.ihaveObs.observe(n, gs.peerhaveIHave[p])
		}
		if len(gs.peerhaveIHave) > 0 {
			gs.peerhaveIHave = make(map[peer.ID]int)
		}
	}
	if len(gs.peerhave) > 0 {
		// throw away the old map and make a new one
		gs.peerhave = make(map[peer.ID]int)
	}

	if len(gs.iasked) > 0 {
		// throw away the old map and make a new one
		gs.iasked = make(map[peer.ID]int)
	}
}

func (gs *GossipSubRouter) clearIDontWantCounters() {
	if len(gs.peerdontwant) > 0 {
		// throw away the old map and make a new one
		gs.peerdontwant = make(map[peer.ID]int)
	}

	// decrement TTLs of all the IDONTWANTs and delete it from the cache when it reaches zero
	for p, mids := range gs.unwanted {
		for mid := range mids {
			mids[mid]--
			if mids[mid] <= 0 {
				delete(mids, mid)
			}
		}
		if len(mids) == 0 {
			delete(gs.unwanted, p)
		}
	}
}

func (gs *GossipSubRouter) applyIwantPenalties() {
	for p, count := range gs.gossipTracer.GetBrokenPromises() {
		gs.logger.Info("peer didn't follow up in IWANT requests; adding penalty", "peer", p, "requestCount", count)
		gs.score.AddPenalty(p, count)
	}
}

func (gs *GossipSubRouter) clearBackoff() {
	// we only clear once every 15 ticks to avoid iterating over the map(s) too much
	if gs.heartbeatTicks%15 != 0 {
		return
	}

	now := time.Now()
	for topic, backoff := range gs.backoff {
		for p, expire := range backoff {
			// add some slack time to the expiration
			// https://github.com/libp2p/specs/pull/289
			if expire.Add(2 * GossipSubHeartbeatInterval).Before(now) {
				delete(backoff, p)
			}
		}
		if len(backoff) == 0 {
			delete(gs.backoff, topic)
		}
	}
}

func (gs *GossipSubRouter) directConnect() {
	// we donly do this every some ticks to allow pending connections to complete and account
	// for restarts/downtime
	if gs.heartbeatTicks%gs.params.DirectConnectTicks != 0 {
		return
	}

	var toconnect []peer.ID
	for p := range gs.direct {
		_, connected := gs.peers[p]
		if !connected {
			toconnect = append(toconnect, p)
		}
	}

	if len(toconnect) > 0 {
		go func() {
			for _, p := range toconnect {
				gs.connect <- connectInfo{p: p}
			}
		}()
	}
}

func (gs *GossipSubRouter) sendGraftPrune(tograft, toprune map[peer.ID][]string, noPX map[peer.ID]bool) {
	for p, topics := range tograft {
		graft := make([]*pb.ControlGraft, 0, len(topics))
		for _, topic := range topics {
			// copy topic string here since
			// the reference to the string
			// topic here changes with every
			// iteration of the slice.
			copiedID := topic
			graft = append(graft, &pb.ControlGraft{TopicID: &copiedID})
		}

		var prune []*pb.ControlPrune
		pruning, ok := toprune[p]
		if ok {
			delete(toprune, p)
			prune = make([]*pb.ControlPrune, 0, len(pruning))
			for _, topic := range pruning {
				prune = append(prune, gs.makePrune(p, topic, gs.doPX && !noPX[p], false))
			}
		}

		out := rpcWithControl(nil, nil, nil, graft, prune, nil)
		gs.sendRPC(p, out, false)
	}

	for p, topics := range toprune {
		prune := make([]*pb.ControlPrune, 0, len(topics))
		for _, topic := range topics {
			prune = append(prune, gs.makePrune(p, topic, gs.doPX && !noPX[p], false))
		}

		out := rpcWithControl(nil, nil, nil, nil, prune, nil)
		gs.sendRPC(p, out, false)
	}
}

// emitGossip emits IHAVE gossip advertising items in the message cache window
// of this topic.
func (gs *GossipSubRouter) emitGossip(topic string, exclude map[peer.ID]struct{}) {
	mids := gs.mcache.GetGossipIDs(topic)

	// Send gossip to GossipFactor peers above threshold, with a minimum of D_lazy.
	// First we collect the peers above gossipThreshold that are not in the exclude set
	// and then randomly select from that set.
	// We also exclude direct peers, as there is no reason to emit gossip to them.
	peers := make([]peer.ID, 0, len(gs.p.topics[topic]))
	for p := range gs.p.topics[topic] {
		_, inExclude := exclude[p]
		_, direct := gs.direct[p]

		// The exclude set is the mesh, and its justification is that mesh peers were already
		// served eagerly. Under a link filter that is only true per link: a mesh peer's
		// inactive links carried neither the push nor the immediate IHAVE, so those ids are
		// owed the lazy channel like any non-mesh peer's. Mesh members therefore stay
		// gossip-eligible when a filter is installed; the emission below restricts their id
		// lists to inactive-link ids they have not already evidenced holding.
		if inExclude && gs.linkFilter == nil {
			continue
		}
		if !direct && gs.feature(GossipSubFeatureMesh, gs.peers[p]) && gs.score.Score(p) >= gs.gossipThreshold {
			peers = append(peers, p)
		}
	}

	target := gs.params.Dlazy
	factor := int(gs.params.GossipFactor * float64(len(peers)))
	if factor > target {
		target = factor
	}

	if target > len(peers) {
		target = len(peers)
	} else {
		gs.shufflePeers(peers)
	}
	peers = peers[:target]

	nextPartial := 0
	iSupportPartial := gs.iSupportSendingPartial(topic)
	// split peers inplace into partial and non partial.
	for i, p := range peers {
		if iSupportPartial && gs.peerRequestsPartial(p, topic) {
			peers[i], peers[nextPartial] = peers[nextPartial], peers[i]
			nextPartial++
		}
	}
	partialMessagePeers := peers[:nextPartial]
	peers = peers[nextPartial:]

	// Emit the IHAVE gossip to the selected peers.
	if len(peers) > 0 && len(mids) > 0 {
		// shuffle to emit in random order
		gs.shuffleStrings(mids)

		// truncation is done after shuffling per peer below
		if len(mids) > gs.params.MaxIHaveLength {
			gs.logger.Debug("too many messages for gossip; will truncate IHAVE list", "messageCount", len(mids))
		}

		for _, p := range peers {
			peerMids := mids
			if _, inMesh := exclude[p]; inMesh && gs.linkFilter != nil {
				// A mesh peer's lazy list is the complement of what it was eagerly offered:
				// inactive-link ids only, minus what it has already IDONTWANTed to us.
				peerMids = make([]string, 0, len(mids))
				for _, mid := range mids {
					if gs.linkFilter(gs.p.host.ID(), p, mid) {
						continue
					}
					if _, held := gs.unwanted[p][computeChecksum(mid)]; held {
						continue
					}
					peerMids = append(peerMids, mid)
				}
				if len(peerMids) == 0 {
					continue
				}
				if len(peerMids) > gs.params.MaxIHaveLength {
					gs.shuffleStrings(peerMids)
					peerMids = peerMids[:gs.params.MaxIHaveLength]
				}
				gs.enqueueGossip(p, &pb.ControlIHave{TopicID: &topic, MessageIDs: peerMids})
				continue
			}
			if len(mids) > gs.params.MaxIHaveLength {
				// we do this per peer so that we emit a different set for each peer.
				// we have enough redundancy in the system that this will significantly increase the message
				// coverage when we do truncate.
				peerMids = make([]string, gs.params.MaxIHaveLength)
				gs.shuffleStrings(mids)
				copy(peerMids, mids)
			}
			gs.enqueueGossip(p, &pb.ControlIHave{TopicID: &topic, MessageIDs: peerMids})
		}
	}

	if len(partialMessagePeers) > 0 {
		gs.extensions.partialMessagesExtension.EmitGossip(topic, partialMessagePeers)
	}
}

func (gs *GossipSubRouter) flush() {
	// send gossip first, which will also piggyback pending control
	for p, ihave := range gs.gossip {
		delete(gs.gossip, p)
		out := rpcWithControl(nil, ihave, nil, nil, nil, nil)
		gs.sendRPC(p, out, false)
	}

	// send the remaining control messages that wasn't merged with gossip
	for p, ctl := range gs.control {
		delete(gs.control, p)
		out := rpcWithControl(nil, nil, nil, ctl.Graft, ctl.Prune, nil)
		gs.sendRPC(p, out, false)
	}
}

func (gs *GossipSubRouter) enqueueGossip(p peer.ID, ihave *pb.ControlIHave) {
	gossip := gs.gossip[p]
	gossip = append(gossip, ihave)
	gs.gossip[p] = gossip
}

func (gs *GossipSubRouter) piggybackGossip(p peer.ID, out *RPC, ihave []*pb.ControlIHave) {
	ctl := out.GetControl()
	if ctl == nil {
		ctl = &pb.ControlMessage{}
		out.Control = ctl
	}

	ctl.Ihave = ihave
}

func (gs *GossipSubRouter) pushControl(p peer.ID, ctl *pb.ControlMessage) {
	// remove IHAVE/IWANT/IDONTWANT from control message, gossip is not retried
	ctl.Ihave = nil
	ctl.Iwant = nil
	ctl.Idontwant = nil
	if ctl.Graft != nil || ctl.Prune != nil {
		gs.control[p] = ctl
	}
}

func (gs *GossipSubRouter) piggybackControl(p peer.ID, out *RPC, ctl *pb.ControlMessage) {
	// check control message for staleness first
	var tograft []*pb.ControlGraft
	var toprune []*pb.ControlPrune

	for _, graft := range ctl.GetGraft() {
		topic := graft.GetTopicID()
		peers, ok := gs.mesh[topic]
		if !ok {
			continue
		}
		_, ok = peers[p]
		if ok {
			tograft = append(tograft, graft)
		}
	}

	for _, prune := range ctl.GetPrune() {
		topic := prune.GetTopicID()
		peers, ok := gs.mesh[topic]
		if !ok {
			toprune = append(toprune, prune)
			continue
		}
		_, ok = peers[p]
		if !ok {
			toprune = append(toprune, prune)
		}
	}

	if len(tograft) == 0 && len(toprune) == 0 {
		return
	}

	xctl := out.Control
	if xctl == nil {
		xctl = &pb.ControlMessage{}
		out.Control = xctl
	}

	if len(tograft) > 0 {
		xctl.Graft = append(xctl.Graft, tograft...)
	}
	if len(toprune) > 0 {
		xctl.Prune = append(xctl.Prune, toprune...)
	}
}

func (gs *GossipSubRouter) makePrune(p peer.ID, topic string, doPX bool, isUnsubscribe bool) *pb.ControlPrune {
	if !gs.feature(GossipSubFeaturePX, gs.peers[p]) {
		// GossipSub v1.0 -- no peer exchange, the peer won't be able to parse it anyway
		return &pb.ControlPrune{TopicID: &topic}
	}

	backoff := uint64(gs.params.PruneBackoff / time.Second)
	if isUnsubscribe {
		backoff = uint64(gs.params.UnsubscribeBackoff / time.Second)
	}

	var px []*pb.PeerInfo
	if doPX {
		// select peers for Peer eXchange
		peers := gs.getPeers(topic, gs.params.PrunePeers, func(xp peer.ID) bool {
			return p != xp && gs.score.Score(xp) >= 0
		})

		cab, cabOK := peerstore.GetCertifiedAddrBook(gs.cab)
		px = make([]*pb.PeerInfo, 0, len(peers))
		for _, p := range peers {
			var record *record.Envelope
			if cabOK {
				record = cab.GetPeerRecord(p)
			}
			px = gs.reducePXRecords(px, gs.logger, p, gs.p.host.Peerstore().Addrs(p), record)
		}
	}

	return &pb.ControlPrune{TopicID: &topic, Peers: px, Backoff: &backoff}
}

// getFanoutPeersForPublishing returns fanout peers for a topic, initializing them if needed,
// and updates lastpub to keep the fanout alive.
func (gs *GossipSubRouter) getFanoutPeersForPublishing(topic string) map[peer.ID]struct{} {
	peers := gs.fanout[topic]
	if len(peers) == 0 {
		peerSlice := gs.getPeers(topic, gs.params.D, func(p peer.ID) bool {
			_, direct := gs.direct[p]
			return !direct && gs.score.Score(p) >= gs.publishThreshold
		})
		if len(peerSlice) > 0 {
			peers = peerListToMap(peerSlice)
			gs.fanout[topic] = peers
		}
	}
	gs.lastpub[topic] = time.Now().UnixNano()
	return peers
}

func (gs *GossipSubRouter) getPeers(topic string, count int, filter func(peer.ID) bool) []peer.ID {
	tmap, ok := gs.p.topics[topic]
	if !ok {
		return nil
	}

	peers := make([]peer.ID, 0, len(tmap))
	for p := range tmap {
		if gs.feature(GossipSubFeatureMesh, gs.peers[p]) && filter(p) && gs.p.peerFilter(p, topic) {
			peers = append(peers, p)
		}
	}

	gs.shufflePeers(peers)

	if count > 0 && len(peers) > count {
		peers = peers[:count]
	}

	return peers
}

// WithDefaultTagTracer returns the tag tracer of the GossipSubRouter as a PubSub option.
// This is useful for cases where the GossipSubRouter is instantiated externally, and is
// injected into the GossipSub constructor as a dependency. This allows the tag tracer to be
// also injected into the GossipSub constructor as a PubSub option dependency.
func (gs *GossipSubRouter) WithDefaultTagTracer() Option {
	return WithRawTracer(gs.tagTracer)
}

// SendControl dispatches the given set of control messages to the given peer.
// The control messages are sent as a single RPC, with the given (optional) messages.
// Args:
//
//	p: the peer to send the control messages to.
//	ctl: the control messages to send.
//	msgs: the messages to send in the same RPC (optional).
//	The control messages are piggybacked on the messages.
//
// Returns:
//
//	nothing.
func (gs *GossipSubRouter) SendControl(p peer.ID, ctl *pb.ControlMessage, msgs ...*pb.Message) {
	out := rpcWithControl(msgs, ctl.Ihave, ctl.Iwant, ctl.Graft, ctl.Prune, ctl.Idontwant)
	gs.sendRPC(p, out, false)
}

func peerListToMap(peers []peer.ID) map[peer.ID]struct{} {
	pmap := make(map[peer.ID]struct{})
	for _, p := range peers {
		pmap[p] = struct{}{}
	}
	return pmap
}

func peerMapToList(peers map[peer.ID]struct{}) []peer.ID {
	plst := make([]peer.ID, 0, len(peers))
	for p := range peers {
		plst = append(plst, p)
	}
	return plst
}

func shufflePeers(peers []peer.ID) {
	for i := range peers {
		j := rand.Intn(i + 1)
		peers[i], peers[j] = peers[j], peers[i]
	}
}

func shufflePeerInfo(peers []*pb.PeerInfo) {
	for i := range peers {
		j := rand.Intn(i + 1)
		peers[i], peers[j] = peers[j], peers[i]
	}
}

func shuffleStrings(lst []string) {
	for i := range lst {
		j := rand.Intn(i + 1)
		lst[i], lst[j] = lst[j], lst[i]
	}
}

func computeChecksum(mid string) checksum {
	var cs checksum
	if len(mid) > 32 || len(mid) == 0 {
		cs.payload = sha256.Sum256([]byte(mid))
	} else {
		cs.length = uint8(copy(cs.payload[:], mid))
	}
	return cs
}
