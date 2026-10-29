package dingbot

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// fakeGateway 模拟钉钉 Stream 网关：/v1.0/gateway/connections/open 发 ticket，/ws 交给 onConn 驱动。
type fakeGateway struct {
	t       *testing.T
	srv     *httptest.Server
	opens   atomic.Int32 // 申请连接次数
	conns   atomic.Int32 // WebSocket 建连次数
	openErr atomic.Bool  // 为 true 时申请连接返回 400
	onConn  func(conn *websocket.Conn, n int32)
}

func newFakeGateway(t *testing.T, onConn func(conn *websocket.Conn, n int32)) *fakeGateway {
	g := &fakeGateway{t: t, onConn: onConn}
	up := websocket.Upgrader{}
	mux := http.NewServeMux()
	mux.HandleFunc(pathOpenConnection, func(w http.ResponseWriter, r *http.Request) {
		g.opens.Add(1)
		var req map[string]any
		_ = json.NewDecoder(r.Body).Decode(&req)
		if g.openErr.Load() {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"code":"InvalidClientCredential","message":"bad secret","requestid":"rid"}`))
			return
		}
		if req["clientId"] != "cid" || req["clientSecret"] != "csec" {
			t.Errorf("申请连接的凭据不对: %v", req)
		}
		ws := "ws" + strings.TrimPrefix(g.srv.URL, "http") + "/ws"
		_ = json.NewEncoder(w).Encode(map[string]string{"endpoint": ws, "ticket": "tk"})
	})
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("ticket") != "tk" {
			t.Errorf("ticket 未带上: %s", r.URL.RawQuery)
		}
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		g.onConn(conn, g.conns.Add(1))
	})
	g.srv = httptest.NewServer(mux)
	t.Cleanup(g.srv.Close)
	return g
}

func (g *fakeGateway) client(mod func(*Config)) *Client {
	cfg := Config{ClientID: "cid", ClientSecret: "csec", OpenAPIHost: g.srv.URL,
		ReconnectMin: 10 * time.Millisecond, ReconnectMax: 50 * time.Millisecond}
	if mod != nil {
		mod(&cfg)
	}
	return NewClient(cfg)
}

func frame(typ, topic, msgID, data string) map[string]any {
	return map[string]any{"specVersion": "1.0", "type": typ, "time": 1,
		"headers": map[string]string{"topic": topic, "messageId": msgID}, "data": data}
}

const botData = `{"msgId":"m1","msgtype":"text","conversationType":"2","conversationId":"cidGROUP==","senderStaffId":"manager7675","isInAtList":true,"text":{"content":" 你好"}}`

// 回调帧：先回 ACK（带回 messageId），再异步交给回调。
func TestStreamCallbackAckAndDispatch(t *testing.T) {
	acks := make(chan frameAck, 1)
	g := newFakeGateway(t, func(conn *websocket.Conn, _ int32) {
		_ = conn.WriteJSON(frame(frameTypeCallback, TopicBotMessage, "mid-1", botData))
		var a frameAck
		if err := conn.ReadJSON(&a); err == nil {
			acks <- a
		}
		_, _, _ = conn.ReadMessage() // 挂住直到客户端关
	})
	c := g.client(nil)
	got := make(chan *Message, 1)
	c.OnMessage(func(_ context.Context, m *Message) { got <- m })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.RunForever(ctx)

	select {
	case a := <-acks:
		if a.Code != 200 || a.Headers["messageId"] != "mid-1" {
			t.Fatalf("ACK 不对: %+v", a)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("没收到 ACK")
	}
	select {
	case m := <-got:
		if m.PlainText() != "你好" || m.SenderStaffID != "manager7675" || !m.IsGroup() {
			t.Fatalf("消息解析不对: %+v", m)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("回调没被调用")
	}
}

// 网关的应用层 ping 要原样回带 data；未订阅的 topic 回 404。
func TestStreamSystemPingAndUnknownTopic(t *testing.T) {
	acks := make(chan frameAck, 2)
	g := newFakeGateway(t, func(conn *websocket.Conn, _ int32) {
		_ = conn.WriteJSON(frame(frameTypeSystem, systemTopicPing, "p1", `{"opaque":"x"}`))
		_ = conn.WriteJSON(frame(frameTypeCallback, "/v1.0/card/instances/callback", "c1", "{}"))
		for i := 0; i < 2; i++ {
			var a frameAck
			if conn.ReadJSON(&a) != nil {
				return
			}
			acks <- a
		}
		_, _, _ = conn.ReadMessage()
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go g.client(nil).RunForever(ctx)

	byID := map[string]frameAck{}
	for i := 0; i < 2; i++ {
		select {
		case a := <-acks:
			byID[a.Headers["messageId"]] = a
		case <-time.After(3 * time.Second):
			t.Fatal("ACK 不够")
		}
	}
	if a := byID["p1"]; a.Code != 200 || a.Data != `{"opaque":"x"}` {
		t.Fatalf("pong 不对: %+v", a)
	}
	if a := byID["c1"]; a.Code != 404 {
		t.Fatalf("未知 topic 应回 404: %+v", a)
	}
}

// 回调 panic 不得带崩进程，后续消息照常处理。
func TestStreamHandlerPanicRecovered(t *testing.T) {
	g := newFakeGateway(t, func(conn *websocket.Conn, _ int32) {
		_ = conn.WriteJSON(frame(frameTypeCallback, TopicBotMessage, "a", botData))
		_ = conn.WriteJSON(frame(frameTypeCallback, TopicBotMessage, "b", botData))
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	})
	c := g.client(nil)
	var n atomic.Int32
	done := make(chan struct{})
	c.OnMessage(func(context.Context, *Message) {
		if n.Add(1) == 1 {
			panic("boom")
		}
		close(done)
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.RunForever(ctx)
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("panic 之后的消息没被处理")
	}
}

// 网关关连接 → 回调 Disconnected（带原因）→ 退避后重连 → Connected。
func TestStreamReconnectAfterServerClose(t *testing.T) {
	g := newFakeGateway(t, func(conn *websocket.Conn, n int32) {
		if n == 1 {
			return // 第一条连接立刻关掉
		}
		_, _, _ = conn.ReadMessage()
	})
	c := g.client(nil)
	var mu sync.Mutex
	var states []ConnState
	var discErr error
	reconnected := make(chan struct{})
	c.OnConnectionState(func(s ConnState, err error) {
		mu.Lock()
		defer mu.Unlock()
		states = append(states, s)
		if s == StateDisconnected && discErr == nil {
			discErr = err
		}
		if len(states) == 3 {
			close(reconnected)
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.RunForever(ctx)
	select {
	case <-reconnected:
	case <-time.After(3 * time.Second):
		t.Fatalf("没重连上，states=%v", states)
	}
	mu.Lock()
	defer mu.Unlock()
	if states[0] != StateConnected || states[1] != StateDisconnected || states[2] != StateConnected {
		t.Fatalf("状态序列不对: %v", states)
	}
	if discErr == nil {
		t.Fatal("Disconnected 应带原因")
	}
}

// 网关下发 SYSTEM/disconnect：结束本连接并立即重连。
func TestStreamServerDisconnectFrame(t *testing.T) {
	g := newFakeGateway(t, func(conn *websocket.Conn, n int32) {
		if n == 1 {
			_ = conn.WriteJSON(frame(frameTypeSystem, systemTopicDisc, "d", ""))
		}
		_, _, _ = conn.ReadMessage()
	})
	c := g.client(func(cfg *Config) { cfg.ReconnectMin, cfg.ReconnectMax = time.Hour, time.Hour })
	errs := make(chan error, 4)
	c.OnConnectionState(func(s ConnState, err error) {
		if s == StateDisconnected {
			errs <- err
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.RunForever(ctx)
	select {
	case err := <-errs:
		if !errors.Is(err, ErrServerDisconnect) {
			t.Fatalf("应是 ErrServerDisconnect: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("没处理 disconnect 帧")
	}
	// 退避是 1 小时，能重连上说明走的是「立即重连」
	deadline := time.Now().Add(3 * time.Second)
	for g.conns.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatal("disconnect 帧之后没有立即重连")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// ctx 取消必须及时返回：无论阻塞在读上还是退避等待中。
func TestStreamCtxCancelReturnsPromptly(t *testing.T) {
	t.Run("阻塞在读", func(t *testing.T) {
		g := newFakeGateway(t, func(conn *websocket.Conn, _ int32) { _, _, _ = conn.ReadMessage() })
		c := g.client(nil)
		connected := make(chan struct{}, 1)
		c.OnConnectionState(func(s ConnState, _ error) {
			if s == StateConnected {
				connected <- struct{}{}
			}
		})
		ctx, cancel := context.WithCancel(context.Background())
		ret := make(chan error, 1)
		go func() { ret <- c.RunForever(ctx) }()
		<-connected
		cancel()
		select {
		case err := <-ret:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("应返回 context.Canceled: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("ctx 取消后没返回")
		}
	})
	t.Run("退避等待中", func(t *testing.T) {
		g := newFakeGateway(t, nil)
		g.openErr.Store(true)
		c := g.client(func(cfg *Config) { cfg.ReconnectMin, cfg.ReconnectMax = time.Hour, time.Hour })
		ctx, cancel := context.WithCancel(context.Background())
		ret := make(chan error, 1)
		go func() { ret <- c.RunForever(ctx) }()
		for g.opens.Load() < 1 {
			time.Sleep(5 * time.Millisecond)
		}
		cancel()
		select {
		case <-ret:
		case <-time.After(2 * time.Second):
			t.Fatal("退避中 ctx 取消后没返回")
		}
	})
}

// 「连着但收不到」：对端不回 pong（不读就不会回），客户端须在 KeepAlive+PongTimeout 后判死并重连。
func TestStreamLivenessTimeout(t *testing.T) {
	g := newFakeGateway(t, func(conn *websocket.Conn, n int32) {
		if n == 1 {
			time.Sleep(2 * time.Second) // 不读 → 不回 pong
			return
		}
		_, _, _ = conn.ReadMessage()
	})
	c := g.client(func(cfg *Config) { cfg.KeepAlive, cfg.PongTimeout = 50*time.Millisecond, 50*time.Millisecond })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.RunForever(ctx)
	deadline := time.Now().Add(1500 * time.Millisecond)
	for g.conns.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatal("僵死连接没被识破")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// 申请连接失败：每次失败都回调 Disconnected 带原因（含平台错误码），且按退避重试。
func TestStreamOpenConnectionFailureReported(t *testing.T) {
	g := newFakeGateway(t, nil)
	g.openErr.Store(true)
	c := g.client(nil)
	errs := make(chan error, 8)
	c.OnConnectionState(func(s ConnState, err error) {
		if s == StateDisconnected {
			errs <- err
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.RunForever(ctx)
	for i := 0; i < 2; i++ {
		select {
		case err := <-errs:
			var ae *APIError
			if !errors.As(err, &ae) || ae.Code != "InvalidClientCredential" {
				t.Fatalf("应带平台错误码: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("失败没有回调")
		}
	}
}

func TestBackoff(t *testing.T) {
	min, max := 100*time.Millisecond, time.Second
	for attempt, want := range map[int]time.Duration{1: 100 * time.Millisecond, 2: 200 * time.Millisecond, 4: 800 * time.Millisecond, 10: time.Second} {
		got := backoff(min, max, attempt)
		if got < want*8/10 || got > want*12/10 {
			t.Errorf("attempt=%d got=%v want≈%v", attempt, got, want)
		}
	}
}
