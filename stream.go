package dingbot

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// Version 是本库版本，随 UA 上报给钉钉网关。
const Version = "0.1.0"

const (
	defaultOpenAPIHost = "https://api.dingtalk.com"
	pathOpenConnection = "/v1.0/gateway/connections/open"

	// TopicBotMessage 机器人消息回调的 Stream topic。
	TopicBotMessage = "/v1.0/im/bot/messages/get"

	frameTypeSystem   = "SYSTEM"
	frameTypeCallback = "CALLBACK"
	frameTypeEvent    = "EVENT"
	systemTopicPing   = "ping"
	systemTopicDisc   = "disconnect"
)

// ErrServerDisconnect 表示网关下发了 SYSTEM/disconnect（通常是网关侧重平衡），本连接随之关闭。
// RunForever 遇到它会立即重连，不走退避。
var ErrServerDisconnect = errors.New("dingbot: 网关要求断开重连")

// Config 控制一个机器人应用的 Stream 长连接。
type Config struct {
	// ClientID / ClientSecret 企业内部应用的凭证（开发者后台「凭证与基础信息」，旧称 AppKey/AppSecret）。
	ClientID     string
	ClientSecret string

	// OpenAPIHost 默认 https://api.dingtalk.com。
	OpenAPIHost string
	// HTTPClient 用于「申请连接」那一次 HTTP 请求；默认带 10s 超时的独立 client。
	HTTPClient *http.Client
	// Dialer 用于建立 WebSocket；默认 websocket.DefaultDialer（尊重 HTTPS_PROXY 等环境变量）。
	Dialer *websocket.Dialer

	// KeepAlive 客户端发 WebSocket ping 的间隔，默认 30s。
	// 超过 KeepAlive+PongTimeout 没收到任何帧（含 pong）即判定连接僵死并重连——
	// 「连着但收不到」比「断了」更难发现（飞书、企微都踩过），故存活判据是【读到东西】而非「没报错」。
	KeepAlive   time.Duration
	PongTimeout time.Duration // 默认 10s

	// ReconnectMin / ReconnectMax 是 RunForever 的指数退避区间，默认 1s / 60s（带 ±20% 抖动）。
	// 成功连上过之后再掉线，退避从 ReconnectMin 重新起算。
	ReconnectMin time.Duration
	ReconnectMax time.Duration

	// Logger 注入调用方的结构化日志器；为 nil 时库完全静默。
	// 库是机制层，「打不打日志、打到哪」是调用方的策略。
	Logger *slog.Logger
}

// MessageHandler 处理一条机器人消息。
//
// 回调在独立 goroutine 中执行，panic 会被 recover 并记日志，绝不会带崩宿主进程。
// 库在【收到帧的当下】就回 ACK（不等回调结束）：回调里通常要做路由、调大模型等慢事，
// 迟迟不 ACK 只会招来网关重投，而重投的消息业务侧无法区分。
type MessageHandler func(ctx context.Context, msg *Message)

// ConnState 是连接状态。
type ConnState int

const (
	StateDisconnected ConnState = iota // 连接失败或掉线；回调 err 带原因
	StateConnected                     // WebSocket 已建立，可以收消息
)

func (s ConnState) String() string {
	switch s {
	case StateConnected:
		return "connected"
	case StateDisconnected:
		return "disconnected"
	}
	return fmt.Sprintf("ConnState(%d)", int(s))
}

// ConnStateHandler 连接状态回调。
//
// ★ 与「只报边沿」不同：每次连接【失败】或【掉线】都会回调一次 StateDisconnected（带本次原因），
// 每次连上回调一次 StateConnected。去重、节流是调用方的策略——调用方要按状态变化发声时，
// 需要拿到每一次失败的原因与次数才能汇总出「持续失败 N 次、已持续 X 分钟」。
// 回调同步执行，请勿阻塞。
type ConnStateHandler func(state ConnState, err error)

// Client 维护一条 Stream 长连接。一个 Client 只应被一个 RunForever 使用。
type Client struct {
	cfg     Config
	log     *slog.Logger
	onMsg   MessageHandler
	onState ConnStateHandler
}

// NewClient 创建 Stream 客户端，未填的配置取默认值。
func NewClient(cfg Config) *Client {
	if cfg.OpenAPIHost == "" {
		cfg.OpenAPIHost = defaultOpenAPIHost
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: 10 * time.Second}
	}
	if cfg.Dialer == nil {
		cfg.Dialer = websocket.DefaultDialer
	}
	if cfg.KeepAlive <= 0 {
		cfg.KeepAlive = 30 * time.Second
	}
	if cfg.PongTimeout <= 0 {
		cfg.PongTimeout = 10 * time.Second
	}
	if cfg.ReconnectMin <= 0 {
		cfg.ReconnectMin = time.Second
	}
	if cfg.ReconnectMax < cfg.ReconnectMin {
		cfg.ReconnectMax = 60 * time.Second
		if cfg.ReconnectMax < cfg.ReconnectMin {
			cfg.ReconnectMax = cfg.ReconnectMin
		}
	}
	log := cfg.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Client{cfg: cfg, log: log}
}

// OnMessage 注册机器人消息回调。须在 Run/RunForever 之前调用。
func (c *Client) OnMessage(h MessageHandler) { c.onMsg = h }

// OnConnectionState 注册连接状态回调。须在 Run/RunForever 之前调用。
func (c *Client) OnConnectionState(h ConnStateHandler) { c.onState = h }

func (c *Client) notify(s ConnState, err error) {
	if c.onState != nil {
		c.onState(s, err)
	}
}

// RunForever 保持长连接直到 ctx 取消，返回 ctx.Err()。
// 断线后按指数退避重连；ctx 取消后保证及时返回（不会卡在退避或读阻塞上）。
func (c *Client) RunForever(ctx context.Context) error {
	attempt := 0
	for {
		connected, err := c.run(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		c.notify(StateDisconnected, err)

		if connected {
			attempt = 0 // 健康连接的偶发掉线：从最小间隔重新退避
		}
		var delay time.Duration
		if errors.Is(err, ErrServerDisconnect) {
			delay = 0 // 网关主动要求重连，立刻照办
			attempt = 0
		} else {
			attempt++
			delay = backoff(c.cfg.ReconnectMin, c.cfg.ReconnectMax, attempt)
		}
		c.log.Debug("dingtalk stream 等待重连", "attempt", attempt, "delay", delay.String(), "err", err)
		if delay > 0 {
			t := time.NewTimer(delay)
			select {
			case <-t.C:
			case <-ctx.Done():
				t.Stop()
				return ctx.Err()
			}
		}
	}
}

// Run 建立一条连接并处理到它断开为止（不重连）。ctx 取消时返回 ctx.Err()。
func (c *Client) Run(ctx context.Context) error {
	_, err := c.run(ctx)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

// backoff 返回第 attempt 次（从 1 起）失败后的等待时间：min·2^(attempt-1)，封顶 max，±20% 抖动。
func backoff(min, max time.Duration, attempt int) time.Duration {
	d := min
	for i := 1; i < attempt && d < max; i++ {
		d *= 2
	}
	if d > max {
		d = max
	}
	jitter := 0.8 + 0.4*rand.Float64()
	return time.Duration(float64(d) * jitter)
}

// run 走完一条连接的完整生命周期；connected 报告本轮是否曾经连上。
//
// 并发结构刻意做到最简，以杜绝官方 SDK 那类「关 channel 与写 channel 竞争」的 panic：
//   - 读：只在本 goroutine 里读（ReadMessage 循环），不经任何 channel 转手；
//   - 写：数据帧经 writeMu 串行（gorilla 只允许一个并发写者），ping 用 WriteControl（官方声明可并发）；
//   - 停：ctx 取消、保活超时都只做一件事——关 conn，让阻塞中的 ReadMessage 出错返回。
func (c *Client) run(ctx context.Context) (connected bool, err error) {
	if c.cfg.ClientID == "" || c.cfg.ClientSecret == "" {
		return false, errors.New("dingbot: ClientID 与 ClientSecret 必填")
	}
	ep, err := c.openConnection(ctx)
	if err != nil {
		return false, err
	}
	wsURL := ep.Endpoint + "?ticket=" + url.QueryEscape(ep.Ticket)
	conn, resp, err := c.cfg.Dialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		if resp != nil {
			return false, fmt.Errorf("dingbot: 建立 WebSocket 失败（HTTP %d）: %w", resp.StatusCode, err)
		}
		return false, fmt.Errorf("dingbot: 建立 WebSocket 失败: %w", err)
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()

	c.log.Info("dingtalk stream 已连接")
	c.notify(StateConnected, nil)

	// 存活判据：KeepAlive+PongTimeout 内必须读到任何东西（数据帧或 pong），否则读超时出错。
	liveness := c.cfg.KeepAlive + c.cfg.PongTimeout
	_ = conn.SetReadDeadline(time.Now().Add(liveness))
	conn.SetPongHandler(func(string) error { return conn.SetReadDeadline(time.Now().Add(liveness)) })

	done := make(chan struct{})
	defer close(done)
	go func() {
		t := time.NewTicker(c.cfg.KeepAlive)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				// 写失败不必处理：连接若真坏了，读那一侧会在存活期限到时出错退出。
				_ = conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(c.cfg.PongTimeout))
			}
		}
	}()

	var writeMu sync.Mutex
	write := func(v any) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
		return conn.WriteJSON(v)
	}

	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return true, ctx.Err()
			}
			return true, fmt.Errorf("dingbot: 连接断开: %w", err)
		}
		_ = conn.SetReadDeadline(time.Now().Add(liveness))
		if err := c.handleFrame(ctx, data, write); err != nil {
			return true, err
		}
	}
}

// dataFrame 是网关下发的帧。
type dataFrame struct {
	SpecVersion string            `json:"specVersion"`
	Type        string            `json:"type"`
	Time        int64             `json:"time"`
	Headers     map[string]string `json:"headers"`
	Data        string            `json:"data"`
}

// frameAck 是对帧的应答，headers 里必须带回原帧的 messageId。
type frameAck struct {
	Code    int               `json:"code"`
	Headers map[string]string `json:"headers"`
	Message string            `json:"message"`
	Data    string            `json:"data"`
}

func ackFor(df *dataFrame, code int, msg, data string) frameAck {
	return frameAck{
		Code:    code,
		Headers: map[string]string{"messageId": df.Headers["messageId"], "contentType": "application/json"},
		Message: msg,
		Data:    data,
	}
}

// handleFrame 处理一帧；返回非 nil 表示本连接应当结束。
func (c *Client) handleFrame(ctx context.Context, data []byte, write func(any) error) error {
	var df dataFrame
	if err := json.Unmarshal(data, &df); err != nil {
		c.log.Warn("dingtalk stream 收到无法解析的帧，已忽略", "err", err, "len", len(data))
		return nil
	}
	topic := df.Headers["topic"]

	switch df.Type {
	case frameTypeSystem:
		switch topic {
		case systemTopicPing:
			// 网关的应用层 ping：原样回带 data
			return write(ackFor(&df, http.StatusOK, "ok", df.Data))
		case systemTopicDisc:
			c.log.Info("dingtalk stream 网关要求断开重连")
			return ErrServerDisconnect
		}
		c.log.Debug("dingtalk stream 未知系统帧", "topic", topic)
		return nil

	case frameTypeCallback, frameTypeEvent:
		if topic != TopicBotMessage {
			return write(ackFor(&df, http.StatusNotFound, "handler not found", ""))
		}
		// 先 ACK 再处理（理由见 MessageHandler 注释）。ACK 写失败说明连接坏了，结束本连接。
		if err := write(ackFor(&df, http.StatusOK, "ok", "")); err != nil {
			return fmt.Errorf("dingbot: 回 ACK 失败: %w", err)
		}
		msg, err := ParseMessage([]byte(df.Data))
		if err != nil {
			c.log.Warn("dingtalk stream 机器人消息解析失败", "err", err)
			return nil
		}
		if c.onMsg != nil {
			go c.dispatch(ctx, msg)
		}
		return nil
	}
	c.log.Debug("dingtalk stream 未知帧类型", "type", df.Type, "topic", topic)
	return nil
}

func (c *Client) dispatch(ctx context.Context, msg *Message) {
	defer func() {
		if r := recover(); r != nil {
			c.log.Error("dingtalk 消息回调 panic，已恢复", "panic", r, "msgId", msg.MsgID)
		}
	}()
	// 回调可能比连接活得久（连接断了消息仍要处理完），故不随连接取消；只继承 ctx 的值。
	c.onMsg(context.WithoutCancel(ctx), msg)
}

type openConnResp struct {
	Endpoint string `json:"endpoint"`
	Ticket   string `json:"ticket"`
}

// openConnection 申请一条 Stream 连接，拿到 WebSocket 地址与一次性 ticket。
func (c *Client) openConnection(ctx context.Context) (*openConnResp, error) {
	body, _ := json.Marshal(map[string]any{
		"clientId":      c.cfg.ClientID,
		"clientSecret":  c.cfg.ClientSecret,
		"ua":            "dingtalk-bot-go/" + Version,
		"subscriptions": []map[string]string{{"type": frameTypeCallback, "topic": TopicBotMessage}},
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.cfg.OpenAPIHost+pathOpenConnection, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	resp, err := c.cfg.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("dingbot: 申请 Stream 连接: %w", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if resp.StatusCode != http.StatusOK {
		return nil, newAPIError(resp, raw)
	}
	var ep openConnResp
	if err := json.Unmarshal(raw, &ep); err != nil || ep.Endpoint == "" || ep.Ticket == "" {
		return nil, fmt.Errorf("dingbot: 申请 Stream 连接返回异常: %s", truncate(raw, 200))
	}
	return &ep, nil
}
