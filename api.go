package dingbot

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// APIClient 调用钉钉 OpenAPI（主动发消息、附件下载）与 sessionWebhook（就地回复）。并发安全。
//
// 请求/响应字段以 alibabacloud-go/dingtalk 的生成代码为准（robot_1_0、oauth2_1_0）。
type APIClient struct {
	clientID     string
	clientSecret string
	robotCode    string
	host         string
	oapiHost     string // 旧版接口（媒体上传）的地址
	http         *http.Client
	log          *slog.Logger

	tokMu     sync.Mutex
	token     string
	tokenExp  time.Time
	tokenSkew time.Duration // 提前这么久视为过期，避免拿着将死的 token 发请求
}

// APIConfig 配置 APIClient。
type APIConfig struct {
	ClientID     string
	ClientSecret string
	// RobotCode 默认等于 ClientID（企业内部应用即如此）。
	RobotCode   string
	OpenAPIHost string       // 默认 https://api.dingtalk.com
	OAPIHost    string       // 旧版接口地址，默认 https://oapi.dingtalk.com（仅媒体上传用）
	HTTPClient  *http.Client // 默认 30s 超时
	Logger      *slog.Logger // nil 即静默
}

// NewAPIClient 创建 OpenAPI 客户端。
func NewAPIClient(cfg APIConfig) *APIClient {
	if cfg.RobotCode == "" {
		cfg.RobotCode = cfg.ClientID
	}
	if cfg.OpenAPIHost == "" {
		cfg.OpenAPIHost = defaultOpenAPIHost
	}
	if cfg.OAPIHost == "" {
		cfg.OAPIHost = "https://oapi.dingtalk.com"
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: 30 * time.Second}
	}
	log := cfg.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &APIClient{
		clientID: cfg.ClientID, clientSecret: cfg.ClientSecret, robotCode: cfg.RobotCode,
		host: cfg.OpenAPIHost, oapiHost: cfg.OAPIHost, http: cfg.HTTPClient, log: log, tokenSkew: 5 * time.Minute,
	}
}

// RobotCode 返回发送消息时使用的机器人编码。
func (a *APIClient) RobotCode() string { return a.robotCode }

// APIError 是 OpenAPI 返回的错误。
type APIError struct {
	HTTPStatus int
	Code       string // 如 InvalidAuthentication、Forbidden.AccessDenied.QpsLimit
	Message    string
	RequestID  string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("dingbot: OpenAPI 错误（HTTP %d，%s）: %s requestId=%s", e.HTTPStatus, e.Code, e.Message, e.RequestID)
}

// authInvalid 报告是否是 token 失效类错误——这类错误刷新 token 后重试一次即可自愈。
//
// 第一性原理：token 可能在有效期内被服务端作废（应用密钥被重置、并发多处取 token 等），
// 只信本地记的过期时间就会拿着死 token 一直失败、直到进程重启（飞书真机踩过）。
func (e *APIError) authInvalid() bool {
	return e.HTTPStatus == http.StatusUnauthorized || strings.Contains(e.Code, "InvalidAuthentication") ||
		strings.Contains(strings.ToLower(e.Code), "token")
}

func newAPIError(resp *http.Response, raw []byte) error {
	var body struct {
		Code      string `json:"code"`
		Message   string `json:"message"`
		RequestID string `json:"requestid"`
	}
	_ = json.Unmarshal(raw, &body)
	if body.Message == "" {
		body.Message = truncate(raw, 300)
	}
	return &APIError{HTTPStatus: resp.StatusCode, Code: body.Code, Message: body.Message, RequestID: body.RequestID}
}

func truncate(b []byte, n int) string {
	if len(b) > n {
		return string(b[:n]) + "…"
	}
	return string(b)
}

// accessToken 返回有效 token，必要时（或 force 时）重新获取。
func (a *APIClient) accessToken(ctx context.Context, force bool) (string, error) {
	a.tokMu.Lock()
	defer a.tokMu.Unlock()
	if !force && a.token != "" && time.Now().Before(a.tokenExp.Add(-a.tokenSkew)) {
		return a.token, nil
	}
	var out struct {
		AccessToken string `json:"accessToken"`
		ExpireIn    int64  `json:"expireIn"` // 秒
	}
	if err := a.doJSON(ctx, "/v1.0/oauth2/accessToken", "", map[string]string{
		"appKey": a.clientID, "appSecret": a.clientSecret,
	}, &out); err != nil {
		return "", fmt.Errorf("dingbot: 获取 accessToken: %w", err)
	}
	if out.AccessToken == "" {
		return "", errors.New("dingbot: 获取 accessToken 返回为空")
	}
	if out.ExpireIn <= 0 {
		out.ExpireIn = 7200
	}
	a.token, a.tokenExp = out.AccessToken, time.Now().Add(time.Duration(out.ExpireIn)*time.Second)
	return a.token, nil
}

// call 带 token 调一个 OpenAPI；遇 token 失效刷新后重试一次。
func (a *APIClient) call(ctx context.Context, path string, in, out any) error {
	for attempt := 0; ; attempt++ {
		tok, err := a.accessToken(ctx, attempt > 0)
		if err != nil {
			return err
		}
		err = a.doJSON(ctx, path, tok, in, out)
		var ae *APIError
		if attempt == 0 && errors.As(err, &ae) && ae.authInvalid() {
			a.log.Info("dingtalk accessToken 失效，刷新后重试", "path", path, "code", ae.Code)
			continue
		}
		return err
	}
}

func (a *APIClient) doJSON(ctx context.Context, path, token string, in, out any) error {
	body, err := json.Marshal(in)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.host+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("x-acs-dingtalk-access-token", token)
	}
	resp, err := a.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return newAPIError(resp, raw)
	}
	if out != nil {
		if err := json.Unmarshal(raw, out); err != nil {
			return fmt.Errorf("dingbot: 解析 %s 响应: %w", path, err)
		}
	}
	return nil
}

// OutMessage 是要发出去的一条消息。用 Text / Markdown / Image 构造。
type OutMessage struct {
	markdown bool
	title    string
	text     string
	mediaID  string // 非空即图片消息
}

// Image 构造图片消息；mediaID 来自 UploadImage。只能走主动发送接口（sessionWebhook 不支持图片）。
func Image(mediaID string) OutMessage { return OutMessage{mediaID: mediaID} }

// Text 构造纯文本消息。
func Text(s string) OutMessage { return OutMessage{text: s} }

// Markdown 构造 Markdown 消息（钉钉只支持 Markdown 子集）。title 是会话列表里透出的摘要。
func Markdown(title, text string) OutMessage {
	return OutMessage{markdown: true, title: title, text: text}
}

// msgKeyParam 转成主动发送接口的 msgKey + msgParam（msgParam 是 JSON 字符串）。
func (m OutMessage) msgKeyParam() (string, string) {
	if m.mediaID != "" {
		p, _ := json.Marshal(map[string]string{"photoURL": m.mediaID})
		return "sampleImageMsg", string(p)
	}
	if m.markdown {
		p, _ := json.Marshal(map[string]string{"title": m.title, "text": m.text})
		return "sampleMarkdown", string(p)
	}
	p, _ := json.Marshal(map[string]string{"content": m.text})
	return "sampleText", string(p)
}

// SendGroup 以机器人身份往群里发消息（不支持 @，平台限制）。返回 processQueryKey，可用于撤回/查已读。
func (a *APIClient) SendGroup(ctx context.Context, openConversationID string, m OutMessage) (string, error) {
	key, param := m.msgKeyParam()
	var out struct {
		ProcessQueryKey string `json:"processQueryKey"`
	}
	err := a.call(ctx, "/v1.0/robot/groupMessages/send", map[string]string{
		"msgKey": key, "msgParam": param, "openConversationId": openConversationID, "robotCode": a.robotCode,
	}, &out)
	return out.ProcessQueryKey, err
}

// SendToUsers 以机器人身份给若干人发单聊消息（userId 即 Message.SenderStaffID）。
//
// 接口对单个收件人的失败不报错，而是在响应里列出被拒的名单；只要有人没发到就返回 *PartialSendError。
func (a *APIClient) SendToUsers(ctx context.Context, userIDs []string, m OutMessage) (string, error) {
	key, param := m.msgKeyParam()
	var out struct {
		ProcessQueryKey           string   `json:"processQueryKey"`
		InvalidStaffIDList        []string `json:"invalidStaffIdList"`
		FlowControlledStaffIDList []string `json:"flowControlledStaffIdList"`
		FilteredStaffIDList       []string `json:"filteredStaffIdList"`
	}
	if err := a.call(ctx, "/v1.0/robot/oToMessages/batchSend", map[string]any{
		"msgKey": key, "msgParam": param, "userIds": userIDs, "robotCode": a.robotCode,
	}, &out); err != nil {
		return "", err
	}
	if len(out.InvalidStaffIDList)+len(out.FlowControlledStaffIDList)+len(out.FilteredStaffIDList) > 0 {
		return out.ProcessQueryKey, &PartialSendError{
			Invalid: out.InvalidStaffIDList, FlowControlled: out.FlowControlledStaffIDList, Filtered: out.FilteredStaffIDList,
		}
	}
	return out.ProcessQueryKey, nil
}

// PartialSendError 列出单聊批量发送中没发到的人。
type PartialSendError struct {
	Invalid        []string // userId 无效（不在本组织 / 不在应用可见范围）
	FlowControlled []string // 被限流
	Filtered       []string // 被过滤（如对方屏蔽了机器人）
}

func (e *PartialSendError) Error() string {
	return fmt.Sprintf("dingbot: 单聊部分未送达 invalid=%v flowControlled=%v filtered=%v", e.Invalid, e.FlowControlled, e.Filtered)
}

// DownloadURL 用回调里的 downloadCode 换临时下载地址（实测是 http:// 的 OSS 签名链接，约 1 小时有效）。
func (a *APIClient) DownloadURL(ctx context.Context, downloadCode string) (string, error) {
	var out struct {
		DownloadURL string `json:"downloadUrl"`
	}
	if err := a.call(ctx, "/v1.0/robot/messageFiles/download", map[string]string{
		"downloadCode": downloadCode, "robotCode": a.robotCode,
	}, &out); err != nil {
		return "", err
	}
	if out.DownloadURL == "" {
		return "", errors.New("dingbot: 下载地址为空")
	}
	return out.DownloadURL, nil
}

// Download 换取下载地址并以流的形式返回内容，调用方负责 Close。size 未知时为 -1。
func (a *APIClient) Download(ctx context.Context, downloadCode string) (body io.ReadCloser, size int64, err error) {
	u, err := a.DownloadURL(ctx, downloadCode)
	if err != nil {
		return nil, 0, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, 0, err
	}
	// 下载可能很大，不能套用 API 那个带总超时的 client：只复用其 Transport，超时交给 ctx。
	hc := &http.Client{Transport: a.http.Transport}
	resp, err := hc.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("dingbot: 下载附件: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		resp.Body.Close()
		return nil, 0, fmt.Errorf("dingbot: 下载附件 HTTP %d: %s", resp.StatusCode, truncate(raw, 200))
	}
	return resp.Body, resp.ContentLength, nil
}

// ErrWebhookExpired 表示这条消息的 sessionWebhook 已过期（有效期 90 分钟），应改用 SendGroup / SendToUsers。
var ErrWebhookExpired = errors.New("dingbot: sessionWebhook 已过期")

// ReplyWebhook 经这条消息的 sessionWebhook 就地回复。
//
// atSender 只在群聊里生效：平台要求 @ 同时出现在 at.atUserIds 与正文的 "@userId" 里才渲染成真 @；
// 而单聊里平台会把 "@userId" 原样显示成一串数字（真机实测），故单聊一律不加。
// 这也是主动发送接口做不到的事——它们都不支持 @。
func (a *APIClient) ReplyWebhook(ctx context.Context, msg *Message, m OutMessage, atSender bool) error {
	if msg.SessionWebhook == "" {
		return errors.New("dingbot: 消息未带 sessionWebhook")
	}
	if m.mediaID != "" {
		return errors.New("dingbot: sessionWebhook 不支持图片消息，请用 SendGroup / SendToUsers")
	}
	if exp := msg.WebhookExpiresAt(); !exp.IsZero() && time.Now().After(exp.Add(-10*time.Second)) {
		return ErrWebhookExpired
	}
	at := atSender && msg.IsGroup() && msg.SenderStaffID != ""
	text := m.text
	if at {
		text += " @" + msg.SenderStaffID
	}
	body := map[string]any{}
	if m.markdown {
		body["msgtype"] = "markdown"
		body["markdown"] = map[string]string{"title": m.title, "text": text}
	} else {
		body["msgtype"] = "text"
		body["text"] = map[string]string{"content": text}
	}
	if at {
		body["at"] = map[string]any{"atUserIds": []string{msg.SenderStaffID}}
	}
	raw, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, msg.SessionWebhook, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := a.http.Do(req)
	if err != nil {
		return fmt.Errorf("dingbot: webhook 回复: %w", err)
	}
	defer resp.Body.Close()
	rb, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	var r struct {
		ErrCode int    `json:"errcode"`
		ErrMsg  string `json:"errmsg"`
	}
	if resp.StatusCode != http.StatusOK || json.Unmarshal(rb, &r) != nil || r.ErrCode != 0 {
		return fmt.Errorf("dingbot: webhook 回复失败（HTTP %d）: %s", resp.StatusCode, truncate(rb, 200))
	}
	return nil
}

// Reply 回复一条消息：sessionWebhook 有效就走它（群聊可 @ 发送人），过期了就退回主动发送接口
// （群聊发群、单聊发给发送人；此时无法 @）。
func (a *APIClient) Reply(ctx context.Context, msg *Message, m OutMessage, atSender bool) error {
	if msg.SessionWebhook != "" && m.mediaID == "" {
		err := a.ReplyWebhook(ctx, msg, m, atSender)
		if !errors.Is(err, ErrWebhookExpired) {
			return err // 成功，或过期以外的失败（不换路重发，免得对方收到两条）
		}
	}
	var err error
	if msg.IsGroup() {
		_, err = a.SendGroup(ctx, msg.ConversationID, m)
		return err
	}
	if msg.SenderStaffID == "" {
		return errors.New("dingbot: 单聊消息缺 senderStaffId，无法主动回复")
	}
	_, err = a.SendToUsers(ctx, []string{msg.SenderStaffID}, m)
	return err
}

// UploadImage 上传一张图片（≤20MB，jpg/png/gif/bmp），返回发图片消息要用的 mediaId。
//
// 钉钉的新版 OpenAPI 没有机器人可用的媒体上传，只能走旧版 oapi 的 /media/upload；
// 新版 accessToken 在旧版接口上同样有效（以 query 参数传入）。
func (a *APIClient) UploadImage(ctx context.Context, name string, data []byte) (string, error) {
	for attempt := 0; ; attempt++ {
		tok, err := a.accessToken(ctx, attempt > 0)
		if err != nil {
			return "", err
		}
		var buf bytes.Buffer
		w := multipart.NewWriter(&buf)
		fw, err := w.CreateFormFile("media", name)
		if err != nil {
			return "", err
		}
		if _, err := fw.Write(data); err != nil {
			return "", err
		}
		if err := w.Close(); err != nil {
			return "", err
		}
		u := a.oapiHost + "/media/upload?type=image&access_token=" + url.QueryEscape(tok)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, &buf)
		if err != nil {
			return "", err
		}
		req.Header.Set("Content-Type", w.FormDataContentType())
		resp, err := a.http.Do(req)
		if err != nil {
			return "", fmt.Errorf("dingbot: 上传图片: %w", err)
		}
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		resp.Body.Close()
		var r struct {
			ErrCode int    `json:"errcode"`
			ErrMsg  string `json:"errmsg"`
			MediaID string `json:"media_id"`
		}
		_ = json.Unmarshal(raw, &r)
		// 旧版接口 token 失效是 errcode 40014 / 42001 / 88，刷新后重试一次。
		if attempt == 0 && (r.ErrCode == 40014 || r.ErrCode == 42001 || r.ErrCode == 88) {
			continue
		}
		if resp.StatusCode != http.StatusOK || r.ErrCode != 0 || r.MediaID == "" {
			return "", fmt.Errorf("dingbot: 上传图片失败（HTTP %d）: %s", resp.StatusCode, truncate(raw, 200))
		}
		return r.MediaID, nil
	}
}
