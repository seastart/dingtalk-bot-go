package dingbot

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// fakeOpenAPI 模拟钉钉 OpenAPI：发 token、收发送请求、给下载地址、提供文件与 sessionWebhook。
type fakeOpenAPI struct {
	srv        *httptest.Server
	tokens     atomic.Int32 // 发出的 token 数
	revokeOnce atomic.Bool  // 为 true 时下一次业务请求回 401（模拟 token 被服务端提前作废）
	last       map[string]any
	lastPath   string
	webhook    map[string]any
}

func newFakeOpenAPI(t *testing.T) *fakeOpenAPI {
	f := &fakeOpenAPI{}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1.0/oauth2/accessToken", func(w http.ResponseWriter, r *http.Request) {
		n := f.tokens.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{"accessToken": "tok" + string(rune('0'+n)), "expireIn": 7200})
	})
	biz := func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("x-acs-dingtalk-access-token") == "" {
			t.Errorf("%s 未带 token", r.URL.Path)
		}
		if f.revokeOnce.CompareAndSwap(true, false) {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"code":"InvalidAuthentication","message":"不合法的access_token","requestid":"r1"}`))
			return
		}
		f.lastPath = r.URL.Path
		f.last = map[string]any{}
		_ = json.NewDecoder(r.Body).Decode(&f.last)
		switch r.URL.Path {
		case "/v1.0/robot/oToMessages/batchSend":
			resp := map[string]any{"processQueryKey": "pqk"}
			if ids, _ := f.last["userIds"].([]any); len(ids) > 1 {
				resp["invalidStaffIdList"] = []string{ids[1].(string)}
			}
			_ = json.NewEncoder(w).Encode(resp)
		case "/v1.0/robot/messageFiles/download":
			_ = json.NewEncoder(w).Encode(map[string]string{"downloadUrl": f.srv.URL + "/file/blob"})
		default:
			_ = json.NewEncoder(w).Encode(map[string]string{"processQueryKey": "pqk"})
		}
	}
	mux.HandleFunc("/v1.0/robot/", biz)
	mux.HandleFunc("/file/blob", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("PNGDATA")) })
	mux.HandleFunc("/webhook", func(w http.ResponseWriter, r *http.Request) {
		f.webhook = map[string]any{}
		_ = json.NewDecoder(r.Body).Decode(&f.webhook)
		_, _ = w.Write([]byte(`{"errcode":0,"errmsg":"ok"}`))
	})
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeOpenAPI) client() *APIClient {
	return NewAPIClient(APIConfig{ClientID: "dingFAKErobot", ClientSecret: "s", OpenAPIHost: f.srv.URL})
}

func TestSendGroupAndTokenCache(t *testing.T) {
	f := newFakeOpenAPI(t)
	a := f.client()
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if _, err := a.SendGroup(ctx, "cidG==", Markdown("标题", "**正文**")); err != nil {
			t.Fatal(err)
		}
	}
	if f.tokens.Load() != 1 {
		t.Fatalf("token 应缓存复用，实际取了 %d 次", f.tokens.Load())
	}
	if f.last["msgKey"] != "sampleMarkdown" || f.last["robotCode"] != "dingFAKErobot" || f.last["openConversationId"] != "cidG==" {
		t.Fatalf("请求体: %v", f.last)
	}
	var p map[string]string
	_ = json.Unmarshal([]byte(f.last["msgParam"].(string)), &p)
	if p["title"] != "标题" || p["text"] != "**正文**" {
		t.Fatalf("msgParam 应是 JSON 字符串: %v", f.last["msgParam"])
	}
}

// token 在有效期内被服务端作废：刷新后重试一次即自愈。
func TestTokenRevokedRefreshesOnce(t *testing.T) {
	f := newFakeOpenAPI(t)
	a := f.client()
	ctx := context.Background()
	if _, err := a.SendGroup(ctx, "cidG==", Text("a")); err != nil {
		t.Fatal(err)
	}
	f.revokeOnce.Store(true)
	if _, err := a.SendGroup(ctx, "cidG==", Text("b")); err != nil {
		t.Fatalf("应刷新 token 后成功: %v", err)
	}
	if f.tokens.Load() != 2 {
		t.Fatalf("应重新取一次 token，实际 %d", f.tokens.Load())
	}
}

func TestSendToUsersPartial(t *testing.T) {
	f := newFakeOpenAPI(t)
	a := f.client()
	_, err := a.SendToUsers(context.Background(), []string{"manager7675", "ghost"}, Text("hi"))
	var pe *PartialSendError
	if !errors.As(err, &pe) || len(pe.Invalid) != 1 || pe.Invalid[0] != "ghost" {
		t.Fatalf("应报部分未送达: %v", err)
	}
	if f.last["msgKey"] != "sampleText" {
		t.Fatalf("请求体: %v", f.last)
	}
}

func TestDownload(t *testing.T) {
	f := newFakeOpenAPI(t)
	body, _, err := f.client().Download(context.Background(), "dcPic")
	if err != nil {
		t.Fatal(err)
	}
	defer body.Close()
	b, _ := io.ReadAll(body)
	if string(b) != "PNGDATA" || f.last["downloadCode"] != "dcPic" {
		t.Fatalf("下载内容 %q 请求 %v", b, f.last)
	}
}

func webhookMsg(f *fakeOpenAPI, convType string, expiresIn time.Duration) *Message {
	return &Message{ConversationType: convType, ConversationID: "cidX==", SenderStaffID: "manager7675",
		SessionWebhook: f.srv.URL + "/webhook", SessionWebhookExpiredTime: time.Now().Add(expiresIn).UnixMilli()}
}

// @ 只在群聊生效，且要同时出现在 at.atUserIds 与正文里；单聊加了会显示成一串数字。
func TestReplyWebhookAtOnlyInGroup(t *testing.T) {
	f := newFakeOpenAPI(t)
	a := f.client()
	ctx := context.Background()

	if err := a.ReplyWebhook(ctx, webhookMsg(f, ConversationGroup, time.Hour), Text("好的"), true); err != nil {
		t.Fatal(err)
	}
	content := f.webhook["text"].(map[string]any)["content"].(string)
	if !strings.HasSuffix(content, "@manager7675") || f.webhook["at"] == nil {
		t.Fatalf("群聊应 @: %v", f.webhook)
	}

	if err := a.ReplyWebhook(ctx, webhookMsg(f, ConversationSingle, time.Hour), Markdown("t", "好的"), true); err != nil {
		t.Fatal(err)
	}
	if f.webhook["at"] != nil || strings.Contains(f.webhook["markdown"].(map[string]any)["text"].(string), "@") {
		t.Fatalf("单聊不应 @: %v", f.webhook)
	}
}

// webhook 过期 → Reply 退回主动发送接口（群发群、单聊发给发送人）。
func TestReplyFallsBackWhenWebhookExpired(t *testing.T) {
	f := newFakeOpenAPI(t)
	a := f.client()
	ctx := context.Background()

	if err := a.ReplyWebhook(ctx, webhookMsg(f, ConversationGroup, -time.Minute), Text("x"), false); !errors.Is(err, ErrWebhookExpired) {
		t.Fatalf("应报过期: %v", err)
	}
	if err := a.Reply(ctx, webhookMsg(f, ConversationGroup, -time.Minute), Text("x"), true); err != nil {
		t.Fatal(err)
	}
	if f.lastPath != "/v1.0/robot/groupMessages/send" || f.last["openConversationId"] != "cidX==" {
		t.Fatalf("群聊应退回群发: %s %v", f.lastPath, f.last)
	}
	if err := a.Reply(ctx, webhookMsg(f, ConversationSingle, -time.Minute), Text("x"), true); err != nil {
		t.Fatal(err)
	}
	if f.lastPath != "/v1.0/robot/oToMessages/batchSend" {
		t.Fatalf("单聊应退回单发: %s", f.lastPath)
	}
}
