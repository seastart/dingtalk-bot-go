package dingbot

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type cardCall struct {
	method, path string
	body         map[string]any
}

// fakeCardAPI 记录卡片接口的调用序列；qpsOnce 为 true 时下一次卡片更新回 403 QpsLimit。
type fakeCardAPI struct {
	srv     *httptest.Server
	mu      sync.Mutex
	calls   []cardCall
	qpsOnce atomic.Bool
	deny    bool
}

func newFakeCardAPI(t *testing.T) *fakeCardAPI {
	f := &fakeCardAPI{}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1.0/oauth2/accessToken" {
			_, _ = w.Write([]byte(`{"accessToken":"tok","expireIn":7200}`))
			return
		}
		if f.deny {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"code":"Forbidden.AccessDenied.AccessTokenPermissionDenied","message":"应用尚未开通所需的权限：[Card.Instance.Write]"}`))
			return
		}
		if r.Method == http.MethodPut && f.qpsOnce.CompareAndSwap(true, false) {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"code":"Forbidden.AccessDenied.QpsLimitForApi","message":"qps"}`))
			return
		}
		body := map[string]any{}
		_ = json.NewDecoder(r.Body).Decode(&body)
		f.mu.Lock()
		f.calls = append(f.calls, cardCall{r.Method, r.URL.Path, body})
		f.mu.Unlock()
		if r.URL.Path == "/v1.0/card/instances/deliver" {
			_, _ = w.Write([]byte(`{"success":true,"result":[{"success":true}]}`))
			return
		}
		_, _ = w.Write([]byte(`{"success":true,"result":true}`))
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeCardAPI) client() *APIClient {
	return NewAPIClient(APIConfig{ClientID: "dingFAKErobot", ClientSecret: "s", OpenAPIHost: f.srv.URL})
}

func flowOf(c cardCall) string {
	cd, _ := c.body["cardData"].(map[string]any)
	pm, _ := cd["cardParamMap"].(map[string]any)
	s, _ := pm["flowStatus"].(string)
	return s
}

func TestAICardLifecycle(t *testing.T) {
	f := newFakeCardAPI(t)
	ctx := context.Background()
	card, err := f.client().CreateAICard(ctx, CardTarget{OpenConversationID: "cidG=="}, "请稍等…")
	if err != nil {
		t.Fatal(err)
	}
	if err := card.Update(ctx, "**#5** 已完成", true); err != nil {
		t.Fatal(err)
	}
	if err := card.Update(ctx, "x", true); err == nil {
		t.Fatal("收口后不应再能更新")
	}
	want := []string{
		"POST /v1.0/card/instances", "POST /v1.0/card/instances/deliver", "PUT /v1.0/card/instances",
		"PUT /v1.0/card/streaming", "PUT /v1.0/card/instances",
	}
	if len(f.calls) != len(want) {
		t.Fatalf("调用序列: %+v", f.calls)
	}
	for i, w := range want {
		if got := f.calls[i].method + " " + f.calls[i].path; got != w {
			t.Fatalf("第 %d 步 %s，想要 %s", i, got, w)
		}
	}
	if f.calls[0].body["cardTemplateId"] != DefaultAICardTemplateID {
		t.Fatal("默认应用官方公共模板")
	}
	if f.calls[1].body["openSpaceId"] != "dtv1.card//IM_GROUP.cidG==" {
		t.Fatalf("群投放 openSpaceId: %v", f.calls[1].body["openSpaceId"])
	}
	if flowOf(f.calls[2]) != cardFlowInputing || flowOf(f.calls[4]) != cardFlowFinished {
		t.Fatalf("状态流转应是 输入中 → 完成: %s → %s", flowOf(f.calls[2]), flowOf(f.calls[4]))
	}
	if f.calls[3].body["isFinalize"] != true {
		t.Fatal("收口时流式接口须 isFinalize")
	}
}

func TestAICardSingleTargetAndQPSRetry(t *testing.T) {
	old := cardQPSBackoff
	cardQPSBackoff = 10 * time.Millisecond
	defer func() { cardQPSBackoff = old }()

	f := newFakeCardAPI(t)
	ctx := context.Background()
	card, err := f.client().CreateAICard(ctx, CardTarget{UserID: "manager7675"}, "…")
	if err != nil {
		t.Fatal(err)
	}
	if f.calls[1].body["openSpaceId"] != "dtv1.card//IM_ROBOT.manager7675" {
		t.Fatalf("单聊投放 openSpaceId: %v", f.calls[1].body["openSpaceId"])
	}
	f.qpsOnce.Store(true)
	if err := card.Update(ctx, "中间帧", false); err != nil {
		t.Fatalf("QpsLimit 应退避后重试成功: %v", err)
	}
}

func TestAICardPermissionDenied(t *testing.T) {
	f := newFakeCardAPI(t)
	f.deny = true
	_, err := f.client().CreateAICard(context.Background(), CardTarget{UserID: "manager7675"}, "…")
	if !IsPermissionDenied(err) {
		t.Fatalf("应识别为缺权限: %v", err)
	}
	if _, err := f.client().CreateAICard(context.Background(), CardTarget{}, "…"); err == nil {
		t.Fatal("没给投放位置应报错")
	}
}
