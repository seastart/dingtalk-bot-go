package dingbot

import (
	"bytes"
	"context"
	"image"
	"image/color"
	"image/png"
	"os"
	"testing"
	"time"
)

// 真机用例：默认跳过。需要 DINGTALK_CLIENT_ID / DINGTALK_CLIENT_SECRET / DINGTALK_TEST_USER（收件人 userId）。
//
//	DINGTALK_REAL=1 go test -run TestRealSendImage -v
func TestRealSendImage(t *testing.T) {
	if os.Getenv("DINGTALK_REAL") == "" {
		t.Skip("未设 DINGTALK_REAL，跳过真机用例")
	}
	user := os.Getenv("DINGTALK_TEST_USER")
	a := NewAPIClient(APIConfig{ClientID: os.Getenv("DINGTALK_CLIENT_ID"), ClientSecret: os.Getenv("DINGTALK_CLIENT_SECRET")})
	img := image.NewRGBA(image.Rect(0, 0, 120, 120))
	for x := 0; x < 120; x++ {
		for y := 0; y < 120; y++ {
			img.Set(x, y, color.RGBA{uint8(x * 2), uint8(y * 2), 180, 255})
		}
	}
	var buf bytes.Buffer
	_ = png.Encode(&buf, img)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	id, err := a.UploadImage(ctx, "dingbot-test.png", buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("mediaId=%s", id)
	if _, err := a.SendToUsers(ctx, []string{user}, Image(id)); err != nil {
		t.Fatal(err)
	}
}

// DINGTALK_REAL=1 go test -run TestRealAICard -v
func TestRealAICard(t *testing.T) {
	if os.Getenv("DINGTALK_REAL") == "" {
		t.Skip("未设 DINGTALK_REAL，跳过真机用例")
	}
	a := NewAPIClient(APIConfig{ClientID: os.Getenv("DINGTALK_CLIENT_ID"), ClientSecret: os.Getenv("DINGTALK_CLIENT_SECRET")})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	card, err := a.CreateAICard(ctx, CardTarget{UserID: os.Getenv("DINGTALK_TEST_USER")}, "🌿 dingbot 卡片测试：请稍等…")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Second)
	if err := card.Update(ctx, "🌿 dingbot 卡片测试：还在处理…", false); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Second)
	if err := card.Update(ctx, "**dingbot 卡片测试完成**\n- 原地更新\n- 已收口", true); err != nil {
		t.Fatal(err)
	}
}
