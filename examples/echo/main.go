// echo：收到什么就回什么（群聊里 @ 发送人），附件则回报下载到的字节数。
//
//	DINGTALK_CLIENT_ID=... DINGTALK_CLIENT_SECRET=... go run ./examples/echo
//
// 加 -frames frames.jsonl 可把每条原始回调落盘，排查平台新字段时用。
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"sync"

	dingbot "github.com/seastart/dingtalk-bot-go"
)

func main() {
	framesPath := flag.String("frames", "", "把原始回调追加写入该文件（JSON Lines）")
	flag.Parse()

	id, secret := os.Getenv("DINGTALK_CLIENT_ID"), os.Getenv("DINGTALK_CLIENT_SECRET")
	if id == "" || secret == "" {
		fmt.Fprintln(os.Stderr, "需要环境变量 DINGTALK_CLIENT_ID / DINGTALK_CLIENT_SECRET")
		os.Exit(2)
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	var frames *os.File
	var framesMu sync.Mutex
	if *framesPath != "" {
		f, err := os.OpenFile(*framesPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
		if err != nil {
			log.Error("打开 frames 文件", "err", err)
			os.Exit(1)
		}
		frames = f
	}

	api := dingbot.NewAPIClient(dingbot.APIConfig{ClientID: id, ClientSecret: secret, Logger: log})
	cli := dingbot.NewClient(dingbot.Config{ClientID: id, ClientSecret: secret, Logger: log})

	cli.OnConnectionState(func(s dingbot.ConnState, err error) { log.Info("连接状态", "state", s, "err", err) })
	cli.OnMessage(func(ctx context.Context, m *dingbot.Message) {
		if frames != nil {
			framesMu.Lock()
			_, _ = frames.Write(append(append([]byte{}, m.Raw...), '\n'))
			framesMu.Unlock()
		}
		log.Info("收到", "type", m.MsgType, "group", m.IsGroup(), "staff", m.SenderStaffID, "text", m.PlainText())

		reply := "echo: " + m.PlainText()
		for _, a := range m.Attachments() {
			body, _, err := api.Download(ctx, a.DownloadCode)
			if err != nil {
				reply += fmt.Sprintf("\n[%s 下载失败: %v]", a.Kind, err)
				continue
			}
			n, _ := io.Copy(io.Discard, body)
			body.Close()
			reply += fmt.Sprintf("\n[%s %s %d 字节]", a.Kind, a.FileName, n)
		}
		if err := api.Reply(ctx, m, dingbot.Text(reply), true); err != nil {
			log.Error("回复失败", "err", err)
		}
	})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	_ = cli.RunForever(ctx)
}
