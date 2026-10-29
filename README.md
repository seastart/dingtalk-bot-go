# dingtalk-bot-go

钉钉企业机器人（企业内部应用机器人）Go 库，面向常驻服务 / gateway 场景：

- **Stream 长连接收消息**：不需要公网地址；自带有上限的指数退避重连、保活与僵死连接识别，ctx 取消即返回，回调 panic 不会带崩进程。
- **发消息**：sessionWebhook 就地回复（群聊可 @ 发送人）+ 主动发送（群聊 / 单聊），webhook 过期自动退回主动发送。
- **附件下载**：图片、文件、语音、视频（含图文混排里的图），以流的形式返回。
- **accessToken 管理**：缓存、提前刷新，被服务端提前作废时自动刷新重试。
- 唯一依赖 `github.com/gorilla/websocket`。

官方文档：[配置企业机器人](https://open.dingtalk.com/document/dingstart/configure-the-robot-application) · [机器人接收消息](https://open.dingtalk.com/document/dingstart/robot-receive-message) · [机器人回复/发送消息](https://open.dingtalk.com/document/dingstart/robot-reply-and-send-messages) · [消息类型](https://open.dingtalk.com/document/development/robot-message-type)

## 为什么不用官方 SDK

- [`dingtalk-stream-sdk-go`](https://github.com/open-dingtalk/dingtalk-stream-sdk-go)：重连固定 3 秒、无退避无上限，且不认 ctx；连接循环退出时关闭读 channel 而读 goroutine 无 recover，存在 `send on closed channel` 拖垮宿主进程的竞态。
- [`alibabacloud-go/dingtalk`](https://github.com/alibabacloud-go/dingtalk)：全量 OpenAPI 生成代码（约 26MB + tea 依赖链），机器人场景只用得到其中几个接口。本库的字段名以它为准，但不依赖它。

## 安装

需要 Go 1.25+。

```bash
go get github.com/seastart/dingtalk-bot-go
```

包名为 `dingbot`。

## 快速开始

```go
api := dingbot.NewAPIClient(dingbot.APIConfig{ClientID: id, ClientSecret: secret})
cli := dingbot.NewClient(dingbot.Config{ClientID: id, ClientSecret: secret})

cli.OnMessage(func(ctx context.Context, m *dingbot.Message) {
	// m.SenderStaffID 是发送者在企业内的 userId；m.PlainText() 已剔除 @机器人
	_ = api.Reply(ctx, m, dingbot.Text("收到："+m.PlainText()), true)
})
cli.OnConnectionState(func(s dingbot.ConnState, err error) {
	log.Println("stream", s, err) // 每次失败都会回调，去重节流由你决定
})

_ = cli.RunForever(ctx) // 阻塞到 ctx 取消
```

完整示例见 [`examples/echo`](./examples/echo)：

```bash
DINGTALK_CLIENT_ID=... DINGTALK_CLIENT_SECRET=... go run ./examples/echo
```

## 应用侧配置

1. [开发者后台](https://open-dev.dingtalk.com) 创建**企业内部应用**，在「凭证与基础信息」拿 ClientID / ClientSecret。
2. 「应用能力 → 机器人」开启机器人，**消息接收模式选 Stream 模式**，点「发布」保存机器人配置。
3. 「版本管理与发布」**创建并发布一个版本**，设置可用范围。
   ★ 这一步不能省：不发版本，群里搜不到机器人；回调里的 `senderStaffId` 也只在发布之后才下发。企业内部应用的版本发布免审，未认证的组织也能发。
4. 群设置 → 机器人 → 添加机器人，搜索机器人名；或在钉钉主搜索框搜索后单聊。

## 平台行为备忘（真机实测，2026-10）

| 现象 | 影响 |
|---|---|
| 同一 ClientID 建多条 Stream 连接**不会互踢**，而是**逐条负载均衡**到其中一条 | 两个进程误用同一凭据时，各自静默只收到一半消息，没有任何报错。一个应用只给一个进程用 |
| sessionWebhook 有效期 90 分钟 | `Reply` 过期后自动改走主动发送 |
| 群聊里 `at.atUserIds` + 正文 `@userId` 渲染为真 @；**单聊里会原样显示成一串数字** | `ReplyWebhook` 只在群聊附加 @ |
| 主动发送接口不支持 @ | 群里的异步通知无法提醒到具体的人 |
| text 消息里平台已剔除 @机器人，留一个前导空格；richText 里 `@机器人名` 是独立文本段 | `PlainText()` 两种都处理 |
| 群聊里 @机器人时不能附带文件 / 语音 / 视频，单聊可以；图片两边都行（走 richText） | — |
| 附件下载地址是 `http://` 的 OSS 签名链接，约 1 小时有效 | 拿到 downloadCode 后尽快下载 |
| 主动发送、取 token 不计入「付费 API 调用量」；Webhook 与 Stream 用量标准版不限额 | — |
| 回调里有文档未写的 `openThreadId`（目前等于 `conversationId`） | 保留在 `Message.OpenThreadID` 与 `Message.Raw` |

## 设计约定

- **库只放钉钉协议知识，不放业务策略**：日志器由调用方注入（`Logger` 为 nil 即完全静默）；连接状态每次失败都回调，节流由调用方决定。
- **收到回调帧当下就回 ACK**，回调在独立 goroutine 中执行——回调里通常要做慢事，迟迟不 ACK 只会招来重投。
- **存活判据是「读到东西」**：`KeepAlive + PongTimeout` 内没收到任何帧（含 pong）即判定连接僵死并重连。「连着但收不到」比「断了」更难发现。
