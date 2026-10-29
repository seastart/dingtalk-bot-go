// Package dingbot 是钉钉企业机器人（企业内部应用机器人）的 Go 库：
// Stream 长连接收消息 + OpenAPI / sessionWebhook 发消息 + 附件下载。
//
// 为什么不用官方的两个 SDK：
//   - dingtalk-stream-sdk-go：重连用 context.Background()、固定 3s 无退避无上限，且连接循环退出时
//     close(readChan) 而无 recover 的读 goroutine 仍可能往里写 → send on closed channel 直接拖垮宿主进程。
//     对一个常驻 daemon 来说，「库里任何一个 goroutine 都不能把调用方带走」是底线。
//   - alibabacloud-go/dingtalk：OpenAPI 全量生成代码（26MB + tea 依赖链），而机器人场景只用得到
//     五六个接口。本库的请求/响应字段名以它的生成代码为准（比文档站更新更可靠），但不依赖它。
//
// 本库只放【钉钉协议知识】（帧格式、字段形态、平台怪癖），不放任何业务策略；
// 日志器由调用方注入，不注入时完全静默。
//
// 真机实测的平台行为（2026-10，均已固化进本库的实现或注释）：
//   - 同一 ClientID 建多条 Stream 连接【不会互踢】，而是逐条消息负载均衡到其中一条——
//     两个进程误用同一凭据时各自静默只收到一半消息，不会有任何报错。
//   - sessionWebhook 有效期 90 分钟；群聊里 at.atUserIds 渲染成真 @，单聊里却会把 "@<userId>"
//     原样显示出来，故 ReplyWebhook 只在群聊里附加 @。
//   - 标准版组织里实测只有媒体上传（UploadImage）计入「付费 API 调用量」；主动发送、AI 卡片、附件下载、取 token
//     都不计（官方计费文档的免计费清单里没列它们，比实际保守）。
//   - text 消息里平台已剔除 @机器人（留一个前导空格）；richText 里 "@机器人名" 作为独立文本段原样保留。
//   - 群聊里 @机器人时不能附带文件 / 语音 / 视频（平台限制），单聊可以。
package dingbot
