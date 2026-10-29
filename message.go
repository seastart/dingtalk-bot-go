package dingbot

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// 会话类型（回调里的 conversationType）。
const (
	ConversationSingle = "1" // 单聊（人与机器人）
	ConversationGroup  = "2" // 群聊
)

// 消息类型（回调里的 msgtype）。
const (
	MsgTypeText     = "text"
	MsgTypeRichText = "richText" // 图文混排；单聊/群聊发图都走这里
	MsgTypePicture  = "picture"
	MsgTypeFile     = "file"  // 仅单聊能收到
	MsgTypeAudio    = "audio" // 仅单聊能收到
	MsgTypeVideo    = "video" // 仅单聊能收到
)

// Message 是一条机器人回调消息（Stream topic /v1.0/im/bot/messages/get 的 data）。
//
// 常用字段显式建模，原始 JSON 保留在 Raw 里——平台常加文档里没写的字段（如 openThreadId），
// 调用方需要时可自行解析，不必等库升级。
type Message struct {
	MsgID            string `json:"msgId"`
	MsgType          string `json:"msgtype"`
	ConversationID   string `json:"conversationId"`   // 会话 ID：群聊即 openConversationId，主动发群消息用它
	ConversationType string `json:"conversationType"` // ConversationSingle / ConversationGroup
	// ConversationTitle 群名，单聊为空。
	ConversationTitle string `json:"conversationTitle"`
	// OpenThreadID 文档未写；实测目前与 ConversationID 相同，保留以备钉钉后续启用话题。
	OpenThreadID string `json:"openThreadId"`

	// SenderStaffID 发送者在本企业的 userId——身份请认它。
	// ★ 应用【发布版本之后】才下发；外部群里的外部成员恒为空。
	SenderStaffID  string `json:"senderStaffId"`
	SenderUnionID  string `json:"senderUnionId"`
	SenderID       string `json:"senderId"` // 加密 ID，不稳定可比，勿用作身份
	SenderNick     string `json:"senderNick"`
	SenderCorpID   string `json:"senderCorpId"`
	SenderPlatform string `json:"senderPlatform"`
	IsAdmin        bool   `json:"isAdmin"`

	ChatbotCorpID string `json:"chatbotCorpId"`
	ChatbotUserID string `json:"chatbotUserId"`
	RobotCode     string `json:"robotCode"` // 企业内部应用里等于 ClientID

	// OriginalMsgID 引用回复时被引消息的 msgId（同 Quote.MsgID，平台在顶层也给一份）。
	OriginalMsgID string `json:"originalMsgId"`
	// OriginalProcessQueryKey 被引消息若是【本机器人经主动发送接口发出的】，这里就是当时
	// 接口返回的 processQueryKey——调用方记下「processQueryKey ↔ 业务对象」即可精确反查，
	// 不必做文本匹配。引用人发的消息、或机器人经 sessionWebhook 回复的消息时为空。
	OriginalProcessQueryKey string `json:"originalProcessQueryKey"`

	IsInAtList bool     `json:"isInAtList"`
	AtUsers    []AtUser `json:"atUsers"`

	// SessionWebhook 本会话的回复地址，有效期见 SessionWebhookExpiredTime（实测 90 分钟）。
	SessionWebhook            string `json:"sessionWebhook"`
	SessionWebhookExpiredTime int64  `json:"sessionWebhookExpiredTime"` // 毫秒时间戳
	CreateAt                  int64  `json:"createAt"`                  // 毫秒时间戳

	// 以下按 MsgType 只有一个有值（由 ParseMessage 从 text / content 解出）。
	// ★ 必须 json:"-"：encoding/json 字段名大小写不敏感，不打标签的 Text 会去匹配回调里的
	// "text" 对象而整条解析失败。
	Text     string         `json:"-"`
	RichText []RichTextItem `json:"-"`
	Picture  *MediaContent  `json:"-"`
	File     *MediaContent  `json:"-"`
	Audio    *MediaContent  `json:"-"`
	Video    *MediaContent  `json:"-"`
	// Quote 被引用的消息（引用回复时非空；只出现在 text 消息里）。
	Quote *Quote `json:"-"`

	Raw json.RawMessage `json:"-"`
}

// AtUser 是被 @ 的人。机器人自己也在列表里（只有 DingtalkID、没有 StaffID）。
type AtUser struct {
	DingtalkID string `json:"dingtalkId"`
	StaffID    string `json:"staffId"`
	UnionID    string `json:"unionId"`
}

// RichTextItem 是图文混排里的一段：文本段 Text 非空，图片段 Type=="picture" 且带 DownloadCode。
type RichTextItem struct {
	Type                string `json:"type"`
	Text                string `json:"text"`
	DownloadCode        string `json:"downloadCode"`
	PictureDownloadCode string `json:"pictureDownloadCode"`
}

// MediaContent 是图片 / 文件 / 语音 / 视频消息的 content。下载用 DownloadCode（见 APIClient.Download）。
type MediaContent struct {
	DownloadCode        string `json:"downloadCode"`
	PictureDownloadCode string `json:"pictureDownloadCode"`
	FileName            string `json:"fileName"`
	FileID              string `json:"fileId"`
	SpaceID             string `json:"spaceId"`
	Recognition         string `json:"recognition"` // 语音识别文本
	Duration            int64  `json:"duration"`
	VideoType           string `json:"videoType"`
}

// ParseMessage 解析一条机器人回调。
func ParseMessage(data []byte) (*Message, error) {
	var m Message
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("dingbot: 解析机器人消息: %w", err)
	}
	m.Raw = append(json.RawMessage(nil), data...)

	// 正文的位置随 msgtype 变：text 在 text.content，其余在 content（对象）。
	var body struct {
		Text struct {
			Content    string          `json:"content"`
			IsReplyMsg bool            `json:"isReplyMsg"`
			RepliedMsg json.RawMessage `json:"repliedMsg"`
		} `json:"text"`
		Content json.RawMessage `json:"content"`
	}
	if err := json.Unmarshal(data, &body); err != nil {
		return nil, fmt.Errorf("dingbot: 解析消息正文: %w", err)
	}
	switch m.MsgType {
	case MsgTypeText:
		m.Text = body.Text.Content
		if body.Text.IsReplyMsg && len(body.Text.RepliedMsg) > 0 {
			q, err := parseQuote(body.Text.RepliedMsg)
			if err != nil {
				return nil, err
			}
			m.Quote = q
		}
	case MsgTypeRichText:
		var rt struct {
			RichText []RichTextItem `json:"richText"`
		}
		if err := json.Unmarshal(body.Content, &rt); err != nil {
			return nil, fmt.Errorf("dingbot: 解析 richText: %w", err)
		}
		m.RichText = rt.RichText
	case MsgTypePicture, MsgTypeFile, MsgTypeAudio, MsgTypeVideo:
		var mc MediaContent
		if err := json.Unmarshal(body.Content, &mc); err != nil {
			return nil, fmt.Errorf("dingbot: 解析 %s: %w", m.MsgType, err)
		}
		switch m.MsgType {
		case MsgTypePicture:
			m.Picture = &mc
		case MsgTypeFile:
			m.File = &mc
		case MsgTypeAudio:
			m.Audio = &mc
		case MsgTypeVideo:
			m.Video = &mc
		}
	}
	return &m, nil
}

// IsGroup 报告是否群聊。
func (m *Message) IsGroup() bool { return m.ConversationType == ConversationGroup }

// WebhookExpiresAt 返回 sessionWebhook 的过期时刻；未下发时为零值。
func (m *Message) WebhookExpiresAt() time.Time {
	if m.SessionWebhookExpiredTime == 0 {
		return time.Time{}
	}
	return time.UnixMilli(m.SessionWebhookExpiredTime)
}

// PlainText 返回人写的文字（已剔除 @机器人、首尾空白）。
//
// 两种消息剔除方式不同（真机实测）：
//   - text：平台已把 @机器人 去掉，只留一个前导空格 → TrimSpace 即可；
//   - richText：群聊里 "@机器人名" 是一个【独立】文本段（后面紧跟一个 "\n" 段）→ 去掉第一个
//     「整段就是一个 @词」的文本段。只在群聊且本条 @ 了机器人时这么做，单聊里不存在 @机器人。
func (m *Message) PlainText() string {
	switch m.MsgType {
	case MsgTypeText:
		return strings.TrimSpace(m.Text)
	case MsgTypeAudio:
		if m.Audio != nil {
			return strings.TrimSpace(m.Audio.Recognition)
		}
	case MsgTypeRichText:
		var b strings.Builder
		stripped := !(m.IsGroup() && m.IsInAtList)
		for _, it := range m.RichText {
			if it.Text == "" {
				continue
			}
			if !stripped && isLoneMention(it.Text) {
				stripped = true
				continue
			}
			b.WriteString(it.Text)
		}
		return strings.TrimSpace(b.String())
	}
	return ""
}

// isLoneMention 判断一段文本是否恰好是一个 "@词"（无空白）。
func isLoneMention(s string) bool {
	s = strings.TrimSpace(s)
	return len(s) > 1 && s[0] == '@' && !strings.ContainsAny(s, " \t\n　")
}

// Attachment 是消息里一个可下载的媒体。
type Attachment struct {
	Kind         string // MsgTypePicture / MsgTypeFile / MsgTypeAudio / MsgTypeVideo
	DownloadCode string
	FileName     string // 仅文件消息有
}

// Attachments 汇总本条消息里所有可下载的媒体（含 richText 里的图片），按出现顺序。
func (m *Message) Attachments() []Attachment {
	var out []Attachment
	add := func(kind string, mc *MediaContent) {
		if mc != nil && mc.DownloadCode != "" {
			out = append(out, Attachment{Kind: kind, DownloadCode: mc.DownloadCode, FileName: mc.FileName})
		}
	}
	add(MsgTypePicture, m.Picture)
	add(MsgTypeFile, m.File)
	add(MsgTypeAudio, m.Audio)
	add(MsgTypeVideo, m.Video)
	for _, it := range m.RichText {
		if it.DownloadCode != "" {
			out = append(out, Attachment{Kind: MsgTypePicture, DownloadCode: it.DownloadCode})
		}
	}
	return out
}

// Quote 是被引用的那条消息（text.repliedMsg）。
//
// ★ 它的 richText 形态与入站 richText【不同】（真机 2026-10）：段类型键是 msgType 而不是 type，
// 文本在 content 而不是 text。照搬入站解析会把被引的图文混排整段丢掉。
type Quote struct {
	MsgID     string `json:"msgId"`
	MsgType   string `json:"msgType"`
	SenderID  string `json:"senderId"`  // 加密 ID，只能判「是不是同一个人」，不是 userId
	CreatedAt int64  `json:"createdAt"` // 毫秒时间戳

	Text  string       // 被引消息的文字（text / richText 的文本段 / 语音识别 / markdown 正文）
	Media []Attachment // 被引消息里可下载的媒体（downloadCode 实测可直接下载）
	Raw   json.RawMessage
}

func parseQuote(raw json.RawMessage) (*Quote, error) {
	var q Quote
	if err := json.Unmarshal(raw, &q); err != nil {
		return nil, fmt.Errorf("dingbot: 解析被引消息: %w", err)
	}
	q.Raw = append(json.RawMessage(nil), raw...)
	var c struct {
		Text     string `json:"text"`
		Content  string `json:"content"`
		Title    string `json:"title"`
		RichText []struct {
			MsgType      string `json:"msgType"`
			Content      string `json:"content"`
			Text         string `json:"text"`
			DownloadCode string `json:"downloadCode"`
		} `json:"richText"`
		MediaContent
	}
	var wrap struct {
		Content json.RawMessage `json:"content"`
	}
	_ = json.Unmarshal(raw, &wrap)
	if len(wrap.Content) > 0 && wrap.Content[0] == '{' {
		if err := json.Unmarshal(wrap.Content, &c); err != nil {
			return nil, fmt.Errorf("dingbot: 解析被引消息正文: %w", err)
		}
	}
	switch q.MsgType {
	case MsgTypeRichText:
		var parts []string
		for _, it := range c.RichText {
			switch {
			case it.DownloadCode != "":
				q.Media = append(q.Media, Attachment{Kind: MsgTypePicture, DownloadCode: it.DownloadCode})
			case it.Content != "" || it.Text != "":
				parts = append(parts, it.Content+it.Text)
			}
		}
		q.Text = strings.TrimSpace(strings.Join(parts, ""))
	case MsgTypePicture, MsgTypeFile, MsgTypeVideo:
		if c.DownloadCode != "" {
			q.Media = append(q.Media, Attachment{Kind: q.MsgType, DownloadCode: c.DownloadCode, FileName: c.FileName})
		}
	case MsgTypeAudio:
		q.Text = strings.TrimSpace(c.Recognition)
	default: // text、机器人发的 markdown 等：取得到什么文字就用什么
		for _, t := range []string{c.Text, c.Content, c.Title} {
			if strings.TrimSpace(t) != "" {
				q.Text = strings.TrimSpace(t)
				break
			}
		}
	}
	return &q, nil
}
