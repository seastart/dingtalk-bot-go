package dingbot

import (
	"testing"
	"time"
)

// 以下帧取自真机回调（2026-10），ID 已替换为形态一致的假值。

const frameGroupText = `{"senderPlatform":"Mac","conversationId":"cidFAKEgroup0000000000==","atUsers":[{"dingtalkId":"$:LWCP_v1:$FAKEbot"}],"chatbotCorpId":"dingFAKEcorp","chatbotUserId":"$:LWCP_v1:$FAKEbot","openThreadId":"cidFAKEgroup0000000000==","msgId":"msgFAKE1==","senderNick":"张三","isAdmin":true,"senderStaffId":"manager7675","sessionWebhookExpiredTime":1793266608764,"createAt":1793261208501,"senderCorpId":"dingFAKEcorp","conversationType":"2","senderId":"$:LWCP_v1:$FAKEsender==","conversationTitle":"测试群","isInAtList":true,"sessionWebhook":"https://oapi.dingtalk.com/robot/sendBySession?session=FAKE","text":{"content":" 你好"},"robotCode":"dingFAKErobot","msgtype":"text"}`

const frameGroupRichText = `{"conversationId":"cidFAKEgroup0000000000==","conversationType":"2","isInAtList":true,"senderStaffId":"manager7675","msgtype":"richText","content":{"richText":[{"text":"@Vinez"},{"text":"\n"},{"pictureDownloadCode":"pdcFAKE","downloadCode":"dcFAKEpic","type":"picture"},{"text":"看下这个"}]}}`

const frameSingleRichText = `{"conversationId":"cidFAKEsingle==","conversationType":"1","senderStaffId":"manager7675","msgtype":"richText","content":{"richText":[{"pictureDownloadCode":"pdc","downloadCode":"dcPic","type":"picture"},{"text":"\n"},{"text":"@张三 这是带图的"}]}}`

const frameSingleFile = `{"conversationId":"cidFAKEsingle==","conversationType":"1","senderStaffId":"manager7675","msgtype":"file","content":{"spaceId":"123","fileName":"部署文档.docx","downloadCode":"dcFile","fileId":"456"}}`

func TestParseGroupText(t *testing.T) {
	m, err := ParseMessage([]byte(frameGroupText))
	if err != nil {
		t.Fatal(err)
	}
	if !m.IsGroup() || m.SenderStaffID != "manager7675" || m.ConversationID != "cidFAKEgroup0000000000==" {
		t.Fatalf("字段不对: %+v", m)
	}
	if m.Text != " 你好" || m.PlainText() != "你好" {
		t.Fatalf("text 剔除前导空格: %q / %q", m.Text, m.PlainText())
	}
	if m.OpenThreadID == "" || len(m.Raw) == 0 {
		t.Fatal("未建模字段应保留在 Raw / OpenThreadID")
	}
	if got := m.WebhookExpiresAt(); !got.Equal(time.UnixMilli(1793266608764)) {
		t.Fatalf("webhook 过期时刻: %v", got)
	}
	if len(m.Attachments()) != 0 {
		t.Fatal("纯文本不应有附件")
	}
}

func TestParseGroupRichTextStripsBotMention(t *testing.T) {
	m, err := ParseMessage([]byte(frameGroupRichText))
	if err != nil {
		t.Fatal(err)
	}
	if got := m.PlainText(); got != "看下这个" {
		t.Fatalf("应剔除独立的 @机器人 段: %q", got)
	}
	att := m.Attachments()
	if len(att) != 1 || att[0].Kind != MsgTypePicture || att[0].DownloadCode != "dcFAKEpic" {
		t.Fatalf("richText 里的图: %+v", att)
	}
}

// 单聊里不存在 @机器人，人写的 "@张三" 必须原样保留。
func TestParseSingleRichTextKeepsMentions(t *testing.T) {
	m, err := ParseMessage([]byte(frameSingleRichText))
	if err != nil {
		t.Fatal(err)
	}
	if got := m.PlainText(); got != "@张三 这是带图的" {
		t.Fatalf("单聊不应剔除 @: %q", got)
	}
}

func TestParseSingleFile(t *testing.T) {
	m, err := ParseMessage([]byte(frameSingleFile))
	if err != nil {
		t.Fatal(err)
	}
	if m.File == nil || m.File.FileName != "部署文档.docx" {
		t.Fatalf("文件消息: %+v", m.File)
	}
	att := m.Attachments()
	if len(att) != 1 || att[0].Kind != MsgTypeFile || att[0].FileName != "部署文档.docx" || att[0].DownloadCode != "dcFile" {
		t.Fatalf("附件: %+v", att)
	}
}

func TestParseMalformed(t *testing.T) {
	if _, err := ParseMessage([]byte(`{`)); err == nil {
		t.Fatal("坏 JSON 应报错")
	}
	if _, err := ParseMessage([]byte(`{"msgtype":"richText","content":"oops"}`)); err == nil {
		t.Fatal("content 形态不对应报错")
	}
}

func TestIsLoneMention(t *testing.T) {
	for s, want := range map[string]bool{"@Vinez": true, " @Vinez ": true, "@": false, "@a b": false, "hi": false, "@张三\n看": false} {
		if got := isLoneMention(s); got != want {
			t.Errorf("isLoneMention(%q)=%v want %v", s, got, want)
		}
	}
}
