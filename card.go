package dingbot

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

// DefaultAICardTemplateID 是钉钉官方提供、所有企业内部应用机器人共用的公共 AI 卡片模板
// （DingTalk-Real-AI 官方连接器同款），不需要去卡片平台自建模板。
// 应用需开通两项「无需审批」的权限：Card.Instance.Write（创建投放/更新）与 Card.Streaming.Write（流式更新）。
const DefaultAICardTemplateID = "02fcf2f4-5e02-4a85-b672-46d1f715543e.schema"

// AI 卡片模板预置的流转状态（cardParamMap.flowStatus）。
const (
	cardFlowInputing = "2" // 输入中（带动画）
	cardFlowFinished = "3" // 完成
)

// CardTarget 是卡片的投放位置：群聊填 OpenConversationID，单聊填 UserID（二选一）。
type CardTarget struct {
	OpenConversationID string
	UserID             string
}

// AICard 是一张已投放的 AI 卡片。它是钉钉里唯一能「发出去之后原地改内容」的消息形态
// （普通消息发出后不能编辑），故「先占位、后覆盖」只能靠它。
//
// 代价要知道：卡片里不能 @人；外观是卡片而非普通气泡。并发安全。
type AICard struct {
	api        *APIClient
	outTrackID string

	mu       sync.Mutex
	finished bool
}

// CreateAICard 创建并投放一张 AI 卡片，初始即「输入中」状态并显示 content。
func (a *APIClient) CreateAICard(ctx context.Context, target CardTarget, content string) (*AICard, error) {
	if (target.OpenConversationID == "") == (target.UserID == "") {
		return nil, errors.New("dingbot: CardTarget 须且只能填群或人之一")
	}
	id, err := newOutTrackID()
	if err != nil {
		return nil, err
	}
	if err := a.callMethod(ctx, http.MethodPost, "/v1.0/card/instances", map[string]any{
		"cardTemplateId":        a.cardTemplate,
		"outTrackId":            id,
		"cardData":              map[string]any{"cardParamMap": map[string]string{"config": `{"autoLayout":true}`}},
		"callbackType":          "STREAM",
		"imGroupOpenSpaceModel": map[string]any{"supportForward": true},
		"imRobotOpenSpaceModel": map[string]any{"supportForward": true},
	}, nil); err != nil {
		return nil, fmt.Errorf("dingbot: 创建 AI 卡片: %w", err)
	}

	deliver := map[string]any{"outTrackId": id, "userIdType": 1}
	if target.OpenConversationID != "" {
		deliver["openSpaceId"] = "dtv1.card//IM_GROUP." + target.OpenConversationID
		deliver["imGroupOpenDeliverModel"] = map[string]any{"robotCode": a.robotCode}
	} else {
		deliver["openSpaceId"] = "dtv1.card//IM_ROBOT." + target.UserID
		deliver["imRobotOpenDeliverModel"] = map[string]any{"spaceType": "IM_ROBOT", "robotCode": a.robotCode}
	}
	var dr struct {
		Result []struct {
			Success  bool   `json:"success"`
			ErrorMsg string `json:"errorMsg"`
		} `json:"result"`
	}
	if err := a.callMethod(ctx, http.MethodPost, "/v1.0/card/instances/deliver", deliver, &dr); err != nil {
		return nil, fmt.Errorf("dingbot: 投放 AI 卡片: %w", err)
	}
	for _, r := range dr.Result {
		if !r.Success {
			return nil, fmt.Errorf("dingbot: 投放 AI 卡片被拒: %s", r.ErrorMsg)
		}
	}

	c := &AICard{api: a, outTrackID: id}
	if err := c.Update(ctx, content, false); err != nil {
		return nil, err
	}
	return c, nil
}

// Update 覆盖卡片内容（markdown）。finished=true 收口：卡片从「输入中」切到「完成」，此后不应再调。
//
// 调用方负责节流中间帧：钉钉卡片接口整体约 40 QPS 且对单卡片高频更新会限流（403 QpsLimit）。
// 限流时本方法退避后重试一次——收口帧失败会让卡片永远停在「输入中」，比晚两秒难受得多。
func (c *AICard) Update(ctx context.Context, content string, finished bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.finished {
		return errors.New("dingbot: AI 卡片已收口")
	}
	flow := cardFlowInputing
	if finished {
		// 先用流式接口把「输入中」生命周期正式关掉，再写终态；只写终态在部分客户端上会留下转圈。
		_ = c.retryQPS(ctx, func() error {
			return c.api.callMethod(ctx, http.MethodPut, "/v1.0/card/streaming", map[string]any{
				"outTrackId": c.outTrackID, "guid": c.outTrackID + "-" + fmt.Sprint(time.Now().UnixNano()),
				"key": "msgContent", "content": content, "isFull": true, "isFinalize": true, "isError": false,
			}, nil)
		})
		flow = cardFlowFinished
	}
	body := map[string]any{
		"outTrackId": c.outTrackID,
		"cardData": map[string]any{"cardParamMap": map[string]string{
			"flowStatus":        flow,
			"msgContent":        content,
			"staticMsgContent":  "",
			"sys_full_json_obj": `{"order":["msgContent"]}`,
			"config":            `{"autoLayout":true}`,
		}},
	}
	if finished {
		body["cardUpdateOptions"] = map[string]any{"updateCardDataByKey": true}
	}
	if err := c.retryQPS(ctx, func() error {
		return c.api.callMethod(ctx, http.MethodPut, "/v1.0/card/instances", body, nil)
	}); err != nil {
		return fmt.Errorf("dingbot: 更新 AI 卡片: %w", err)
	}
	if finished {
		c.finished = true
	}
	return nil
}

// cardQPSBackoff 遇到 QpsLimit 后重试前的停顿。变量而非常量：测试要把它调小。
var cardQPSBackoff = 2 * time.Second

func (c *AICard) retryQPS(ctx context.Context, f func() error) error {
	err := f()
	var ae *APIError
	if err == nil || !errors.As(err, &ae) || !strings.Contains(ae.Code, "QpsLimit") {
		return err
	}
	select {
	case <-ctx.Done():
		return err
	case <-time.After(cardQPSBackoff):
	}
	return f()
}

func newOutTrackID() (string, error) {
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return "dingbot-" + hex.EncodeToString(b), nil
}

// IsPermissionDenied 报告错误是否因应用缺权限（如没开 Card.Instance.Write）。
// 这类错误重试无用，调用方应降级并提示去开发者后台开通。
func IsPermissionDenied(err error) bool {
	var ae *APIError
	return errors.As(err, &ae) && strings.Contains(ae.Code, "PermissionDenied")
}

// callMethod 是 call 的任意 HTTP 方法版（卡片更新用 PUT）。
func (a *APIClient) callMethod(ctx context.Context, method, path string, in, out any) error {
	for attempt := 0; ; attempt++ {
		tok, err := a.accessToken(ctx, attempt > 0)
		if err != nil {
			return err
		}
		err = a.doJSONMethod(ctx, method, path, tok, in, out)
		var ae *APIError
		if attempt == 0 && errors.As(err, &ae) && ae.authInvalid() {
			continue
		}
		return err
	}
}
