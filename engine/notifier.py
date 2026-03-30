"""
息壤（Xi-Rang）通知模块

静默与唤醒机制：
- 平时绝对安静
- 只在有动作时推送一条极简指令

支持渠道（通过环境变量配置，可同时启用多个）：
- Telegram Bot
- 企业微信机器人
- 飞书机器人
- 钉钉机器人
- 控制台输出（兜底，始终启用）

配置方式：在项目根目录创建 .env 文件，或设置环境变量：
    TELEGRAM_BOT_TOKEN=xxx
    TELEGRAM_CHAT_ID=xxx
    WECOM_WEBHOOK=https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=xxx
    FEISHU_WEBHOOK=https://open.feishu.cn/open-apis/bot/v2/hook/xxx
    DINGTALK_WEBHOOK=https://oapi.dingtalk.com/robot/send?access_token=xxx
"""

import os
import json
import logging
from typing import Optional
from urllib.request import Request, urlopen
from urllib.error import URLError

logger = logging.getLogger("xirang.notifier")


# ── 消息格式化 ────────────────────────────────────────


def format_rebalance_message(report: dict) -> str:
    """常规再平衡通知"""
    weights = report.get("weights", {})
    w_str = " | ".join(f"{k} {v:.1%}" for k, v in weights.items())

    return (
        f"📊 息壤 · 再平衡通知\n"
        f"━━━━━━━━━━━━━━━\n"
        f"日期: {report['date']}\n"
        f"操作: {report['action']}\n"
        f"NAV: ${report['nav']:,.2f}\n"
        f"权重: {w_str}\n"
        f"回撤: {report['drawdown']:.2%}\n"
        f"累计调仓: {report['rebalance_count']} 次"
    )


def format_protection_message(report: dict) -> str:
    """风控警报通知"""
    weights = report.get("weights", {})
    w_str = " | ".join(f"{k} {v:.1%}" for k, v in weights.items())

    return (
        f"🚨 息壤 · 风控警报\n"
        f"━━━━━━━━━━━━━━━\n"
        f"日期: {report['date']}\n"
        f"状态: {report['state']}\n"
        f"操作: {report['action']}\n"
        f"NAV: ${report['nav']:,.2f}\n"
        f"回撤: {report['drawdown']:.2%}\n"
        f"SPY-TLT相关性: {report['spy_tlt_corr']:.2f}\n"
        f"权重: {w_str}\n"
        f"保护触发: {report['protection_count']} 次"
    )


def format_message(report: dict) -> Optional[str]:
    """
    根据报告内容决定是否发送通知，以及用什么格式。

    Returns:
        格式化的消息字符串，如果不需要通知则返回 None
    """
    action = report.get("action")

    # 没有动作 = 静默，不发通知
    if action is None:
        return None

    state = report.get("state", "IDLE")
    if state == "PROTECTION":
        return format_protection_message(report)
    else:
        return format_rebalance_message(report)


# ── 发送渠道 ──────────────────────────────────────────


def _post_json(url: str, payload: dict, headers: Optional[dict] = None) -> bool:
    """通用 JSON POST"""
    h = {"Content-Type": "application/json"}
    if headers:
        h.update(headers)
    data = json.dumps(payload).encode("utf-8")
    req = Request(url, data=data, headers=h, method="POST")
    try:
        with urlopen(req, timeout=10) as resp:
            return resp.status == 200
    except (URLError, Exception) as e:
        logger.error(f"发送失败 [{url[:40]}...]: {e}")
        return False


def send_telegram(message: str) -> bool:
    """Telegram Bot 推送"""
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        return False

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    return _post_json(url, {"chat_id": chat_id, "text": message, "parse_mode": "HTML"})


def send_wecom(message: str) -> bool:
    """企业微信机器人推送"""
    webhook = os.environ.get("WECOM_WEBHOOK")
    if not webhook:
        return False

    return _post_json(webhook, {"msgtype": "text", "text": {"content": message}})


def send_feishu(message: str) -> bool:
    """飞书机器人推送"""
    webhook = os.environ.get("FEISHU_WEBHOOK")
    if not webhook:
        return False

    return _post_json(webhook, {"msg_type": "text", "content": {"text": message}})


def send_dingtalk(message: str) -> bool:
    """钉钉机器人推送"""
    webhook = os.environ.get("DINGTALK_WEBHOOK")
    if not webhook:
        return False

    return _post_json(webhook, {"msgtype": "text", "text": {"content": message}})


# ── 统一入口 ──────────────────────────────────────────


CHANNELS = [
    ("Telegram", send_telegram),
    ("企业微信", send_wecom),
    ("飞书", send_feishu),
    ("钉钉", send_dingtalk),
]


def notify(report: dict) -> bool:
    """
    通知入口。根据报告内容决定是否推送。

    静默规则：没有动作（action=None）时不发任何通知。

    Returns:
        True 如果发送了通知（至少一个渠道成功），False 如果静默
    """
    message = format_message(report)

    if message is None:
        logger.debug("无操作，静默")
        return False

    # 控制台始终输出
    print(f"\n{'─'*40}")
    print(message)
    print(f"{'─'*40}\n")

    # 尝试所有已配置的渠道
    sent = False
    for name, sender in CHANNELS:
        try:
            if sender(message):
                logger.info(f"✓ {name} 推送成功")
                sent = True
        except Exception as e:
            logger.error(f"✗ {name} 推送失败: {e}")

    if not sent:
        logger.info("未配置任何推送渠道，仅控制台输出")

    return True
