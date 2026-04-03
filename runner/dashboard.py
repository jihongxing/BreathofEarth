"""
息壤（Xi-Rang）家族看板报告

生成 HTML 格式的月报，可通过邮件推送或 Web 查看。
数据来源：SQLite 快照，不依赖外部图表库（纯 HTML/SVG）。

用法：
    python -m runner.dashboard                     # 生成所有组合报告
    python -m runner.dashboard --portfolio us       # 只生成美股
    python -m runner.dashboard --days 30            # 最近 30 天
    python -m runner.dashboard --push               # 生成并推送通知
    python -m runner.dashboard --lang en            # 英文报告
"""

import json
import sys
import logging
from datetime import datetime, timedelta
from pathlib import Path

from db.database import Database
from engine.config import PORTFOLIOS
from engine.notifier import notify_monthly_report

logger = logging.getLogger("xirang.dashboard")

OUTPUT_DIR = Path("frontend/reports")


# ── i18n 翻译 ─────────────────────────────────────────

REPORT_I18N = {
    "zh": {
        "title": "息壤 · 家族月报",
        "generated_at": "生成时间",
        "state_idle": "正常运行",
        "state_protection": "保护模式",
        "net_asset": "净资产",
        "total_return": "总收益",
        "this_month": "本月",
        "max_drawdown": "最大回撤",
        "current_drawdown": "当前回撤",
        "trading_days": "交易日",
        "nav_trend": "净资产走势",
        "drawdown_monitor": "回撤监控",
        "asset_allocation": "资产配置",
        "risk_events": "风控事件",
        "no_risk_events": "暂无风控事件",
        "rebalance_history": "调仓记录",
        "th_date": "日期",
        "th_type": "类型",
        "th_turnover": "换手率",
        "th_friction": "摩擦成本",
        "th_reason": "原因",
        "no_transactions": "暂无调仓记录",
        "insufficient_data": "数据不足",
        "no_data": "暂无数据",
    },
    "en": {
        "title": "Terragen · Monthly Report",
        "generated_at": "Generated",
        "state_idle": "Running",
        "state_protection": "Protection Mode",
        "net_asset": "Net Asset",
        "total_return": "Total Return",
        "this_month": "This Month",
        "max_drawdown": "Max Drawdown",
        "current_drawdown": "Drawdown",
        "trading_days": "Trading Days",
        "nav_trend": "NAV Trend",
        "drawdown_monitor": "Drawdown Monitor",
        "asset_allocation": "Asset Allocation",
        "risk_events": "Risk Events",
        "no_risk_events": "No risk events",
        "rebalance_history": "Rebalance History",
        "th_date": "Date",
        "th_type": "Type",
        "th_turnover": "Turnover",
        "th_friction": "Friction Cost",
        "th_reason": "Reason",
        "no_transactions": "No rebalance records",
        "insufficient_data": "Insufficient data",
        "no_data": "No data available",
    },
}


def rt(key: str, lang: str = "zh") -> str:
    """Report translation helper."""
    return REPORT_I18N.get(lang, REPORT_I18N["zh"]).get(key, key)


# ── SVG 生成 ──────────────────────────────────────────

def generate_svg_sparkline(values: list[float], width: int = 400, height: int = 80, color: str = "#6366f1", insufficient_label: str = "数据不足") -> str:
    """生成 SVG 折线图"""
    if len(values) < 2:
        return f'<svg width="{width}" height="{height}"><text x="10" y="40" fill="#888">{insufficient_label}</text></svg>'

    min_v, max_v = min(values), max(values)
    span = max_v - min_v or 1
    padding = 4

    points = []
    for i, v in enumerate(values):
        x = padding + i * (width - 2 * padding) / (len(values) - 1)
        y = height - padding - (v - min_v) / span * (height - 2 * padding)
        points.append(f"{x:.1f},{y:.1f}")

    polyline = " ".join(points)
    fill_points = f"{padding},{height - padding} " + polyline + f" {width - padding},{height - padding}"

    return (
        f'<svg width="{width}" height="{height}" xmlns="http://www.w3.org/2000/svg">'
        f'<polygon points="{fill_points}" fill="{color}" fill-opacity="0.1"/>'
        f'<polyline points="{polyline}" fill="none" stroke="{color}" stroke-width="2"/>'
        f'</svg>'
    )


def generate_svg_pie(weights: dict, size: int = 160) -> str:
    """生成 SVG 饼图"""
    colors = ["#6366f1", "#22c55e", "#eab308", "#8b8d97", "#ef4444", "#06b6d4"]
    cx, cy, r = size // 2, size // 2, size // 2 - 8

    paths = []
    legends = []
    start_angle = 0
    import math

    items = list(weights.items())
    for i, (name, pct) in enumerate(items):
        if pct <= 0:
            continue
        angle = pct * 360
        end_angle = start_angle + angle

        large_arc = 1 if angle > 180 else 0
        x1 = cx + r * math.cos(math.radians(start_angle - 90))
        y1 = cy + r * math.sin(math.radians(start_angle - 90))
        x2 = cx + r * math.cos(math.radians(end_angle - 90))
        y2 = cy + r * math.sin(math.radians(end_angle - 90))

        color = colors[i % len(colors)]
        if abs(pct - 1.0) < 0.001:
            paths.append(f'<circle cx="{cx}" cy="{cy}" r="{r}" fill="{color}"/>')
        else:
            paths.append(
                f'<path d="M{cx},{cy} L{x1:.1f},{y1:.1f} A{r},{r} 0 {large_arc},1 {x2:.1f},{y2:.1f} Z" '
                f'fill="{color}"/>'
            )
        legends.append(f'<span style="color:{color}">●</span> {name} {pct:.1%}')
        start_angle = end_angle

    svg = f'<svg width="{size}" height="{size}" xmlns="http://www.w3.org/2000/svg">{"".join(paths)}</svg>'
    legend_html = '<div style="font-size:12px;margin-top:8px">' + "<br>".join(legends) + "</div>"
    return svg + legend_html


# ── 组合报告数据生成 ──────────────────────────────────

def generate_portfolio_report(db: Database, portfolio_id: str, days: int = 90) -> dict:
    """生成单个组合的报告数据"""
    pf_config = PORTFOLIOS.get(portfolio_id)
    if not pf_config:
        return {}

    assets = pf_config["assets"]
    asset_names = pf_config["asset_names"]
    currency = pf_config["currency"]
    name = pf_config["name"]

    snapshots = db.get_snapshots(portfolio_id, limit=days)
    snapshots.reverse()  # 按时间正序

    if not snapshots:
        return {"name": name, "currency": currency, "error": True}

    first, last = snapshots[0], snapshots[-1]
    total_return = (last["nav"] / first["nav"] - 1) if first["nav"] > 0 else 0

    # 最近 30 天收益（月度）
    recent_30 = snapshots[-30:] if len(snapshots) >= 30 else snapshots
    monthly_return = (recent_30[-1]["nav"] / recent_30[0]["nav"] - 1) if recent_30[0]["nav"] > 0 else 0

    # 最大回撤
    hwm = 0
    max_dd = 0
    for s in snapshots:
        hwm = max(hwm, s["nav"])
        dd = (s["nav"] - hwm) / hwm if hwm > 0 else 0
        max_dd = min(max_dd, dd)

    # 当前权重
    last_weights = json.loads(last["weights"]) if isinstance(last["weights"], str) else last["weights"]
    weight_map = {}
    for i in range(min(len(assets), len(last_weights))):
        weight_map[asset_names.get(assets[i], assets[i])] = last_weights[i]

    # NAV 序列
    nav_series = [s["nav"] for s in snapshots]
    dd_series = [s["drawdown"] for s in snapshots]
    date_series = [s["date"] for s in snapshots]

    # 交易记录
    with db._conn() as conn:
        tx_rows = conn.execute(
            "SELECT date, type, turnover, friction_cost, reason FROM transactions WHERE portfolio_id = ? ORDER BY date DESC LIMIT 10",
            (portfolio_id,),
        ).fetchall()
    transactions = [dict(r) for r in tx_rows]

    # 风控事件
    with db._conn() as conn:
        risk_rows = conn.execute(
            "SELECT date, event_type, severity FROM risk_events WHERE portfolio_id = ? ORDER BY date DESC LIMIT 5",
            (portfolio_id,),
        ).fetchall()
    risk_events = [dict(r) for r in risk_rows]

    return {
        "portfolio_id": portfolio_id,
        "name": name,
        "currency": currency,
        "state": last["state"],
        "nav": last["nav"],
        "total_return": total_return,
        "monthly_return": monthly_return,
        "max_drawdown": max_dd,
        "drawdown": last["drawdown"],
        "weights": weight_map,
        "nav_series": nav_series,
        "dd_series": dd_series,
        "date_series": date_series,
        "transactions": transactions,
        "risk_events": risk_events,
        "period": f"{first['date']} ~ {last['date']}",
        "trading_days": len(snapshots),
    }


# ── HTML 渲染 ─────────────────────────────────────────

def render_html_report(reports: list[dict], lang: str = "zh") -> str:
    """渲染 HTML 月报，支持中英双语"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    html_lang = "zh-CN" if lang == "zh" else "en"

    sections = []
    for r in reports:
        if r.get("error"):
            sections.append(f'<div class="card"><h2>{r["name"]}</h2><p>{rt("no_data", lang)}</p></div>')
            continue

        insuf = rt("insufficient_data", lang)
        nav_svg = generate_svg_sparkline(r["nav_series"], width=480, height=100, color="#6366f1", insufficient_label=insuf)
        dd_svg = generate_svg_sparkline(r["dd_series"], width=480, height=60, color="#ef4444", insufficient_label=insuf)
        pie_svg = generate_svg_pie(r["weights"])

        # 状态徽章
        state_class = "green" if r["state"] == "IDLE" else "red"
        state_label = rt("state_idle", lang) if r["state"] == "IDLE" else rt("state_protection", lang)

        # 交易记录
        tx_rows = ""
        for t in r.get("transactions", []):
            turnover = f'{t["turnover"] * 100:.2f}%' if t.get("turnover") else "-"
            cost = f'${t["friction_cost"]:.2f}' if t.get("friction_cost") else "-"
            tx_rows += f'<tr><td>{t["date"]}</td><td>{t["type"]}</td><td>{turnover}</td><td>{cost}</td><td>{t.get("reason", "")}</td></tr>'

        # 风控事件
        risk_items = ""
        for e in r.get("risk_events", []):
            sev_color = "#ef4444" if e["severity"] == "HIGH" else "#eab308" if e["severity"] == "MEDIUM" else "#8b8d97"
            risk_items += f'<div class="event"><span style="color:{sev_color}">●</span> {e["date"]} — {e["event_type"]}</div>'
        if not risk_items:
            risk_items = f'<div class="event" style="color:#8b8d97">{rt("no_risk_events", lang)}</div>'

        no_tx = f'<tr><td colspan="5" style="color:#8b8d97;text-align:center">{rt("no_transactions", lang)}</td></tr>'

        sections.append(f"""
        <div class="portfolio-section">
          <h2>{r["name"]} <span class="badge {state_class}">{state_label}</span></h2>
          <div class="stats">
            <div class="stat"><div class="label">{rt("net_asset", lang)}</div><div class="value">{r["currency"]}{r["nav"]:,.2f}</div></div>
            <div class="stat"><div class="label">{rt("total_return", lang)}</div><div class="value {'red' if r['total_return'] < 0 else ''}">{r["total_return"]:+.2%}</div></div>
            <div class="stat"><div class="label">{rt("this_month", lang)}</div><div class="value {'red' if r['monthly_return'] < 0 else ''}">{r["monthly_return"]:+.2%}</div></div>
            <div class="stat"><div class="label">{rt("max_drawdown", lang)}</div><div class="value red">{r["max_drawdown"]:.2%}</div></div>
            <div class="stat"><div class="label">{rt("current_drawdown", lang)}</div><div class="value">{r["drawdown"]:.2%}</div></div>
            <div class="stat"><div class="label">{rt("trading_days", lang)}</div><div class="value">{r["trading_days"]}</div></div>
          </div>
          <div class="charts">
            <div class="chart-main">
              <h3>{rt("nav_trend", lang)}</h3>
              {nav_svg}
              <h3 style="margin-top:16px">{rt("drawdown_monitor", lang)}</h3>
              {dd_svg}
            </div>
            <div class="chart-side">
              <h3>{rt("asset_allocation", lang)}</h3>
              {pie_svg}
              <h3 style="margin-top:16px">{rt("risk_events", lang)}</h3>
              {risk_items}
            </div>
          </div>
          <h3>{rt("rebalance_history", lang)}</h3>
          <table>
            <thead><tr><th>{rt("th_date", lang)}</th><th>{rt("th_type", lang)}</th><th>{rt("th_turnover", lang)}</th><th>{rt("th_friction", lang)}</th><th>{rt("th_reason", lang)}</th></tr></thead>
            <tbody>{tx_rows if tx_rows else no_tx}</tbody>
          </table>
        </div>
        """)

    return f"""<!DOCTYPE html>
<html lang="{html_lang}">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{rt("title", lang)}</title>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #0f1117; color: #e4e4e7; margin: 0; padding: 20px; }}
  .header {{ text-align: center; padding: 20px 0; }}
  .header h1 {{ font-size: 1.8rem; margin-bottom: 4px; }}
  .header .date {{ color: #8b8d97; font-size: 0.85rem; }}
  .portfolio-section {{ background: #1a1d27; border-radius: 12px; padding: 24px; margin-bottom: 24px; border: 1px solid #2a2d3a; }}
  .portfolio-section h2 {{ font-size: 1.2rem; margin-bottom: 16px; }}
  .portfolio-section h3 {{ font-size: 0.9rem; color: #8b8d97; margin-bottom: 8px; }}
  .badge {{ font-size: 0.7rem; padding: 2px 8px; border-radius: 4px; font-weight: 600; }}
  .badge.green {{ background: rgba(34,197,94,0.15); color: #22c55e; }}
  .badge.red {{ background: rgba(239,68,68,0.15); color: #ef4444; }}
  .stats {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(120px, 1fr)); gap: 12px; margin-bottom: 20px; }}
  .stat {{ background: #0f1117; padding: 12px; border-radius: 8px; }}
  .stat .label {{ color: #8b8d97; font-size: 0.75rem; margin-bottom: 4px; }}
  .stat .value {{ font-size: 1.2rem; font-weight: 700; }}
  .stat .value.red {{ color: #ef4444; }}
  .charts {{ display: grid; grid-template-columns: 2fr 1fr; gap: 16px; margin-bottom: 20px; }}
  .event {{ padding: 4px 0; font-size: 0.85rem; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 0.85rem; }}
  th, td {{ padding: 8px 10px; text-align: left; border-bottom: 1px solid #2a2d3a; }}
  th {{ color: #8b8d97; font-weight: 600; }}
  @media (max-width: 600px) {{
    .charts {{ grid-template-columns: 1fr; }}
    .stats {{ grid-template-columns: repeat(2, 1fr); }}
  }}
</style>
</head>
<body>
  <div class="header">
    <h1>{rt("title", lang)}</h1>
    <div class="date">{rt("generated_at", lang)}: {now}</div>
  </div>
  {"".join(sections)}
</body>
</html>"""


# ── 文件命名规范 ──────────────────────────────────────
# 新版：report_{YYYYMM}_{portfolio}_{lang}.html  （按组合独立存储）
# 旧版兼容：monthly_report_{YYYYMMDD}_{lang}.html

import re

_REPORT_PATTERN = re.compile(r"report_(\d{6})_(\w+)_(\w+)\.html")


def _report_path(year: int, month: int, portfolio_id: str, lang: str) -> Path:
    """生成月报文件路径。"""
    return OUTPUT_DIR / f"report_{year:04d}{month:02d}_{portfolio_id}_{lang}.html"


# ── 生成并保存（按组合独立）────────────────────────────

def generate_and_save(days: int = 90, portfolio_id: str = None, push: bool = False, lang: str = "zh") -> Path:
    """生成报告并保存为 HTML 文件。每个组合独立保存。"""
    db = Database()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    portfolios = {portfolio_id: PORTFOLIOS[portfolio_id]} if portfolio_id else PORTFOLIOS
    now = datetime.now()
    year, month = now.year, now.month
    saved_paths = []

    for pid in portfolios:
        report = generate_portfolio_report(db, pid, days=days)
        if not report or report.get("error"):
            continue

        html = render_html_report([report], lang=lang)

        # 按组合独立保存
        filepath = _report_path(year, month, pid, lang)
        filepath.write_text(html, encoding="utf-8")
        saved_paths.append(filepath)
        logger.info(f"✓ 月报已生成: {filepath}")

    # 同时生成合并版 latest（向后兼容旧 API）
    all_reports = []
    for pid in portfolios:
        r = generate_portfolio_report(db, pid, days=days)
        if r and not r.get("error"):
            all_reports.append(r)
    if all_reports:
        combined = render_html_report(all_reports, lang=lang)
        latest = OUTPUT_DIR / f"latest_{lang}.html"
        latest.write_text(combined, encoding="utf-8")
        today_str = now.strftime("%Y%m%d")
        (OUTPUT_DIR / f"monthly_report_{today_str}_{lang}.html").write_text(combined, encoding="utf-8")

    # 推送通知
    if push and all_reports:
        summary = {
            "period": all_reports[0].get("period", ""),
            "portfolios": [
                {
                    "name": r["name"],
                    "currency": r["currency"],
                    "nav": r["nav"],
                    "monthly_return": r["monthly_return"],
                    "drawdown": r["drawdown"],
                }
                for r in all_reports
            ],
        }
        notify_monthly_report(summary)
        logger.info("✓ 月报通知已推送")

    return saved_paths[0] if saved_paths else None


# ── 月报列表（供 API 使用）────────────────────────────

def list_reports() -> list[dict]:
    """
    扫描 reports 目录，返回按年月聚合的月报列表。

    返回格式：
    [
      {
        "year": 2026, "month": 4,
        "portfolios": {
          "us": {"zh": true, "en": true},
          "cn": {"zh": true, "en": false}
        }
      },
      ...
    ]
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    index = {}  # key: (year, month)

    for f in OUTPUT_DIR.glob("report_*.html"):
        m = _REPORT_PATTERN.match(f.name)
        if not m:
            continue
        ym = m.group(1)       # "202604"
        pid = m.group(2)      # "us"
        lang = m.group(3)     # "zh"
        year = int(ym[:4])
        month = int(ym[4:6])
        key = (year, month)

        if key not in index:
            index[key] = {}
        if pid not in index[key]:
            index[key][pid] = {}
        index[key][pid][lang] = True

    # 也扫描旧版文件以便兼容
    for f in OUTPUT_DIR.glob("monthly_report_*.html"):
        old_m = re.match(r"monthly_report_(\d{8})_(\w+)\.html", f.name)
        if not old_m:
            continue
        date_str = old_m.group(1)  # "20260403"
        lang = old_m.group(2)
        year = int(date_str[:4])
        month = int(date_str[4:6])
        key = (year, month)
        if key not in index:
            index[key] = {}
        # 旧版是合并的，标记为 "all"
        if "_all" not in index[key]:
            index[key]["_all"] = {}
        index[key]["_all"][lang] = True

    # 排序输出（最新在前）
    result = []
    for (year, month) in sorted(index.keys(), reverse=True):
        result.append({
            "year": year,
            "month": month,
            "portfolios": index[(year, month)],
        })
    return result


def get_report_html(year: int, month: int, portfolio_id: str, lang: str = "zh") -> str:
    """获取指定月份、指定组合的月报 HTML。"""
    # 优先读新版独立文件
    filepath = _report_path(year, month, portfolio_id, lang)
    if filepath.exists():
        return filepath.read_text(encoding="utf-8")

    # 没有缓存，实时生成
    db = Database()
    report = generate_portfolio_report(db, portfolio_id, days=90)
    if not report or report.get("error"):
        no_data = rt("no_data", lang)
        return f"<html><body style='background:#0f1117;color:#888;padding:40px;font-family:sans-serif'><h2>{no_data}</h2></body></html>"

    html = render_html_report([report], lang=lang)

    # 缓存
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    filepath.write_text(html, encoding="utf-8")
    return html


def get_latest_report_html(lang: str = "zh") -> str:
    """获取最新报告的 HTML（向后兼容旧 API）"""
    latest = OUTPUT_DIR / f"latest_{lang}.html"
    if latest.exists():
        return latest.read_text(encoding="utf-8")
    # 没有缓存，实时生成
    db = Database()
    reports = [generate_portfolio_report(db, pid) for pid in PORTFOLIOS]
    reports = [r for r in reports if r]
    if not reports:
        no_data = rt("no_data", lang)
        return f"<html><body><h1>{no_data}</h1></body></html>"
    return render_html_report(reports, lang=lang)


# ── CLI 入口 ──────────────────────────────────────────

if __name__ == "__main__":
    days = 90
    only = None
    lang = "zh"
    push = "--push" in sys.argv

    if "--days" in sys.argv:
        idx = sys.argv.index("--days")
        if idx + 1 < len(sys.argv):
            days = int(sys.argv[idx + 1])

    if "--portfolio" in sys.argv:
        idx = sys.argv.index("--portfolio")
        if idx + 1 < len(sys.argv):
            only = sys.argv[idx + 1]

    if "--lang" in sys.argv:
        idx = sys.argv.index("--lang")
        if idx + 1 < len(sys.argv):
            lang = sys.argv[idx + 1]

    path = generate_and_save(days=days, portfolio_id=only, push=push, lang=lang)
    if path:
        print(f"报告已生成: {path}")
