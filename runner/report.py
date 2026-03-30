"""
息壤（Xi-Rang）汇总报告 V2.0

"放上去不管，过几个月回来一眼看透"的终极看板。

维度：
1. 核心指标（NAV、CAGR、MDD、水下时间）
2. ASCII 净值走势图
3. 基准对比（SPY、60/40、通胀）
4. 收益归因（四大资产各自贡献）
5. 持仓权重 + 偏离度预警
6. 月度收益
7. 风控与运行统计
8. 系统健康评分

用法：
    python -m runner.report              # 全部历史
    python -m runner.report --days 30    # 最近 30 天
    python -m runner.report --days 90    # 最近 90 天
"""

import json
import sys
import math
from datetime import datetime, timedelta
from db.database import Database
from engine.config import ASSETS


# ── ASCII 净值走势图 ──────────────────────────────────

def ascii_sparkline(values: list[float], width: int = 56, height: int = 8) -> list[str]:
    """
    生成 ASCII 净值走势图。

    Returns:
        多行字符串列表
    """
    if len(values) < 2:
        return ["  (数据不足，无法绘图)"]

    # 采样到 width 个点
    if len(values) > width:
        step = len(values) / width
        sampled = [values[int(i * step)] for i in range(width)]
    else:
        sampled = values
        width = len(sampled)

    min_v = min(sampled)
    max_v = max(sampled)
    span = max_v - min_v
    if span == 0:
        span = 1

    # 构建网格
    grid = [[" "] * width for _ in range(height)]
    for x, v in enumerate(sampled):
        y = int((v - min_v) / span * (height - 1))
        y = min(y, height - 1)
        grid[y][x] = "█"

    # 连线：填充相邻列之间的空隙
    for x in range(1, len(sampled)):
        y_prev = int((sampled[x - 1] - min_v) / span * (height - 1))
        y_curr = int((sampled[x] - min_v) / span * (height - 1))
        y_lo, y_hi = min(y_prev, y_curr), max(y_prev, y_curr)
        for y in range(y_lo, y_hi + 1):
            if grid[y][x] == " ":
                grid[y][x] = "▪"

    # 翻转（高值在上）+ 加 Y 轴标签
    lines = []
    for row_idx in range(height - 1, -1, -1):
        val = min_v + (row_idx / (height - 1)) * span
        label = f"  ${val/1000:>6.1f}k │"
        lines.append(label + "".join(grid[row_idx]))

    # X 轴
    lines.append(" " * 10 + "└" + "─" * width)

    return lines


# ── 水下时间计算 ──────────────────────────────────────

def calc_underwater(navs: list[float]) -> tuple[int, int]:
    """
    计算最长水下时间和当前水下时间。

    Returns:
        (max_underwater_days, current_underwater_days)
    """
    hwm = 0
    max_uw = 0
    current_uw = 0

    for nav in navs:
        if nav >= hwm:
            hwm = nav
            current_uw = 0
        else:
            current_uw += 1
        max_uw = max(max_uw, current_uw)

    return max_uw, current_uw


# ── 主报告 ────────────────────────────────────────────

def generate_report(days: int = 0):
    db = Database()

    # 拉取快照
    with db._conn() as conn:
        if days > 0:
            since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
            rows = conn.execute(
                "SELECT * FROM daily_snapshots WHERE date >= ? ORDER BY date ASC", (since,)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM daily_snapshots ORDER BY date ASC"
            ).fetchall()

    if not rows:
        print("暂无运行数据。")
        return

    snapshots = [dict(r) for r in rows]
    first = snapshots[0]
    last = snapshots[-1]

    first_nav = first["nav"]
    last_nav = last["nav"]
    total_return = last_nav / first_nav - 1
    total_days = len(snapshots)

    date_span = (datetime.strptime(last["date"], "%Y-%m-%d") - datetime.strptime(first["date"], "%Y-%m-%d")).days
    years = date_span / 365.25 if date_span > 0 else 0
    cagr = (last_nav / first_nav) ** (1 / years) - 1 if years > 0.1 else total_return

    # 最大回撤
    max_nav_val = 0
    max_dd = 0
    max_dd_date = first["date"]
    for s in snapshots:
        if s["nav"] > max_nav_val:
            max_nav_val = s["nav"]
        dd = (s["nav"] - max_nav_val) / max_nav_val
        if dd < max_dd:
            max_dd = dd
            max_dd_date = s["date"]

    # 水下时间
    navs = [s["nav"] for s in snapshots]
    max_uw, current_uw = calc_underwater(navs)

    # ── 头部 ──────────────────────────────────────────
    period = f"最近 {days} 天" if days > 0 else "全部历史"
    state_label = "IDLE (正常)" if last["state"] == "IDLE" else "PROTECTION (保护中) ⚠"

    print()
    print("=" * 68)
    print("  [息壤 Xi-Rang] 仿真运行报告 (Paper Trading Report)")
    print("=" * 68)
    print(f"  时间范围:       {first['date']} ~ {last['date']} ({total_days} 个交易日)")
    print(f"  起始 / 当前:    ${first_nav:,.2f}  ➔  ${last_nav:,.2f}")
    print(f"  总收益率:       {total_return:+.2%}")
    if years > 0.1:
        print(f"  年化收益(CAGR): {cagr:.2%}")
    print(f"  最大回撤(MDD):  {max_dd:.2%} (发生于 {max_dd_date})")
    print(f"  最长水下时间:   {max_uw} 天 (Max Underwater Period)")
    if current_uw > 0:
        print(f"  当前水下时间:   {current_uw} 天 ⚠")
    print(f"  当前状态:       {state_label} | 当前回撤: {last['drawdown']:.2%}")
    print("-" * 68)

    # ── ASCII 净值走势 ────────────────────────────────
    print(f"\n  ── 📊 净值走势 (NAV Trend) ──")
    sparkline = ascii_sparkline(navs)
    for line in sparkline:
        print(f"  {line}")

    # ── 基准对比 ──────────────────────────────────────
    print(f"\n  ── ⚔️ 基准对比 (Benchmarks) ──")

    # 从快照中提取各资产的收益（用持仓变化估算）
    first_positions = json.loads(first["positions"]) if isinstance(first["positions"], str) else first["positions"]
    last_positions = json.loads(last["positions"]) if isinstance(last["positions"], str) else last["positions"]

    # SPY 基准：如果全仓 SPY，收益是多少
    if first_positions[0] > 0:
        spy_total_return = (last_positions[0] / first_positions[0] - 1) if first_positions[0] > 0 else 0
    else:
        spy_total_return = 0

    # 60/40 估算：60% SPY + 40% TLT
    if first_positions[0] > 0 and first_positions[1] > 0:
        spy_growth = last_positions[0] / first_positions[0] if first_positions[0] > 0 else 1
        tlt_growth = last_positions[1] / first_positions[1] if first_positions[1] > 0 else 1
        benchmark_6040 = 0.6 * spy_growth + 0.4 * tlt_growth - 1
    else:
        benchmark_6040 = 0

    # 通胀目标（年化 3% 按天数折算）
    inflation_target = (1.03 ** (date_span / 365.25) - 1) if date_span > 0 else 0

    def bar(ret, scale=200):
        n = int(abs(ret) * scale)
        return "█" * max(n, 1)

    xirang_label = "👑 息壤 (Xi-Rang)"
    spy_label = "🇺🇸 标普500 (SPY)"
    b6040_label = "⚖️  股债 60/40"
    cpi_label = "🔥 通胀目标 3%"

    print(f"  [{xirang_label:<18}] : {total_return:+.2%}  {bar(total_return)}")
    print(f"  [{spy_label:<18}] : {spy_total_return:+.2%}  {bar(spy_total_return)}")
    print(f"  [{b6040_label:<18}] : {benchmark_6040:+.2%}  {bar(benchmark_6040)}")
    print(f"  [{cpi_label:<18}] : {inflation_target:+.2%}  {bar(inflation_target)}")

    # 结论
    beat_spy = total_return > spy_total_return
    beat_6040 = total_return > benchmark_6040
    beat_cpi = total_return > inflation_target
    conclusions = []
    if beat_cpi:
        conclusions.append("跑赢通胀 ✓")
    if beat_6040:
        conclusions.append("跑赢 60/40 ✓")
    if beat_spy:
        conclusions.append("跑赢 SPY ✓")
    else:
        conclusions.append("温和跑输 SPY（牛市正常）")
    print(f"  > 结论: {', '.join(conclusions)}")

    # ── 收益归因 ──────────────────────────────────────
    print(f"\n  ── 🏆 收益归因 (Attribution) ──")
    total_pnl = last_nav - first_nav
    print(f"  总 PnL: ${total_pnl:+,.2f}")

    asset_pnl = []
    for i, asset in enumerate(ASSETS):
        pnl = last_positions[i] - first_positions[i]
        asset_pnl.append((asset, pnl))

    # 按贡献排序
    asset_pnl.sort(key=lambda x: x[1], reverse=True)
    for asset, pnl in asset_pnl:
        pct = pnl / first_nav * 100
        tag = ""
        if pnl == max(p for _, p in asset_pnl):
            tag = "  (MVP 🌟)"
        elif pnl == min(p for _, p in asset_pnl):
            tag = "  (主要拖累)" if pnl < 0 else ""
        sign = "+" if pnl >= 0 else ""
        print(f"    {asset}: {sign}${pnl:,.2f}  ({sign}{pct:.2f}%){tag}")

    # ── 持仓与偏离度预警 ──────────────────────────────
    print(f"\n  ── ⚖️ 持仓与偏离度预警 (Weights & Triggers) ──")
    current_weights = json.loads(last["weights"]) if isinstance(last["weights"], str) else last["weights"]
    if isinstance(current_weights, list):
        weight_list = current_weights
    else:
        weight_list = list(current_weights.values())

    target = 0.25
    threshold = 0.05
    nearest_trigger = float("inf")

    for i, asset in enumerate(ASSETS):
        w = weight_list[i]
        drift = abs(w - target)
        distance = threshold - drift
        nearest_trigger = min(nearest_trigger, distance)
        bar_str = "█" * int(w * 50)
        status = f"距阈值 {distance:.1%}" if distance > 0 else "⚠ 已超阈值!"
        print(f"    {asset}: {w:>6.1%}  {bar_str:<14} [{status}]")

    if nearest_trigger > 0.02:
        print(f"  > 预测: 近期无即将触发的常规调仓。")
    elif nearest_trigger > 0:
        print(f"  > 预测: 接近调仓阈值，关注 1-2 周内可能触发。")
    else:
        print(f"  > 预测: 已超阈值，下次运行将触发调仓。")

    # ── 月度收益 ──────────────────────────────────────
    print(f"\n  ── 📅 月度收益 ──")
    monthly = {}
    for s in snapshots:
        month_key = s["date"][:7]
        if month_key not in monthly:
            monthly[month_key] = {"first": s["nav"], "last": s["nav"]}
        monthly[month_key]["last"] = s["nav"]

    prev_nav = first_nav
    for month, vals in monthly.items():
        m_ret = vals["last"] / prev_nav - 1
        bar_len = int(abs(m_ret) * 400)
        if m_ret >= 0:
            bar_str = "█" * max(bar_len, 1)
            print(f"    {month}:  {m_ret:+.2%}  {bar_str}")
        else:
            bar_str = "░" * max(bar_len, 1)
            print(f"    {month}:  {m_ret:+.2%}  {bar_str}")
        prev_nav = vals["last"]

    # ── 风控与运行统计 ────────────────────────────────
    print(f"\n  ── 🚨 风控与运行统计 ──")

    with db._conn() as conn:
        if days > 0:
            run_rows = conn.execute(
                "SELECT status, COUNT(*) as cnt FROM daily_runs WHERE date >= ? GROUP BY status", (since,)
            ).fetchall()
            tx_rows = conn.execute(
                "SELECT * FROM transactions WHERE date >= ? ORDER BY date ASC", (since,)
            ).fetchall()
            risk_rows = conn.execute(
                "SELECT * FROM risk_events WHERE date >= ? ORDER BY date ASC", (since,)
            ).fetchall()
        else:
            run_rows = conn.execute(
                "SELECT status, COUNT(*) as cnt FROM daily_runs GROUP BY status"
            ).fetchall()
            tx_rows = conn.execute(
                "SELECT * FROM transactions ORDER BY date ASC"
            ).fetchall()
            risk_rows = conn.execute(
                "SELECT * FROM risk_events ORDER BY date ASC"
            ).fetchall()

    run_stats = {r["status"]: r["cnt"] for r in run_rows}
    transactions = [dict(r) for r in tx_rows]
    risk_events = [dict(r) for r in risk_rows]
    protection_days = sum(1 for s in snapshots if s["state"] == "PROTECTION")
    total_friction = sum(t.get("friction_cost", 0) or 0 for t in transactions)

    total_runs = sum(run_stats.values())
    success_rate = run_stats.get("SUCCESS", 0) / max(total_runs, 1) * 100

    print(f"  运行成功率:     {success_rate:.1f}% ({run_stats.get('SUCCESS', 0)}/{total_runs})")
    if run_stats.get("FAILED", 0) > 0:
        print(f"  ⚠ 失败次数:     {run_stats['FAILED']} 次")
    print(f"  调仓次数:       {len(transactions)} 次")
    print(f"  调仓摩擦成本:   ${total_friction:,.2f}")
    if total_pnl != 0:
        friction_pct = total_friction / abs(total_pnl) * 100
        print(f"  摩擦占利润比:   {friction_pct:.1f}%")
    print(f"  风控事件总数:   {len(risk_events)} 次")
    print(f"  保护模式天数:   {protection_days} 天 ({protection_days / max(total_days, 1) * 100:.1f}%)")

    if risk_events:
        print(f"\n  [风控事件日志]")
        for e in risk_events:
            icon = "🔴" if e["severity"] == "HIGH" else "🟡" if e["severity"] == "MEDIUM" else "🟢"
            print(f"    {icon} {e['date']} | {e['event_type']} | 回撤: {e['drawdown']:.2%} | corr: {e['spy_tlt_corr']:.2f}")

    # 最近操作
    recent_actions = [s for s in snapshots[-30:] if s["action"]]
    if recent_actions:
        print(f"\n  [最近操作记录]")
        for s in recent_actions[-10:]:
            icon = "🟡" if "保护" in (s["action"] or "") else "🟢" if "解除" in (s["action"] or "") else "📊"
            print(f"    {icon} {s['date']} | NAV ${s['nav']:,.2f} | {s['action']}")

    # ── 系统健康评分 ──────────────────────────────────
    print(f"\n  ── 🩺 系统综合评估 ──")
    score = 100
    issues = []

    if run_stats.get("FAILED", 0) > 0:
        penalty = min(10 * run_stats["FAILED"], 30)
        score -= penalty
        issues.append(f"有 {run_stats['FAILED']} 次运行失败 (-{penalty})")

    if max_dd < -0.15:
        score -= 20
        issues.append(f"最大回撤 {max_dd:.2%} 超过 -15% 红线 (-20)")

    if protection_days / max(total_days, 1) > 0.2:
        score -= 10
        issues.append(f"保护模式占比过高 {protection_days / total_days * 100:.1f}% (-10)")

    if len(transactions) > total_days * 0.1:
        score -= 10
        issues.append(f"调仓过于频繁 {len(transactions)} 次 / {total_days} 天 (-10)")

    if total_pnl != 0 and total_friction / abs(total_pnl) > 0.1:
        score -= 5
        issues.append(f"摩擦成本占利润 {total_friction / abs(total_pnl) * 100:.1f}% 偏高 (-5)")

    if not beat_cpi and years > 0.5:
        score -= 10
        issues.append(f"未跑赢通胀目标 (-10)")

    score = max(score, 0)
    if score >= 90:
        grade = "A  优秀 ✓"
    elif score >= 75:
        grade = "B  良好"
    elif score >= 60:
        grade = "C  及格"
    else:
        grade = "D  需要关注 ⚠"

    print(f"  评分: {score}/100  ({grade})")
    if issues:
        for issue in issues:
            print(f"    ⚠ {issue}")
    else:
        print(f"    系统运行正常，无异常。")

    print()
    print("=" * 68)
    print()


if __name__ == "__main__":
    days = 0
    if "--days" in sys.argv:
        idx = sys.argv.index("--days")
        if idx + 1 < len(sys.argv):
            days = int(sys.argv[idx + 1])
    generate_report(days)
