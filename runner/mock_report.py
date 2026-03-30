"""
模拟 3 个月后的报告效果。
用假数据填充临时数据库，然后调用真实的 report V2.0 生成器。
"""

import json
import random
from datetime import datetime, timedelta
from pathlib import Path
from db.database import Database

MOCK_DB_PATH = Path("db/mock_xirang.db")


def generate_mock_data():
    if MOCK_DB_PATH.exists():
        MOCK_DB_PATH.unlink()

    db = Database(db_path=MOCK_DB_PATH)

    nav = 100000.0
    positions = [25000.0, 25000.0, 25000.0, 25000.0]
    state = "IDLE"
    hwm = nav
    cooldown = 0
    rebalance_count = 0
    protection_count = 0

    start = datetime(2026, 4, 1)
    trading_days = []
    current = start
    while current <= datetime(2026, 6, 30):
        if current.weekday() < 5:
            trading_days.append(current)
        current += timedelta(days=1)

    random.seed(42)
    events = []

    for i, day in enumerate(trading_days):
        date_str = day.strftime("%Y-%m-%d")

        # 模拟行情
        if i < 30:
            # 4月：温和上涨，SPY 领涨
            spy_ret = random.gauss(0.0008, 0.008)
            tlt_ret = random.gauss(-0.0002, 0.005)
            gld_ret = random.gauss(0.0004, 0.006)
            shv_ret = random.gauss(0.00015, 0.0003)
            corr = random.uniform(-0.3, 0.3)
        elif 30 <= i < 40:
            # 5月中旬：股债双杀
            spy_ret = random.gauss(-0.011, 0.014)
            tlt_ret = random.gauss(-0.006, 0.008)
            gld_ret = random.gauss(0.004, 0.007)
            shv_ret = random.gauss(0.00015, 0.0003)
            corr = random.uniform(0.45, 0.72)
        elif 40 <= i < 48:
            # 5月底：缓慢企稳
            spy_ret = random.gauss(0.001, 0.01)
            tlt_ret = random.gauss(0.0005, 0.005)
            gld_ret = random.gauss(0.001, 0.005)
            shv_ret = random.gauss(0.00015, 0.0003)
            corr = random.uniform(0.1, 0.4)
        else:
            # 6月：强劲反弹
            spy_ret = random.gauss(0.004, 0.009)
            tlt_ret = random.gauss(0.001, 0.004)
            gld_ret = random.gauss(0.0015, 0.005)
            shv_ret = random.gauss(0.00015, 0.0003)
            corr = random.uniform(-0.2, 0.25)

        rets = [spy_ret, tlt_ret, gld_ret, shv_ret]
        positions = [p * (1 + r) for p, r in zip(positions, rets)]
        nav = sum(positions)
        weights = [p / nav for p in positions]

        if nav > hwm:
            hwm = nav
        dd = (nav - hwm) / hwm

        action = None
        trigger = None

        if state == "IDLE":
            if dd <= -0.14:
                state = "PROTECTION"
                cooldown = 40
                protection_count += 1
                rebalance_count += 1
                action = "紧急避险: 硬止损触发"
                trigger = f"硬止损: 回撤 {dd:.2%}"
                nav *= 0.9998  # 摩擦
                positions = [nav * w for w in [0.03, 0.07, 0.15, 0.75]]
                events.append((date_str, "HIGH", action, dd, corr))
            elif dd <= -0.12 or (corr > 0.5 and spy_ret < 0 and tlt_ret < 0):
                state = "PROTECTION"
                cooldown = 20
                protection_count += 1
                rebalance_count += 1
                reason = "相关性崩溃" if corr > 0.5 else "回撤预警"
                action = f"常规保护: {reason}"
                trigger = f"{reason}: corr={corr:.2f}, dd={dd:.2%}"
                nav *= 0.9997
                positions = [nav * w for w in [0.10, 0.20, 0.20, 0.50]]
                events.append((date_str, "MEDIUM", action, dd, corr))
            else:
                max_drift = max(abs(w - 0.25) for w in weights)
                if max_drift > 0.05:
                    rebalance_count += 1
                    action = f"阈值再平衡: 偏离 {max_drift:.2%}"
                    nav *= 0.9999
                    positions = [nav * 0.25 for _ in range(4)]
        elif state == "PROTECTION":
            if cooldown > 0:
                cooldown -= 1
            risk_active = dd <= -0.12 or (corr > 0.5 and spy_ret < 0 and tlt_ret < 0)
            if not risk_active and cooldown == 0:
                state = "IDLE"
                rebalance_count += 1
                action = "解除保护: 风控恢复正常"
                nav *= 0.9997
                positions = [nav * 0.25 for _ in range(4)]

        weights = [p / nav for p in positions]

        db.save_snapshot(
            date=date_str, state=state, nav=nav,
            positions=positions, weights=weights,
            drawdown=dd, spy_tlt_corr=corr,
            action=action, trigger_reason=trigger,
        )

        if action:
            turnover = random.uniform(0.03, 0.12)
            db.save_transaction(
                date=date_str, tx_type="REBALANCE",
                target_weights=weights,
                turnover=turnover,
                friction_cost=nav * turnover * 0.001,
                reason=action,
            )

        # 模拟偶尔一次失败
        if i == 22:
            db.record_run(date_str, status="FAILED", report="数据拉取超时")
        else:
            db.record_run(date_str, status="SUCCESS", report=json.dumps({
                "date": date_str, "state": state, "nav": round(nav, 2),
            }))

    for date_str, severity, action, dd, corr in events:
        db.save_risk_event(
            date=date_str, event_type=action.split(":")[0].strip(),
            severity=severity, drawdown=dd, spy_tlt_corr=corr,
            action_taken=action,
        )

    db.update_portfolio(
        state=state, nav=nav,
        positions=json.dumps(positions),
        high_water_mark=hwm,
        cooldown_counter=cooldown,
        rebalance_count=rebalance_count,
        protection_count=protection_count,
    )

    return db


if __name__ == "__main__":
    # 1. 生成模拟数据
    db = generate_mock_data()

    # 2. 临时替换 Database 的默认路径，调用真实 report
    import db.database as db_module
    original_path = db_module.DB_PATH
    db_module.DB_PATH = MOCK_DB_PATH

    from runner.report import generate_report
    generate_report()

    # 3. 恢复并清理
    db_module.DB_PATH = original_path
    if MOCK_DB_PATH.exists():
        MOCK_DB_PATH.unlink()
