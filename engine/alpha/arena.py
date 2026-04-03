"""
策略竞技场 — 多策略并行运行 + 季度评估 + 自动淘汰

核心功能：
1. 并行运行所有 ENABLED 策略
2. 季度评估：基于夏普比率和最大回撤
3. 自动淘汰：表现差的策略被 SUSPENDED
4. 资金再分配：按风险调整后收益加权
5. 策略排行榜与竞技报告

设计原则：
- 每个策略独立运行，互不干扰
- 评估完全基于数据，无人工干预（但 admin 可手动覆盖）
- SUSPENDED 策略保留历史记录，可手动重新启用
"""

import math
import logging
from datetime import datetime, timedelta
from db.database import Database
from engine.alpha.registry import REGISTRY, get_strategy_class

logger = logging.getLogger("xirang.alpha.arena")


class StrategyArena:
    """策略竞技场：管理所有 Alpha 策略的运行与评估"""

    # 评估参数
    MIN_SHARPE_THRESHOLD = 0.3      # 夏普 < 0.3 触发淘汰警告
    SUSPEND_SHARPE_THRESHOLD = 0.0  # 夏普 < 0 直接暂停
    MAX_DRAWDOWN_LIMIT = -0.15      # 最大回撤 > 15% 直接暂停
    EVAL_WINDOW_DAYS = 90           # 评估窗口：90 天
    MIN_EVAL_DAYS = 30              # 最少运行 30 天才评估

    def __init__(self, db: Database):
        self.db = db

    def run_all(self, portfolio_id: str, current_date: str,
                spy_price: float, nav: float) -> list[dict]:
        """
        运行所有 ENABLED 策略。

        Returns:
            每个策略的执行结果列表
        """
        results = []
        for strategy_id, cls in REGISTRY.items():
            instance = cls(self.db)
            instance.ensure_registered(portfolio_id)

            if not instance.is_enabled(portfolio_id):
                continue

            try:
                result = instance.run(
                    portfolio_id=portfolio_id,
                    current_date=current_date,
                    spy_price=spy_price,
                    nav=nav,
                )
                result["strategy_id"] = strategy_id
                results.append(result)
                logger.info(f"策略 {strategy_id}: {result.get('action', 'N/A')}")
            except Exception as e:
                logger.error(f"策略 {strategy_id} 执行失败: {e}")
                results.append({
                    "strategy_id": strategy_id,
                    "action": "ERROR",
                    "reason": str(e),
                })

        return results

    def quarterly_evaluation(self, portfolio_id: str = "us") -> dict:
        """
        季度评估：计算绩效指标 → 排名 → 淘汰/重分配。

        Returns:
            评估报告 dict
        """
        strategies = self.db.list_strategies(portfolio_id)
        evaluations = []

        for s in strategies:
            sid = s["id"]
            if sid not in REGISTRY:
                continue

            snapshots = self._get_snapshots(sid, self.EVAL_WINDOW_DAYS)
            if len(snapshots) < self.MIN_EVAL_DAYS:
                evaluations.append({
                    "strategy_id": sid,
                    "name": s["name"],
                    "status": s["status"],
                    "verdict": "INSUFFICIENT_DATA",
                    "reason": f"数据不足（{len(snapshots)}/{self.MIN_EVAL_DAYS} 天）",
                    "days": len(snapshots),
                })
                continue

            metrics = self._calculate_metrics(snapshots)
            verdict, reason = self._evaluate(metrics, s)

            evaluations.append({
                "strategy_id": sid,
                "name": s["name"],
                "status": s["status"],
                "metrics": metrics,
                "verdict": verdict,
                "reason": reason,
            })

            # 执行淘汰
            if verdict == "SUSPEND" and s["status"] == "ENABLED":
                self.db.update_strategy_status(sid, "SUSPENDED")
                logger.warning(f"策略 {sid} 被暂停: {reason}")

        # 资金再分配（仅 ENABLED 策略）
        allocation = self._reallocate(evaluations)

        report = {
            "date": datetime.now().strftime("%Y-%m-%d"),
            "portfolio_id": portfolio_id,
            "evaluations": evaluations,
            "allocation": allocation,
            "summary": self._generate_summary(evaluations),
        }

        # 保存评估结果到审计日志
        self.db.save_audit_log(
            "ARENA_EVAL", "system",
            f"季度评估完成: {len(evaluations)} 个策略, "
            f"{sum(1 for e in evaluations if e['verdict'] == 'SUSPEND')} 个暂停",
        )

        return report

    def get_leaderboard(self, portfolio_id: str = "us") -> list[dict]:
        """
        策略排行榜：按夏普比率降序。

        Returns:
            策略排行列表
        """
        strategies = self.db.list_strategies(portfolio_id)
        board = []

        for s in strategies:
            sid = s["id"]
            if sid not in REGISTRY:
                continue

            snapshots = self._get_snapshots(sid, self.EVAL_WINDOW_DAYS)
            metrics = self._calculate_metrics(snapshots) if len(snapshots) >= 2 else {}

            board.append({
                "rank": 0,
                "strategy_id": sid,
                "name": s["name"],
                "status": s["status"],
                "allocation_pct": s.get("allocation_pct", 0),
                "capital": s.get("capital", 0),
                "total_premium": s.get("total_premium", 0),
                "total_pnl": s.get("total_pnl", 0),
                "trade_count": s.get("trade_count", 0),
                "sharpe": metrics.get("sharpe", 0),
                "max_drawdown": metrics.get("max_drawdown", 0),
                "annualized_return": metrics.get("annualized_return", 0),
                "win_rate": metrics.get("win_rate", 0),
                "days_active": len(snapshots),
            })

        # 按夏普比率排序
        board.sort(key=lambda x: x["sharpe"], reverse=True)
        for i, b in enumerate(board):
            b["rank"] = i + 1

        return board

    # ── 内部方法 ──────────────────────────────────────

    def _get_snapshots(self, strategy_id: str, days: int) -> list[dict]:
        """获取策略日快照"""
        with self.db._conn() as conn:
            rows = conn.execute(
                """SELECT * FROM alpha_snapshots
                   WHERE strategy_id = ?
                   ORDER BY date DESC LIMIT ?""",
                (strategy_id, days),
            ).fetchall()
            return [dict(r) for r in reversed(rows)]

    def _calculate_metrics(self, snapshots: list[dict]) -> dict:
        """计算绩效指标"""
        if len(snapshots) < 2:
            return {"sharpe": 0, "max_drawdown": 0, "annualized_return": 0,
                    "volatility": 0, "win_rate": 0}

        navs = [s["nav"] for s in snapshots]
        returns = []
        for i in range(1, len(navs)):
            if navs[i - 1] > 0:
                returns.append((navs[i] - navs[i - 1]) / navs[i - 1])

        if not returns:
            return {"sharpe": 0, "max_drawdown": 0, "annualized_return": 0,
                    "volatility": 0, "win_rate": 0}

        # 年化收益
        total_return = (navs[-1] / navs[0] - 1) if navs[0] > 0 else 0
        days = len(snapshots)
        annualized = (1 + total_return) ** (252 / max(days, 1)) - 1

        # 波动率（年化）
        mean_r = sum(returns) / len(returns)
        variance = sum((r - mean_r) ** 2 for r in returns) / max(len(returns) - 1, 1)
        vol = math.sqrt(variance) * math.sqrt(252)

        # 夏普比率（无风险利率 5%）
        sharpe = (annualized - 0.05) / vol if vol > 0 else 0

        # 最大回撤
        peak = navs[0]
        max_dd = 0
        for n in navs:
            if n > peak:
                peak = n
            dd = (n - peak) / peak if peak > 0 else 0
            if dd < max_dd:
                max_dd = dd

        # 胜率
        win_rate = sum(1 for r in returns if r > 0) / len(returns) if returns else 0

        return {
            "sharpe": round(sharpe, 3),
            "max_drawdown": round(max_dd, 4),
            "annualized_return": round(annualized, 4),
            "volatility": round(vol, 4),
            "win_rate": round(win_rate, 4),
            "total_return": round(total_return, 4),
            "days": days,
        }

    def _evaluate(self, metrics: dict, strategy: dict) -> tuple[str, str]:
        """评估策略表现，返回 (verdict, reason)"""
        sharpe = metrics.get("sharpe", 0)
        max_dd = metrics.get("max_drawdown", 0)

        # 最大回撤过大 → 暂停
        if max_dd < self.MAX_DRAWDOWN_LIMIT:
            return "SUSPEND", f"最大回撤 {max_dd:.1%} 超过限制 {self.MAX_DRAWDOWN_LIMIT:.0%}"

        # 夏普 < 0 → 暂停
        if sharpe < self.SUSPEND_SHARPE_THRESHOLD:
            return "SUSPEND", f"夏普比率 {sharpe:.2f} < 0，策略亏损"

        # 夏普 < 0.3 → 警告
        if sharpe < self.MIN_SHARPE_THRESHOLD:
            return "WARNING", f"夏普比率 {sharpe:.2f} 偏低（阈值 {self.MIN_SHARPE_THRESHOLD}）"

        return "PASS", f"表现良好（夏普 {sharpe:.2f}）"

    def _reallocate(self, evaluations: list[dict]) -> dict:
        """按夏普比率加权重新分配资金"""
        active = [e for e in evaluations
                  if e.get("verdict") in ("PASS", "WARNING")
                  and e.get("metrics", {}).get("sharpe", 0) > 0]

        if not active:
            return {"message": "无可分配策略"}

        total_sharpe = sum(e["metrics"]["sharpe"] for e in active)
        allocation = {}
        for e in active:
            weight = e["metrics"]["sharpe"] / total_sharpe if total_sharpe > 0 else 1 / len(active)
            allocation[e["strategy_id"]] = {
                "weight": round(weight, 4),
                "sharpe": e["metrics"]["sharpe"],
            }
            # 更新数据库
            self.db.upsert_strategy(e["strategy_id"], allocation_pct=round(weight * 0.10, 4))

        return allocation

    def _generate_summary(self, evaluations: list[dict]) -> str:
        """生成评估摘要"""
        total = len(evaluations)
        passed = sum(1 for e in evaluations if e.get("verdict") == "PASS")
        warned = sum(1 for e in evaluations if e.get("verdict") == "WARNING")
        suspended = sum(1 for e in evaluations if e.get("verdict") == "SUSPEND")
        insufficient = sum(1 for e in evaluations if e.get("verdict") == "INSUFFICIENT_DATA")

        parts = [f"共 {total} 个策略"]
        if passed:
            parts.append(f"{passed} 个通过")
        if warned:
            parts.append(f"{warned} 个警告")
        if suspended:
            parts.append(f"{suspended} 个暂停")
        if insufficient:
            parts.append(f"{insufficient} 个数据不足")

        return "，".join(parts)
