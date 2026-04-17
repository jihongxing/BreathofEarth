"""
息壤（Xi-Rang）券商适配层基础设施

把“如何和券商说话”从执行器里拆出来。
执行器负责生成调仓动作，适配器负责读取账户、报价、订单与回执。
"""

from __future__ import annotations

import importlib
import json
import logging
import os
import ssl
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from urllib import error, parse, request
from typing import Optional

from engine.execution.base import OrderSide, OrderStatus

logger = logging.getLogger("xirang.execution.adapter")


class BrokerMode(Enum):
    READ_ONLY = "read_only"
    PAPER = "paper"
    LIVE = "live"


@dataclass(frozen=True)
class BrokerCapabilities:
    supports_live_trading: bool = False
    supports_paper_trading: bool = False
    supports_read_only: bool = True
    supports_order_updates: bool = False
    supported_markets: tuple[str, ...] = ()


@dataclass
class PositionSnapshot:
    symbol: str
    quantity: float
    market_value: float
    avg_cost: Optional[float] = None


@dataclass
class AccountSnapshot:
    broker_name: str
    mode: BrokerMode
    account_id: Optional[str]
    currency: str
    cash: float
    total_value: float
    positions: dict[str, PositionSnapshot] = field(default_factory=dict)
    as_of: Optional[datetime] = None
    raw: dict = field(default_factory=dict)


@dataclass
class QuoteSnapshot:
    symbol: str
    bid: float
    ask: float
    last: float
    as_of: Optional[datetime] = None

    @property
    def mid_price(self) -> float:
        if self.bid > 0 and self.ask > 0:
            return (self.bid + self.ask) / 2
        return self.last


@dataclass
class BrokerOrderRequest:
    symbol: str
    side: OrderSide
    quantity: int
    order_type: str = "MARKET"
    limit_price: Optional[float] = None
    client_order_id: Optional[str] = None


@dataclass
class BrokerOrderReceipt:
    order_id: str
    status: OrderStatus
    symbol: str
    side: OrderSide
    requested_quantity: int
    filled_quantity: int = 0
    avg_fill_price: Optional[float] = None
    message: str = ""
    broker_reference: Optional[str] = None
    commission: Optional[float] = None
    raw: dict = field(default_factory=dict)


class BrokerAdapter(ABC):
    """
    券商 API 适配器基类。

    约束：
    - 适配器不负责投资决策
    - 适配器不负责更新本地组合状态
    - 适配器先做“可读、可验、可对账”，再做“可交易”
    """

    broker_name = "unknown"

    def __init__(self, mode: BrokerMode = BrokerMode.READ_ONLY, assets: Optional[list[str]] = None):
        self.mode = mode
        self.assets = list(assets or [])
        self.broker_role = "primary"

    @property
    @abstractmethod
    def capabilities(self) -> BrokerCapabilities:
        """返回适配器能力矩阵。"""

    @abstractmethod
    def connect(self) -> bool:
        """建立连接。"""

    @abstractmethod
    def get_account_snapshot(self) -> AccountSnapshot:
        """拉取账户快照。"""

    @abstractmethod
    def get_quote(self, symbol: str) -> QuoteSnapshot:
        """获取单一标的报价。"""

    def get_quotes(self, symbols: list[str]) -> dict[str, QuoteSnapshot]:
        return {symbol: self.get_quote(symbol) for symbol in symbols}

    @abstractmethod
    def place_order(self, order: BrokerOrderRequest) -> BrokerOrderReceipt:
        """提交订单。"""

    @abstractmethod
    def get_order_status(self, order_id: str) -> BrokerOrderReceipt:
        """查询订单状态。"""

    @abstractmethod
    def cancel_order(self, order_id: str) -> BrokerOrderReceipt:
        """取消订单。"""

    def supports_trading(self) -> bool:
        if self.mode == BrokerMode.LIVE:
            return self.capabilities.supports_live_trading
        if self.mode == BrokerMode.PAPER:
            return self.capabilities.supports_paper_trading
        return False

    def ensure_can_trade(self):
        if not self.supports_trading():
            raise RuntimeError(
                f"{self.broker_name} 当前模式为 {self.mode.value}，"
                "未启用真实交易能力。请先完成只读对账与影子运行。"
            )


class PlaceholderBrokerAdapter(BrokerAdapter):
    """
    尚未接通真实 API 的占位适配器。

    目的不是伪装“已完成”，而是先把边界钉住，让上层代码与测试可以围绕统一协议演进。
    """

    capability_matrix = BrokerCapabilities()

    @property
    def capabilities(self) -> BrokerCapabilities:
        return self.capability_matrix

    def connect(self) -> bool:
        logger.warning("%s 适配器尚未接入真实 API", self.broker_name)
        return False

    def get_account_snapshot(self) -> AccountSnapshot:
        raise NotImplementedError(f"{self.broker_name} 账户同步尚未实现")

    def get_quote(self, symbol: str) -> QuoteSnapshot:
        raise NotImplementedError(f"{self.broker_name} 行情接口尚未实现")

    def place_order(self, order: BrokerOrderRequest) -> BrokerOrderReceipt:
        self.ensure_can_trade()
        raise NotImplementedError(f"{self.broker_name} 下单接口尚未实现")

    def get_order_status(self, order_id: str) -> BrokerOrderReceipt:
        raise NotImplementedError(f"{self.broker_name} 订单查询尚未实现")

    def cancel_order(self, order_id: str) -> BrokerOrderReceipt:
        raise NotImplementedError(f"{self.broker_name} 撤单接口尚未实现")


class IBKRAdapter(PlaceholderBrokerAdapter):
    broker_name = "ibkr"
    capability_matrix = BrokerCapabilities(
        supports_live_trading=True,
        supports_paper_trading=True,
        supports_read_only=True,
        supports_order_updates=True,
        supported_markets=("US", "HK", "SG", "GLOBAL"),
    )

    def __init__(self, mode: BrokerMode = BrokerMode.READ_ONLY, assets: Optional[list[str]] = None):
        super().__init__(mode=mode, assets=assets)
        self.base_url = os.environ.get("IBKR_API_BASE_URL", "https://127.0.0.1:5000/v1/api").rstrip("/")
        self.account_id = os.environ.get("IBKR_ACCOUNT_ID", "").strip() or None
        self.timeout_sec = float(os.environ.get("IBKR_TIMEOUT_SEC", "10"))
        self.verify_tls = os.environ.get("IBKR_VERIFY_TLS", "").lower() in {"1", "true", "yes"}
        self.allow_order_submission = os.environ.get("IBKR_ENABLE_ORDER_SUBMISSION", "").lower() in {"1", "true", "yes"}
        self.default_tif = os.environ.get("IBKR_ORDER_TIF", "DAY").upper()
        self.default_listing_exchange = os.environ.get("IBKR_LISTING_EXCHANGE", "SMART").upper()
        self.reply_confirm_limit = int(os.environ.get("IBKR_REPLY_CONFIRM_LIMIT", "3"))

    def _ssl_context(self):
        if self.verify_tls:
            return None
        return ssl._create_unverified_context()

    def _request_json(
        self,
        path: str,
        query: Optional[dict] = None,
        *,
        method: str = "GET",
        body=None,
    ):
        url = f"{self.base_url}{path}"
        if query:
            url = f"{url}?{parse.urlencode(query)}"
        headers = {"Accept": "application/json"}
        data = None
        if body is not None:
            headers["Content-Type"] = "application/json"
            data = json.dumps(body).encode("utf-8")
        req = request.Request(url=url, headers=headers, method=method.upper(), data=data)
        try:
            with request.urlopen(req, timeout=self.timeout_sec, context=self._ssl_context()) as resp:
                payload = resp.read()
        except error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="ignore")
            raise RuntimeError(f"IBKR API HTTP {exc.code}: {detail}") from exc
        except error.URLError as exc:
            raise RuntimeError(f"IBKR API 不可达: {exc.reason}") from exc

        if not payload:
            return None
        return json.loads(payload.decode("utf-8"))

    def _extract_first_result(self, payload):
        if isinstance(payload, list):
            return payload[0] if payload else {}
        if isinstance(payload, dict):
            if isinstance(payload.get("orders"), list) and payload["orders"]:
                return payload["orders"][0]
            if isinstance(payload.get("result"), list) and payload["result"]:
                return payload["result"][0]
            return payload
        return {}

    def _require_order_submission_enabled(self):
        if not self.allow_order_submission:
            raise RuntimeError(
                "IBKR 真实下单开关未开启。"
                "请显式设置 IBKR_ENABLE_ORDER_SUBMISSION=1 后再允许系统提交订单。"
            )

    def _ensure_trading_session(self):
        # IBKR 官方要求下单前先激活 /iserver/accounts 会话。
        try:
            self._request_json("/iserver/accounts")
        except Exception as exc:
            raise RuntimeError(f"IBKR 交易会话未就绪: {exc}") from exc

    def _symbol_override_env_key(self, symbol: str) -> str:
        sanitized = []
        for ch in symbol.upper():
            sanitized.append(ch if ch.isalnum() else "_")
        return "IBKR_CONID_" + "".join(sanitized)

    def _resolve_conid(self, symbol: str) -> str:
        env_key = self._symbol_override_env_key(symbol)
        override = os.environ.get(env_key, "").strip()
        if override:
            return override

        payload = self._request_json(
            "/iserver/secdef/search",
            query={"symbol": symbol},
        )
        items = payload if isinstance(payload, list) else payload.get("result", []) if isinstance(payload, dict) else []

        candidates = []
        symbol_upper = symbol.upper()
        for item in items:
            item_symbol = str(item.get("symbol") or item.get("ticker") or "").strip().upper()
            if item_symbol != symbol_upper:
                continue
            conid = str(item.get("conid") or item.get("conidex") or "").strip()
            if not conid:
                continue

            sec_type = str(item.get("secType") or item.get("assetClass") or "").upper()
            if not sec_type:
                sections = item.get("sections") or []
                if isinstance(sections, list):
                    for section in sections:
                        sec_type = str(section.get("secType") or section.get("assetClass") or "").upper()
                        if sec_type:
                            break
            if sec_type and sec_type not in {"STK", "ETF"}:
                continue
            candidates.append(conid)

        unique_candidates = sorted(set(candidates))
        if len(unique_candidates) == 1:
            return unique_candidates[0]
        if len(unique_candidates) > 1:
            raise RuntimeError(
                f"IBKR 标的 {symbol} 匹配到多个 conid，请显式设置环境变量 {env_key} 固定映射"
            )
        raise RuntimeError(f"IBKR 无法解析标的 {symbol} 的 conid")

    def _extract_order_id(self, payload) -> Optional[str]:
        item = self._extract_first_result(payload)
        for key in ("order_id", "orderId", "id"):
            value = item.get(key)
            if value not in (None, ""):
                return str(value)
        return None

    def _extract_reply_id(self, payload) -> Optional[str]:
        item = self._extract_first_result(payload)
        for key in ("replyid", "replyId"):
            value = item.get(key)
            if value not in (None, ""):
                return str(value)
        if item.get("message") and item.get("id") and not (item.get("order_id") or item.get("orderId")):
            return str(item["id"])
        return None

    def _confirm_reply_chain(self, payload):
        current = payload
        for _ in range(max(self.reply_confirm_limit, 0)):
            reply_id = self._extract_reply_id(current)
            if not reply_id:
                return current
            current = self._request_json(
                f"/iserver/reply/{reply_id}",
                method="POST",
                body={"confirmed": True},
            )
        raise RuntimeError("IBKR 订单确认链超过限制，系统拒绝继续自动确认")

    def _extract_receipt_message(self, payload) -> str:
        item = self._extract_first_result(payload)
        message = item.get("message")
        if isinstance(message, list):
            return " | ".join(str(part) for part in message if part not in (None, ""))
        if message is None:
            for key in ("warn_text", "warning", "error", "text"):
                value = item.get(key)
                if value not in (None, ""):
                    return str(value)
            return ""
        return str(message)

    def _map_order_status(self, raw_status: str) -> OrderStatus:
        status = "".join(ch for ch in str(raw_status or "").upper() if ch.isalnum())
        if not status:
            return OrderStatus.SUBMITTED
        if status in {"FILLED", "EXECUTED"}:
            return OrderStatus.FILLED
        if status in {"PARTIALFILLED", "PARTIALLYFILLED"}:
            return OrderStatus.PARTIAL_FILLED
        if status in {"CANCELLED", "APICANCELLED"}:
            return OrderStatus.CANCELLED
        if status in {"REJECTED", "ERROR", "EXPIRED", "INACTIVE"}:
            return OrderStatus.FAILED
        if status in {
            "PENDING",
            "PENDINGSUBMIT",
            "PRESUBMITTED",
            "SUBMITTED",
            "APIPENDING",
            "PENDINGCANCEL",
        }:
            return OrderStatus.SUBMITTED
        return OrderStatus.SUBMITTED

    def _build_order_receipt(
        self,
        payload,
        *,
        requested_order: Optional[BrokerOrderRequest] = None,
        fallback_order_id: Optional[str] = None,
    ) -> BrokerOrderReceipt:
        item = self._extract_first_result(payload)
        raw_status = (
            item.get("order_status")
            or item.get("orderStatus")
            or item.get("status")
            or item.get("state")
            or "Submitted"
        )
        filled_quantity = item.get("filled_qty")
        if filled_quantity is None:
            filled_quantity = item.get("filledQuantity")
        if filled_quantity is None:
            filled_quantity = item.get("cumFill")
        if filled_quantity in (None, ""):
            filled_quantity = 0

        avg_fill_price = item.get("avg_price")
        if avg_fill_price is None:
            avg_fill_price = item.get("avgFillPrice")
        if avg_fill_price is None:
            avg_fill_price = item.get("price")
        commission = item.get("commission")
        if commission is None:
            commission = item.get("order_fee")
        if commission is None:
            commission = item.get("fee")

        side_value = requested_order.side if requested_order else None
        if side_value is None:
            raw_side = str(item.get("side") or item.get("action") or "BUY").upper()
            side_value = OrderSide.SELL if raw_side == "SELL" else OrderSide.BUY

        return BrokerOrderReceipt(
            order_id=self._extract_order_id(payload) or str(fallback_order_id or ""),
            status=self._map_order_status(raw_status),
            symbol=str(item.get("ticker") or item.get("symbol") or (requested_order.symbol if requested_order else "")).strip(),
            side=side_value,
            requested_quantity=int(item.get("totalSize") or item.get("quantity") or (requested_order.quantity if requested_order else 0)),
            filled_quantity=int(float(filled_quantity or 0)),
            avg_fill_price=float(avg_fill_price) if avg_fill_price not in (None, "") else None,
            message=self._extract_receipt_message(payload),
            broker_reference=str(item.get("local_order_id") or item.get("permId") or item.get("broker_reference") or "") or None,
            commission=float(commission) if commission not in (None, "") else None,
            raw=item if isinstance(item, dict) else {"payload": item},
        )

    def _submit_order_payload(self, account_id: str, payload: dict):
        request_body = {"orders": [payload]}
        try:
            return self._request_json(
                f"/iserver/account/{account_id}/orders",
                method="POST",
                body=request_body,
            )
        except RuntimeError:
            return self._request_json(
                f"/iserver/account/{account_id}/orders",
                method="POST",
                body=[payload],
            )

    def _extract_account_id(self, accounts_payload) -> str:
        accounts = accounts_payload or []
        if isinstance(accounts, dict):
            accounts = accounts.get("accounts", accounts.get("result", []))

        if self.account_id:
            for item in accounts:
                candidate = str(item.get("accountId") or item.get("id") or "").strip()
                if candidate == self.account_id:
                    return candidate
            raise RuntimeError(f"IBKR 账户 {self.account_id} 不在 /portfolio/accounts 返回列表中")

        for item in accounts:
            candidate = str(item.get("accountId") or item.get("id") or "").strip()
            if candidate:
                return candidate

        raise RuntimeError("IBKR 未返回可用账户")

    def _fetch_positions(self, account_id: str) -> list[dict]:
        positions = []
        page = 0
        while True:
            payload = self._request_json(f"/portfolio/{account_id}/positions/{page}")
            if not payload:
                break
            if isinstance(payload, dict):
                batch = payload.get("positions") or payload.get("result") or []
            else:
                batch = payload
            if not batch:
                break
            positions.extend(batch)
            if len(batch) < 100:
                break
            page += 1
        return positions

    def _extract_ledger_base(self, ledger_payload: dict) -> dict:
        if not ledger_payload:
            return {}

        if isinstance(ledger_payload, dict) and "result" in ledger_payload:
            for item in ledger_payload.get("result", []):
                key = str(item.get("key", "")).upper()
                if key.endswith("BASE") or str(item.get("secondKey", "")).upper() == "BASE":
                    return item
            return ledger_payload.get("result", [{}])[0] if ledger_payload.get("result") else {}

        if isinstance(ledger_payload, dict):
            if "BASE" in ledger_payload:
                return ledger_payload["BASE"]
            for key, item in ledger_payload.items():
                if str(key).upper().endswith("BASE"):
                    return item

        return {}

    def connect(self) -> bool:
        try:
            accounts = self._request_json("/portfolio/accounts")
            self._extract_account_id(accounts)
            return True
        except Exception as exc:
            logger.warning("IBKR 只读连接失败: %s", exc)
            return False

    def get_account_snapshot(self) -> AccountSnapshot:
        accounts_payload = self._request_json("/portfolio/accounts")
        account_id = self._extract_account_id(accounts_payload)
        ledger_payload = self._request_json(f"/portfolio/{account_id}/ledger")
        positions_payload = self._fetch_positions(account_id)

        ledger_base = self._extract_ledger_base(ledger_payload)
        currency = str(ledger_base.get("currency") or ledger_base.get("secondKey") or "BASE")
        cash = float(ledger_base.get("cashbalance") or ledger_base.get("cashBalance") or 0.0)
        total_value = float(
            ledger_base.get("netliquidationvalue")
            or ledger_base.get("netLiquidationValue")
            or ledger_base.get("marketValue")
            or 0.0
        )

        positions = {}
        for item in positions_payload:
            symbol = str(item.get("contractDesc") or item.get("ticker") or item.get("symbol") or "").strip()
            if not symbol:
                continue
            positions[symbol] = PositionSnapshot(
                symbol=symbol,
                quantity=float(item.get("position") or item.get("quantity") or 0.0),
                market_value=float(item.get("mktValue") or item.get("marketValue") or 0.0),
                avg_cost=float(item.get("avgCost") or item.get("avg_cost") or 0.0) or None,
            )

        return AccountSnapshot(
            broker_name=self.broker_name,
            mode=self.mode,
            account_id=account_id,
            currency=currency,
            cash=cash,
            total_value=total_value,
            positions=positions,
            raw={
                "accounts": accounts_payload,
                "ledger": ledger_payload,
                "positions": positions_payload,
            },
        )

    def place_order(self, order: BrokerOrderRequest) -> BrokerOrderReceipt:
        self.ensure_can_trade()
        self._require_order_submission_enabled()
        self._ensure_trading_session()

        accounts_payload = self._request_json("/portfolio/accounts")
        account_id = self._extract_account_id(accounts_payload)
        conid = self._resolve_conid(order.symbol)

        order_type = str(order.order_type or "MARKET").upper()
        order_payload = {
            "acctId": account_id,
            "conid": conid,
            "ticker": order.symbol,
            "orderType": "MKT" if order_type == "MARKET" else order_type,
            "side": order.side.value,
            "quantity": int(order.quantity),
            "tif": self.default_tif,
            "listingExchange": self.default_listing_exchange,
        }
        if order.client_order_id:
            order_payload["cOID"] = order.client_order_id
        if order.limit_price is not None:
            order_payload["price"] = float(order.limit_price)

        payload = self._submit_order_payload(account_id, order_payload)
        confirmed_payload = self._confirm_reply_chain(payload)
        receipt = self._build_order_receipt(confirmed_payload, requested_order=order)
        if not receipt.order_id:
            raise RuntimeError(f"IBKR 未返回有效订单号: {confirmed_payload}")
        return receipt

    def get_order_status(self, order_id: str) -> BrokerOrderReceipt:
        self._ensure_trading_session()
        payload = self._request_json(f"/iserver/account/order/status/{order_id}")
        return self._build_order_receipt(payload, fallback_order_id=order_id)

    def cancel_order(self, order_id: str) -> BrokerOrderReceipt:
        self.ensure_can_trade()
        self._require_order_submission_enabled()
        self._ensure_trading_session()
        accounts_payload = self._request_json("/portfolio/accounts")
        account_id = self._extract_account_id(accounts_payload)
        payload = self._request_json(
            f"/iserver/account/{account_id}/order/{order_id}",
            method="DELETE",
        )
        receipt = self._build_order_receipt(payload, fallback_order_id=order_id)
        if receipt.status == OrderStatus.SUBMITTED:
            receipt.status = OrderStatus.CANCELLED
        if not receipt.message:
            receipt.message = "cancel requested"
        return receipt


class FutuAdapter(PlaceholderBrokerAdapter):
    broker_name = "futu"
    capability_matrix = BrokerCapabilities(
        supports_live_trading=True,
        supports_paper_trading=True,
        supports_read_only=True,
        supports_order_updates=True,
        supported_markets=("US", "HK", "CN_CONNECT"),
    )

    def __init__(self, mode: BrokerMode = BrokerMode.READ_ONLY, assets: Optional[list[str]] = None):
        super().__init__(mode=mode, assets=assets)
        self.host = os.environ.get("FUTU_HOST", "127.0.0.1")
        self.port = int(os.environ.get("FUTU_PORT", "11111"))
        self.account_id = int(os.environ.get("FUTU_ACC_ID", "0"))
        self.market = os.environ.get("FUTU_TRD_MARKET", "").upper()
        self.env_name = os.environ.get(
            "FUTU_TRD_ENV",
            "SIMULATE" if self.mode == BrokerMode.PAPER else "REAL",
        ).upper()
        self.security_firm = os.environ.get("FUTU_SECURITY_FIRM", "FUTUSECURITIES").upper()
        self.allow_order_submission = os.environ.get("FUTU_ENABLE_ORDER_SUBMISSION", "").lower() in {"1", "true", "yes"}
        self.unlock_trade_password = os.environ.get("FUTU_UNLOCK_TRADE_PWD", "")

    def _load_futu(self):
        try:
            return importlib.import_module("futu")
        except ImportError as exc:
            raise RuntimeError(
                "未安装 futu-api，无法连接 Futu OpenD。"
                "请先安装 futu-api 并启动 OpenD。"
            ) from exc

    def _guess_market(self) -> str:
        if self.market:
            return self.market
        if any(symbol.endswith(".SS") or symbol.endswith(".SZ") or symbol == "MONEY" for symbol in self.assets):
            return "CN"
        return "US"

    def _resolve_enum(self, module, enum_name: str, member_name: str):
        enum_type = getattr(module, enum_name)
        if hasattr(enum_type, member_name):
            return getattr(enum_type, member_name)
        raise RuntimeError(f"Futu {enum_name}.{member_name} 不存在")

    def _get_trade_context(self, module):
        market = self._resolve_enum(module, "TrdMarket", self._guess_market())
        trd_env = self._resolve_enum(module, "TrdEnv", self.env_name)
        security_firm_enum = getattr(module, "SecurityFirm", None)
        security_firm = None
        if security_firm_enum is not None:
            if hasattr(security_firm_enum, self.security_firm):
                security_firm = getattr(security_firm_enum, self.security_firm)
            elif hasattr(security_firm_enum, "FUTUSECURITIES"):
                security_firm = getattr(security_firm_enum, "FUTUSECURITIES")

        kwargs = {
            "filter_trdmarket": market,
            "host": self.host,
            "port": self.port,
        }
        if security_firm is not None:
            kwargs["security_firm"] = security_firm

        ctx = module.OpenSecTradeContext(**kwargs)
        return ctx, trd_env

    def _pick_account(self, accounts_df, trd_env):
        records = accounts_df.to_dict("records") if hasattr(accounts_df, "to_dict") else list(accounts_df or [])
        if self.account_id:
            for item in records:
                if int(item.get("acc_id", 0)) == self.account_id:
                    return self.account_id
            raise RuntimeError(f"Futu 账户 {self.account_id} 不在 get_acc_list 返回列表中")

        for item in records:
            item_env = str(item.get("trd_env", "")).upper()
            target_env = str(getattr(trd_env, "name", trd_env)).upper()
            if not item_env or item_env == target_env:
                acc_id = int(item.get("acc_id", 0))
                if acc_id:
                    return acc_id

        raise RuntimeError("Futu 未返回可用账户")

    def _close_context(self, ctx):
        close = getattr(ctx, "close", None)
        if callable(close):
            close()

    def _require_order_submission_enabled(self):
        if not self.allow_order_submission:
            raise RuntimeError(
                "Futu 真实下单开关未开启。"
                "请显式设置 FUTU_ENABLE_ORDER_SUBMISSION=1 后再允许系统提交订单。"
            )

    def _normalize_symbol(self, symbol: str) -> str:
        if "." in symbol:
            return symbol
        market = self._guess_market()
        if market == "CN":
            return symbol
        return f"{market}.{symbol}"

    def _ensure_trade_unlock(self, ctx):
        if self.mode != BrokerMode.LIVE:
            return
        if not self.unlock_trade_password:
            raise RuntimeError("Futu 实盘下单需要 FUTU_UNLOCK_TRADE_PWD 才能解锁交易")

        unlock = getattr(ctx, "unlock_trade", None)
        if not callable(unlock):
            raise RuntimeError("当前 futu-api 未提供 unlock_trade，无法进入真实交易")

        ret, data = unlock(password=self.unlock_trade_password)
        module = self._load_futu()
        if ret != getattr(module, "RET_OK"):
            raise RuntimeError(f"unlock_trade 失败: {data}")

    def _resolve_trade_side(self, module, side: OrderSide):
        enum_name = "TrdSide"
        member_name = "BUY" if side == OrderSide.BUY else "SELL"
        return self._resolve_enum(module, enum_name, member_name)

    def _resolve_order_type(self, module, order_type: str):
        order_type_name = str(order_type or "MARKET").upper()
        enum_type = getattr(module, "OrderType")
        if order_type_name == "MARKET" and hasattr(enum_type, "MARKET"):
            return getattr(enum_type, "MARKET")
        if order_type_name in {"LIMIT", "MARKET"} and hasattr(enum_type, "NORMAL"):
            return getattr(enum_type, "NORMAL")
        if hasattr(enum_type, order_type_name):
            return getattr(enum_type, order_type_name)
        raise RuntimeError(f"Futu 不支持订单类型 {order_type_name}")

    def _find_order_row(self, frame, order_id: str | int | None = None):
        records = frame.to_dict("records") if hasattr(frame, "to_dict") else list(frame or [])
        if order_id in (None, ""):
            return records[0] if records else {}
        target = str(order_id)
        for row in records:
            candidate = row.get("order_id")
            if candidate is not None and str(candidate) == target:
                return row
        return records[0] if records else {}

    def _map_futu_order_status(self, raw_status: str) -> OrderStatus:
        status = str(raw_status or "").upper()
        if status in {"FILLED_ALL", "FILLED"}:
            return OrderStatus.FILLED
        if status in {"FILLED_PART", "CANCELLED_PART", "CANCELLING_PART"}:
            return OrderStatus.PARTIAL_FILLED
        if status in {"CANCELLED_ALL", "CANCELLED"}:
            return OrderStatus.CANCELLED
        if status in {"FAILED", "DISABLED", "DELETED"}:
            return OrderStatus.FAILED
        if status in {
            "UNSUBMITTED",
            "WAITING_SUBMIT",
            "SUBMITTING",
            "SUBMITTED",
            "CANCELLING",
        }:
            return OrderStatus.SUBMITTED
        return OrderStatus.SUBMITTED

    def _build_futu_order_receipt(
        self,
        row: dict,
        *,
        requested_order: Optional[BrokerOrderRequest] = None,
        fallback_order_id: Optional[str] = None,
    ) -> BrokerOrderReceipt:
        raw_side = str(row.get("trd_side") or row.get("side") or "").upper()
        side = requested_order.side if requested_order else (OrderSide.SELL if raw_side == "SELL" else OrderSide.BUY)
        requested_quantity = row.get("qty")
        if requested_quantity in (None, ""):
            requested_quantity = row.get("order_qty")
        if requested_quantity in (None, "") and requested_order is not None:
            requested_quantity = requested_order.quantity

        filled_quantity = row.get("dealt_qty")
        if filled_quantity in (None, ""):
            filled_quantity = row.get("filled_qty")
        if filled_quantity in (None, ""):
            filled_quantity = 0

        avg_fill_price = row.get("dealt_avg_price")
        if avg_fill_price in (None, ""):
            avg_fill_price = row.get("avg_fill_price")
        if avg_fill_price in (None, ""):
            avg_fill_price = row.get("price")

        commission = row.get("order_fee")
        if commission in (None, ""):
            commission = row.get("fee_amount")
        if commission in (None, ""):
            commission = row.get("fee")

        symbol = str(row.get("code") or (requested_order.symbol if requested_order else "")).strip()
        if "." in symbol and requested_order is not None:
            normalized_requested = self._normalize_symbol(requested_order.symbol)
            if symbol == normalized_requested:
                symbol = requested_order.symbol

        return BrokerOrderReceipt(
            order_id=str(row.get("order_id") or fallback_order_id or ""),
            status=self._map_futu_order_status(str(row.get("order_status") or row.get("status") or "")),
            symbol=symbol,
            side=side,
            requested_quantity=int(float(requested_quantity or 0)),
            filled_quantity=int(float(filled_quantity or 0)),
            avg_fill_price=float(avg_fill_price) if avg_fill_price not in (None, "") else None,
            message=str(row.get("remark") or row.get("last_err_msg") or row.get("msg") or ""),
            broker_reference=str(row.get("remark") or row.get("order_id") or "") or None,
            commission=float(commission) if commission not in (None, "") else None,
            raw=dict(row or {}),
        )

    def _query_order_frame(self, ctx, module, trd_env, account_id: int, order_id: str | int):
        ret, data = ctx.order_list_query(
            order_id=order_id,
            trd_env=trd_env,
            acc_id=account_id,
            refresh_cache=True,
        )
        if ret != getattr(module, "RET_OK"):
            raise RuntimeError(f"order_list_query 失败: {data}")
        return data

    def _query_order_fee(self, ctx, module, trd_env, account_id: int, order_id: str | int) -> Optional[float]:
        fee_query = getattr(ctx, "order_fee_query", None)
        if not callable(fee_query):
            return None
        ret, data = fee_query(order_id_list=[str(order_id)], trd_env=trd_env, acc_id=account_id)
        if ret != getattr(module, "RET_OK"):
            return None
        row = self._find_order_row(data, order_id)
        fee = row.get("fee_amount")
        if fee in (None, ""):
            fee = row.get("order_fee")
        if fee in (None, ""):
            return None
        return float(fee)

    def connect(self) -> bool:
        try:
            module = self._load_futu()
            ctx, _ = self._get_trade_context(module)
            try:
                ret, data = ctx.get_acc_list()
            finally:
                self._close_context(ctx)
            return ret == getattr(module, "RET_OK") and len(data.index) >= 1
        except Exception as exc:
            logger.warning("Futu 只读连接失败: %s", exc)
            return False

    def get_account_snapshot(self) -> AccountSnapshot:
        module = self._load_futu()
        ctx, trd_env = self._get_trade_context(module)
        try:
            ret, accounts_df = ctx.get_acc_list()
            if ret != getattr(module, "RET_OK"):
                raise RuntimeError(f"get_acc_list 失败: {accounts_df}")
            account_id = self._pick_account(accounts_df, trd_env)

            ret, accinfo_df = ctx.accinfo_query(trd_env=trd_env, acc_id=account_id, refresh_cache=True)
            if ret != getattr(module, "RET_OK"):
                raise RuntimeError(f"accinfo_query 失败: {accinfo_df}")

            ret, positions_df = ctx.position_list_query(trd_env=trd_env, acc_id=account_id, refresh_cache=True)
            if ret != getattr(module, "RET_OK"):
                raise RuntimeError(f"position_list_query 失败: {positions_df}")
        finally:
            self._close_context(ctx)

        accinfo_records = accinfo_df.to_dict("records") if hasattr(accinfo_df, "to_dict") else list(accinfo_df or [])
        account_row = accinfo_records[0] if accinfo_records else {}
        currency = str(account_row.get("currency") or self._guess_market())
        cash = float(account_row.get("cash") or 0.0)
        total_value = float(account_row.get("total_assets") or account_row.get("total_asset") or 0.0)

        positions = {}
        position_records = positions_df.to_dict("records") if hasattr(positions_df, "to_dict") else list(positions_df or [])
        for item in position_records:
            symbol = str(item.get("code") or "").strip()
            if not symbol:
                continue
            avg_cost = item.get("average_cost")
            if avg_cost is None:
                avg_cost = item.get("diluted_cost")
            if avg_cost is None:
                avg_cost = item.get("cost_price")
            positions[symbol] = PositionSnapshot(
                symbol=symbol,
                quantity=float(item.get("qty") or 0.0),
                market_value=float(item.get("market_val") or 0.0),
                avg_cost=float(avg_cost) if avg_cost not in (None, "") else None,
            )

        return AccountSnapshot(
            broker_name=self.broker_name,
            mode=self.mode,
            account_id=str(account_id),
            currency=currency,
            cash=cash,
            total_value=total_value,
            positions=positions,
            raw={
                "accinfo": accinfo_records,
                "positions": position_records,
            },
        )

    def place_order(self, order: BrokerOrderRequest) -> BrokerOrderReceipt:
        self.ensure_can_trade()
        self._require_order_submission_enabled()

        module = self._load_futu()
        ctx, trd_env = self._get_trade_context(module)
        try:
            self._ensure_trade_unlock(ctx)
            ret, accounts_df = ctx.get_acc_list()
            if ret != getattr(module, "RET_OK"):
                raise RuntimeError(f"get_acc_list 失败: {accounts_df}")
            account_id = self._pick_account(accounts_df, trd_env)

            normalized_symbol = self._normalize_symbol(order.symbol)
            trade_side = self._resolve_trade_side(module, order.side)
            order_type = self._resolve_order_type(module, order.order_type)
            price = float(order.limit_price) if order.limit_price is not None else 0.0
            ret, data = ctx.place_order(
                price=price,
                qty=int(order.quantity),
                code=normalized_symbol,
                trd_side=trade_side,
                order_type=order_type,
                trd_env=trd_env,
                acc_id=account_id,
                remark=order.client_order_id or "",
            )
            if ret != getattr(module, "RET_OK"):
                raise RuntimeError(f"place_order 失败: {data}")

            row = self._find_order_row(data)
            receipt = self._build_futu_order_receipt(row, requested_order=order)
            if receipt.order_id:
                fee = self._query_order_fee(ctx, module, trd_env, account_id, receipt.order_id)
                if fee is not None:
                    receipt.commission = fee
                    receipt.raw["order_fee"] = fee
            return receipt
        finally:
            self._close_context(ctx)

    def get_order_status(self, order_id: str) -> BrokerOrderReceipt:
        module = self._load_futu()
        ctx, trd_env = self._get_trade_context(module)
        try:
            ret, accounts_df = ctx.get_acc_list()
            if ret != getattr(module, "RET_OK"):
                raise RuntimeError(f"get_acc_list 失败: {accounts_df}")
            account_id = self._pick_account(accounts_df, trd_env)
            data = self._query_order_frame(ctx, module, trd_env, account_id, order_id)
            row = self._find_order_row(data, order_id)
            receipt = self._build_futu_order_receipt(row, fallback_order_id=order_id)
            fee = self._query_order_fee(ctx, module, trd_env, account_id, order_id)
            if fee is not None:
                receipt.commission = fee
                receipt.raw["order_fee"] = fee
            return receipt
        finally:
            self._close_context(ctx)

    def cancel_order(self, order_id: str) -> BrokerOrderReceipt:
        self.ensure_can_trade()
        self._require_order_submission_enabled()

        module = self._load_futu()
        ctx, trd_env = self._get_trade_context(module)
        try:
            self._ensure_trade_unlock(ctx)
            ret, accounts_df = ctx.get_acc_list()
            if ret != getattr(module, "RET_OK"):
                raise RuntimeError(f"get_acc_list 失败: {accounts_df}")
            account_id = self._pick_account(accounts_df, trd_env)
            modify_order_op = self._resolve_enum(module, "ModifyOrderOp", "CANCEL")
            ret, data = ctx.modify_order(
                modify_order_op=modify_order_op,
                order_id=order_id,
                price=0.0,
                qty=0,
                trd_env=trd_env,
                acc_id=account_id,
            )
            if ret != getattr(module, "RET_OK"):
                raise RuntimeError(f"modify_order(cancel) 失败: {data}")

            row = self._find_order_row(data, order_id)
            if not row:
                data = self._query_order_frame(ctx, module, trd_env, account_id, order_id)
                row = self._find_order_row(data, order_id)

            receipt = self._build_futu_order_receipt(row, fallback_order_id=order_id)
            if receipt.status == OrderStatus.SUBMITTED:
                receipt.status = OrderStatus.CANCELLED
            if not receipt.message:
                receipt.message = "cancel requested"
            return receipt
        finally:
            self._close_context(ctx)
