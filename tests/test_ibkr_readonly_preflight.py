from live import ibkr_readonly_preflight as preflight


def _ready_env():
    env = {
        "IBKR_API_BASE_URL": "https://127.0.0.1:5000/v1/api",
        "IBKR_ACCOUNT_ID": "DU123456",
        "IBKR_VERIFY_TLS": "true",
        "IBKR_TIMEOUT_SEC": "10",
        "IBKR_ORDER_TIF": "DAY",
        "IBKR_LISTING_EXCHANGE": "SMART",
        "IBKR_REPLY_CONFIRM_LIMIT": "3",
    }
    for symbol in preflight.PRODUCTION_ASSETS:
        env[f"IBKR_CONID_{symbol}"] = f"conid-{symbol}"
    return env


def test_missing_required_envs_are_not_ready(tmp_path):
    report = preflight.run_ibkr_readonly_preflight(
        output_dir=tmp_path,
        environ={},
    )

    assert report["status"] == "NOT_READY"
    assert report["readonly"] is True
    assert report["trading_disabled"] is True
    assert report["live_leverage_approved"] is False
    assert report["human_review_required"] is True
    assert report["blockers"]
    assert (tmp_path / "latest_ibkr_readonly_preflight.json").exists()


def test_dangerous_execution_gates_fail_closed(tmp_path):
    env = _ready_env()
    env["XIRANG_ENABLE_LIVE_CORE_EXECUTION"] = "1"
    env["IBKR_ENABLE_ORDER_SUBMISSION"] = "true"

    report = preflight.run_ibkr_readonly_preflight(
        output_dir=tmp_path,
        environ=env,
    )

    assert report["status"] == "FAIL_CLOSED"
    assert report["connection"]["attempted"] is False
    assert any("must fail closed" in blocker for blocker in report["blockers"])


def test_approval_context_fails_closed_for_readonly_preflight(tmp_path):
    env = _ready_env()
    env["XIRANG_LIVE_CORE_APPROVAL_ID"] = "APPROVED-123"

    report = preflight.run_ibkr_readonly_preflight(
        output_dir=tmp_path,
        environ=env,
    )

    assert report["status"] == "FAIL_CLOSED"
    assert any("approval context" in blocker for blocker in report["blockers"])


def test_ready_env_without_connect_is_ready_for_readonly_connect(tmp_path):
    report = preflight.run_ibkr_readonly_preflight(
        output_dir=tmp_path,
        environ=_ready_env(),
    )

    assert report["status"] == "READY_FOR_READONLY_CONNECT"
    assert report["connection"]["requested"] is False
    assert report["connection"]["attempted"] is False
    assert report["blockers"] == []


def test_missing_conid_mapping_blocks_preflight(tmp_path):
    env = _ready_env()
    del env["IBKR_CONID_QQQ"]

    report = preflight.run_ibkr_readonly_preflight(
        output_dir=tmp_path,
        environ=env,
    )

    assert report["status"] == "NOT_READY"
    assert any("QQQ" in blocker for blocker in report["blockers"])


def test_connect_success_uses_readonly_adapter_without_trading(monkeypatch, tmp_path):
    class FakeAdapter:
        broker_name = "ibkr"

        def __init__(self):
            self.place_order_called = False
            self.cancel_order_called = False

        def connect(self):
            return True

        def place_order(self, order):
            self.place_order_called = True
            raise AssertionError("preflight must not submit orders")

        def cancel_order(self, order_id):
            self.cancel_order_called = True
            raise AssertionError("preflight must not cancel orders")

    adapter = FakeAdapter()

    def fake_create_broker_adapter(**kwargs):
        assert kwargs["broker_name"] == "ibkr"
        assert kwargs["mode"].value == "read_only"
        assert kwargs["assets"] == list(preflight.PRODUCTION_ASSETS)
        return adapter

    monkeypatch.setattr(preflight, "create_broker_adapter", fake_create_broker_adapter)

    report = preflight.run_ibkr_readonly_preflight(
        output_dir=tmp_path,
        environ=_ready_env(),
        connect=True,
    )

    assert report["status"] == "READY"
    assert report["connection"]["attempted"] is True
    assert report["connection"]["connected"] is True
    assert adapter.place_order_called is False
    assert adapter.cancel_order_called is False


def test_connect_failure_is_attention_not_ready(monkeypatch, tmp_path):
    class DisconnectedAdapter:
        broker_name = "ibkr"

        def connect(self):
            return False

    monkeypatch.setattr(
        preflight,
        "create_broker_adapter",
        lambda **_: DisconnectedAdapter(),
    )

    report = preflight.run_ibkr_readonly_preflight(
        output_dir=tmp_path,
        environ=_ready_env(),
        connect=True,
    )

    assert report["status"] == "ATTENTION"
    assert report["connection"]["attempted"] is True
    assert report["connection"]["connected"] is False
    assert report["warnings"]


def test_connect_skipped_when_static_preflight_not_ready(monkeypatch, tmp_path):
    def fail_if_called(**kwargs):
        raise AssertionError("adapter must not be created when static preflight is blocked")

    monkeypatch.setattr(preflight, "create_broker_adapter", fail_if_called)

    report = preflight.run_ibkr_readonly_preflight(
        output_dir=tmp_path,
        environ={},
        connect=True,
    )

    assert report["status"] == "NOT_READY"
    assert report["connection"]["requested"] is True
    assert report["connection"]["attempted"] is False
    assert "skipped" in " ".join(report["warnings"])
