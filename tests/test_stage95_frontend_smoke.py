from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
FRONTEND = ROOT / "frontend"


def _text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _function_body(source: str, name: str, next_name: str) -> str:
    start = source.index(f"function {name}")
    end = source.index(f"function {next_name}", start)
    return source[start:end]


def test_stage95_frontend_requests_and_renders_admission_gate():
    app_js = _text(FRONTEND / "app.js")

    assert 'api("/api/stage95-admission/" + currentPortfolio)' in app_js
    assert 'api("/api/multi-strategy-shadow/" + currentPortfolio)' in app_js
    assert 'api("/api/ibkr-readonly-preflight/" + currentPortfolio)' in app_js
    assert "buildStage95AdmissionErrorPayload" in app_js
    assert "buildMultiStrategyShadowErrorPayload" in app_js
    assert "buildIbkrReadonlyPreflightErrorPayload" in app_js
    assert (
        "renderStage95ShadowAudit(shadowAudit, stage95Summary, stage95Admission, "
        "d.currency, multiStrategyShadow, ibkrReadonlyPreflight)"
    ) in app_js
    assert "renderStage95AdmissionGate(admissionData)" in app_js
    assert "renderIbkrReadonlyPreflightPanel(ibkrReadonlyPreflightData)" in app_js
    assert "renderMultiStrategyShadowPanel(multiStrategyData, currencySymbol)" in app_js


def test_stage95_admission_gate_is_readonly_no_control_surface():
    app_js = _text(FRONTEND / "app.js")
    body = _function_body(app_js, "renderStage95AdmissionGate", "renderStage95ObservationSummary")

    assert "NOT_APPROVED" in body
    assert "READY_FOR_HUMAN_REVIEW" in body
    assert "stage95AdmissionBlockers" in body
    assert "stage95AdmissionReadonlyHint" in body

    forbidden_fragments = [
        "<button",
        "<input",
        "<select",
        "<textarea",
        "onclick",
        "href=",
        "place_order",
        "cancel_order",
    ]
    for fragment in forbidden_fragments:
        assert fragment not in body


def test_stage95_admission_i18n_and_styles_are_present():
    i18n_js = _text(FRONTEND / "i18n.js")
    style_css = _text(FRONTEND / "style.css")

    required_i18n_keys = [
        "stage95AdmissionTitle",
        "stage95AdmissionStatus",
        "stage95AdmissionNotApproved",
        "stage95ReadyForHumanReview",
        "stage95AdmissionBlockers",
        "stage95AdmissionReadonlyHint",
    ]
    for key in required_i18n_keys:
        assert i18n_js.count(key) >= 2

    assert ".stage95-admission" in style_css
    assert ".stage95-section.level-warning" in style_css


def test_multi_strategy_shadow_panel_is_readonly_no_control_surface():
    app_js = _text(FRONTEND / "app.js")
    body = _function_body(app_js, "renderMultiStrategyShadowPanel", "renderStage95AdmissionGate")

    assert "multiStrategyTitle" in body
    assert "NOT_APPROVED" in body
    assert "live_leverage_approved" in body
    assert "multiStrategyReadonlyHint" in body

    forbidden_fragments = [
        "<button",
        "<input",
        "<select",
        "<textarea",
        "onclick",
        "href=",
        "place_order",
        "cancel_order",
        "create_broker_adapter",
    ]
    for fragment in forbidden_fragments:
        assert fragment not in body


def test_multi_strategy_shadow_i18n_and_styles_are_present():
    i18n_js = _text(FRONTEND / "i18n.js")
    style_css = _text(FRONTEND / "style.css")

    required_i18n_keys = [
        "multiStrategyTitle",
        "multiStrategyStatus",
        "multiStrategyCount",
        "multiStrategyAdmission",
        "multiStrategyMissing",
        "multiStrategyApiUnavailable",
        "multiStrategyReadonlyHint",
    ]
    for key in required_i18n_keys:
        assert i18n_js.count(key) >= 2

    assert ".multi-strategy-panel" in style_css
    assert ".multi-strategy-table" in style_css


def test_ibkr_readonly_preflight_panel_is_readonly_no_control_surface():
    app_js = _text(FRONTEND / "app.js")
    body = _function_body(app_js, "renderIbkrReadonlyPreflightPanel", "renderMultiStrategyShadowPanel")

    assert "ibkrPreflightTitle" in body
    assert "ibkrPreflightReadonlyHint" in body
    assert "live_leverage_approved" in body
    assert "connection.connected" in body

    forbidden_fragments = [
        "<button",
        "<input",
        "<select",
        "<textarea",
        "onclick",
        "href=",
        "place_order",
        "cancel_order",
        "create_broker_adapter",
        "IBKR_ENABLE_ORDER_SUBMISSION",
    ]
    for fragment in forbidden_fragments:
        assert fragment not in body


def test_ibkr_readonly_preflight_i18n_and_styles_are_present():
    i18n_js = _text(FRONTEND / "i18n.js")
    style_css = _text(FRONTEND / "style.css")

    required_i18n_keys = [
        "ibkrPreflightTitle",
        "ibkrPreflightMissing",
        "ibkrPreflightApiUnavailable",
        "ibkrPreflightConnection",
        "ibkrPreflightAttempted",
        "ibkrPreflightReadonlyHint",
    ]
    for key in required_i18n_keys:
        assert i18n_js.count(key) >= 2

    assert ".ibkr-preflight-panel" in style_css
