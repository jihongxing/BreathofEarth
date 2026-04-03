"""Week 9-12 综合验证：Alpha 沙盒 + 策略竞技场"""
import json
import urllib.request
import urllib.error

BASE = "http://127.0.0.1:8000"


def req(method, path, data=None, token=None):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    body = json.dumps(data).encode() if data else None
    r = urllib.request.Request(f"{BASE}{path}", data=body, headers=headers, method=method)
    resp = urllib.request.urlopen(r)
    ct = resp.headers.get("Content-Type", "")
    raw = resp.read()
    return json.loads(raw) if "json" in ct else raw.decode()


def test_all():
    # 1. Login
    result = req("POST", "/api/auth/login", {"username": "admin", "password": "xirang2026"})
    token = result["access_token"]
    print("1. Login OK")

    # 2. Get strategies (auto-register)
    strategies = req("GET", "/api/alpha/strategies", token=token)
    assert len(strategies) >= 1
    cc = [s for s in strategies if s["id"] == "covered_call"][0]
    assert cc["status"] in ("DISABLED", "ENABLED", "SUSPENDED")
    print(f"2. Strategies: {len(strategies)}, covered_call status={cc['status']}")

    # 3. Disabled strategy cannot be run
    # First ensure it's disabled
    req("POST", "/api/alpha/strategies/covered_call/toggle",
        {"action": "disable"}, token=token)
    try:
        req("POST", "/api/alpha/strategies/covered_call/run", token=token)
        assert False, "Should have raised"
    except urllib.error.HTTPError as e:
        assert e.code == 400
    print("3. Disabled strategy run correctly rejected")

    # 4. Enable strategy
    result = req("POST", "/api/alpha/strategies/covered_call/toggle",
                 {"action": "enable", "allocation_pct": 0.10}, token=token)
    assert result["status"] == "ENABLED"
    print("4. Strategy enabled OK")

    # 5. Verify enabled
    strategies = req("GET", "/api/alpha/strategies", token=token)
    cc = [s for s in strategies if s["id"] == "covered_call"][0]
    assert cc["status"] == "ENABLED"
    print("5. Status confirmed ENABLED")

    # 6. Run strategy
    result = req("POST", "/api/alpha/strategies/covered_call/run?spy_price=450", token=token)
    print(f"6. Strategy run: action={result.get('action')}")

    # 7. Check transactions
    txs = req("GET", "/api/alpha/strategies/covered_call/transactions", token=token)
    print(f"7. Transactions: {len(txs)} records")

    # 8. Strategy detail
    detail = req("GET", "/api/alpha/strategies/covered_call", token=token)
    assert detail["id"] == "covered_call"
    print(f"8. Detail: status={detail['status']}, trades={detail['trade_count']}")

    # ── 竞技场测试 ──────────────────────────────────

    # 9. Leaderboard
    board = req("GET", "/api/alpha/arena/leaderboard", token=token)
    assert isinstance(board, list)
    assert len(board) >= 1
    assert board[0]["rank"] == 1
    print(f"9. Leaderboard: {len(board)} strategies, #1={board[0]['name']}")

    # 10. Run all strategies
    result = req("POST", "/api/alpha/arena/run-all?spy_price=450", token=token)
    assert "strategies_run" in result
    print(f"10. Run all: {result['strategies_run']} strategies executed")

    # 11. Quarterly evaluation
    report = req("POST", "/api/alpha/arena/evaluate", token=token)
    assert "evaluations" in report
    assert "summary" in report
    print(f"11. Evaluation: {report['summary']}")

    # 12. Disable and verify
    req("POST", "/api/alpha/strategies/covered_call/toggle",
        {"action": "disable"}, token=token)
    detail = req("GET", "/api/alpha/strategies/covered_call", token=token)
    assert detail["status"] == "DISABLED"
    print("12. Strategy disabled OK")

    # 13. Audit log check
    audit = req("GET", "/api/governance/audit-log", token=token)
    alpha_events = [a for a in audit if "ALPHA" in a["action"] or "ARENA" in a["action"]]
    assert len(alpha_events) >= 2
    print(f"13. Audit log: {len(alpha_events)} alpha/arena events")

    # 14. Frontend checks
    html = req("GET", "/")
    assert "tab-alpha" in html
    assert "达尔文沙盒" in html
    assert "arena-leaderboard" in html
    assert "arena-controls" in html
    print("14. Frontend HTML has alpha tab + arena elements")

    js = req("GET", "/static/app.js")
    assert "loadAlpha" in js
    assert "loadArenaLeaderboard" in js
    assert "btn-arena-evaluate" in js
    print("15. Frontend JS has all alpha/arena functions")

    print("\n=== ALL 15 CHECKS PASSED ===")


if __name__ == "__main__":
    test_all()
