"""
息壤（Xi-Rang）Phase 1 券商只读同步 runner

用法：
    python -m runner.broker_sync --portfolio us
    python -m runner.broker_sync --portfolio us --role backup
    python -m runner.broker_sync --portfolio cn --broker futu
"""

import json
import logging
import sys

from db.database import Database
from engine.execution.sync import BrokerSyncService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("xirang.broker_sync")


def run_broker_sync():
    portfolio_id = "us"
    broker_role = "primary"
    broker_name = None

    if "--portfolio" in sys.argv:
        idx = sys.argv.index("--portfolio")
        if idx + 1 < len(sys.argv):
            portfolio_id = sys.argv[idx + 1]

    if "--role" in sys.argv:
        idx = sys.argv.index("--role")
        if idx + 1 < len(sys.argv):
            broker_role = sys.argv[idx + 1]

    if "--broker" in sys.argv:
        idx = sys.argv.index("--broker")
        if idx + 1 < len(sys.argv):
            broker_name = sys.argv[idx + 1]

    service = BrokerSyncService(Database())
    result = service.sync_portfolio(
        portfolio_id=portfolio_id,
        broker_role=broker_role,
        broker_name=broker_name,
    )
    logger.info("券商同步完成: %s", result)
    return result


if __name__ == "__main__":
    print(json.dumps(run_broker_sync(), ensure_ascii=False, indent=2))
