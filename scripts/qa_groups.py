"""Disjoint test groups shared by local runs and GitHub Actions."""

from pathlib import Path

GROUPS = {
    "contracts": "01 · Контракты сообщений",
    "payment": "02 · Payment API, БД и идемпотентность",
    "execution": "03 · Executor, lease и исполнение",
    "delivery": "04 · Kafka, inbox/outbox и восстановление",
    "reconciliation": "05 · Сверка, вебхуки и версии",
    "channels": "06 · Каналы A/B/C и сквозной поток",
}


def classify(path: Path) -> str:
    if path.parts[0] == "libs":
        return "contracts"
    name = path.stem
    if name in {
        "test_callbacks",
        "test_callback_timestamp",
        "test_confirmation",
        "test_reconciliation",
        "test_refinements",
    }:
        return "reconciliation"
    if name in {"test_extra_channels", "test_multichannel_flow", "test_channel_routing"}:
        return "channels"
    if name.startswith(("test_consumer", "test_publisher", "test_result")) or name in {
        "test_kafka_delivery",
        "test_worker",
    }:
        return "delivery"
    return "payment" if path.parts[1] == "payment" else "execution"


def inventory() -> dict[str, list[str]]:
    groups: dict[str, list[str]] = {key: [] for key in GROUPS}
    for root in ["libs/contracts/tests", "services/payment/tests", "services/executor/tests"]:
        for path in sorted(Path(root).rglob("test_*.py")):
            groups[classify(path)].append(str(path))
    return groups
