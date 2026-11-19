"""Create a curator-readable summary, validating every expected group's evidence."""

import os
from pathlib import Path
from xml.etree import ElementTree as ET

from qa_groups import GROUPS

lines = [
    "# Итог QA",
    "",
    f"Коммит: `{os.getenv('GITHUB_SHA', 'local')}`",
    "",
    "| Группа | Тестов | Ошибок | Пропусков | Итог |",
    "|---|---:|---:|---:|---|",
]
valid = True
total = 0
for key, label in GROUPS.items():
    path = Path("reports") / (key + ".xml")
    if not path.exists():
        valid = False
        lines.append(f"| {label} | — | — | — | НЕ ВЫПОЛНЕНО |")
        continue
    suites = list(ET.parse(path).getroot().iter("testsuite"))
    counts = {
        k: sum(int(s.get(k, "0")) for s in suites)
        for k in ["tests", "failures", "errors", "skipped"]
    }
    errors = counts["failures"] + counts["errors"]
    ok = counts["tests"] > 0 and not errors and not counts["skipped"]
    valid &= ok
    total += counts["tests"]
    lines.append(
        f"| {label} | {counts['tests']} | {errors} | {counts['skipped']} | "
        + ("PASS" if ok else "FAIL")
        + " |"
    )
lines += [
    "",
    f"Всего тестов: **{total}**. " + ("Все группы прошли." if valid else "Приёмка не пройдена."),
    "",
    "JUnit XML и полные логи доступны в артефакте qa-results этого запуска.",
    "Интеграционные проверки используют PostgreSQL и Kafka. "
    "Виртуальные каналы: реальных списаний нет.",
]
text = "\n".join(lines) + "\n"
Path("reports").mkdir(exist_ok=True)
Path("reports/summary.md").write_text(text)
if target := os.getenv("GITHUB_STEP_SUMMARY"):
    with open(target, "a") as stream:
        stream.write(text)
print(text)
raise SystemExit(0 if valid else 1)
