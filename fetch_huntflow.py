#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Сборщик данных для дашборда подбора PPM.

Ходит в Huntflow API v2, собирает переходы кандидатов по этапам
и кладет агрегат в docs/data.json. Персональных данных кандидатов
в выгрузке нет: только даты переходов, группа этапа, кто из
рекрутеров сделал действие и название вакансии.

Запуск (в GitHub Actions это делается автоматически):
  HUNTFLOW_TOKEN=xxx python fetch_huntflow.py

Переменные окружения:
  HUNTFLOW_TOKEN       персональный API-токен (обязательно)
  HUNTFLOW_ACCOUNT_ID  id организации, если их несколько (опционально)
"""

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

API = os.environ.get("HUNTFLOW_API", "https://api.huntflow.ru/v2")
TOKEN = os.environ.get("HUNTFLOW_TOKEN")
CACHE_FILE = Path("cache.json")   # кэш логов, чтобы не перекачивать всё каждый час
OUT_FILE = Path("docs/data.json")
PAGE_SIZE = 50
RPS_DELAY = 0.25                  # пауза между запросами, чтобы не ловить лимиты

# ---------------------------------------------------------------------
# Маппинг этапов на группы воронки.
# Группы: added, screening, interview, offer, hired, rejected, other.
# Все этапы PPM замаплены явно. Если в ХФ появится новый этап,
# скрипт попробует угадать по ключевым словам, а дашборд подсветит
# нераспознанные, тогда просто допиши строку сюда.
GROUP_OVERRIDES = {
    "Новые":                 "added",
    "Отправлено сообщение":  "screening",
    "Оценка рекрутером":     "screening",
    "Интервью с рекрутером": "interview",
    "Техническое интервью":  "interview",
    "Тестовое задание":      "other",
    "Оценка заказчиком":     "other",
    "Platform Test":         "interview",
    "System Design":         "interview",
    "Интервью с заказчиком": "interview",
    "Финальное интервью":    "interview",
    "Проверка СБ":           "other",
    "Выставлен оффер":       "offer",
    "Оффер принят":          "hired",
    "Отказ":                 "rejected",
}

KEYWORDS = [
    ("hired",     ["вышел", "нанят", "оформ", "hired"]),
    ("offer",     ["оффер", "offer"]),
    ("interview", ["интервью", "собес", "встреча", "техничес", "финал"]),
    ("screening", ["скрининг", "резюме", "рассмотрен", "screening", "отклик"]),
    ("rejected",  ["отказ", "резерв", "reject"]),
]


def classify(name: str, hf_type: str) -> str:
    if name in GROUP_OVERRIDES:
        return GROUP_OVERRIDES[name]
    if hf_type == "hired":
        return "hired"
    if hf_type == "trash":
        return "rejected"
    low = (name or "").lower()
    for group, words in KEYWORDS:
        if any(w in low for w in words):
            return group
    return "other"


# ------------------------- HTTP-обвязка ------------------------------

SESSION = requests.Session()


def api_get(path: str, params: dict | None = None) -> dict:
    for attempt in range(1, 6):
        r = SESSION.get(API + path, params=params, timeout=30)
        if r.status_code == 401:
            sys.exit("Huntflow ответил 401: токен неверный или истек. "
                     "Проверь секрет HUNTFLOW_TOKEN в настройках репозитория.")
        if r.status_code == 429 or r.status_code >= 500:
            wait = 5 * attempt
            print(f"  {r.status_code} по {path}, жду {wait}с и повторяю...")
            time.sleep(wait)
            continue
        r.raise_for_status()
        time.sleep(RPS_DELAY)
        return r.json()
    sys.exit(f"Huntflow стабильно не отвечает по {path}, прерываюсь.")


def paged(path: str, params: dict | None = None):
    page = 1
    while True:
        data = api_get(path, {**(params or {}), "count": PAGE_SIZE, "page": page})
        items = data.get("items", [])
        yield from items
        total_pages = data.get("total_pages")
        if total_pages is not None:
            if page >= int(total_pages):
                break
        elif len(items) < PAGE_SIZE:
            break
        page += 1


# ----------------------------- сбор ----------------------------------

def main() -> None:
    if not TOKEN:
        sys.exit("Не задан HUNTFLOW_TOKEN. Добавь секрет в репозиторий "
                 "(Settings, Secrets and variables, Actions).")

    SESSION.headers["Authorization"] = f"Bearer {TOKEN}"

    # 1. Организация
    accounts = api_get("/accounts").get("items", [])
    if not accounts:
        sys.exit("Токен валиден, но организаций не видно. Спроси поддержку ХФ.")
    acc_env = os.environ.get("HUNTFLOW_ACCOUNT_ID")
    if acc_env:
        acc_id = int(acc_env)
        acc_name = next((a.get("name", "") for a in accounts if a["id"] == acc_id), str(acc_id))
    else:
        acc_id = accounts[0]["id"]
        acc_name = accounts[0].get("name", "")
        if len(accounts) > 1:
            print("Организаций несколько, беру первую. Доступные:")
            for a in accounts:
                print(f"  id={a['id']}  {a.get('name','')}")
            print("Чтобы выбрать другую, задай переменную HUNTFLOW_ACCOUNT_ID.")
    print(f"Организация: {acc_name} (id={acc_id})")

    # 2. Справочник этапов
    statuses: dict[int, tuple[str, str]] = {}
    for st in paged(f"/accounts/{acc_id}/vacancies/statuses"):
        group = classify(st.get("name", ""), st.get("type", ""))
        statuses[st["id"]] = (st.get("name", ""), group)
    print(f"Этапов в справочнике: {len(statuses)}")
    unmapped = sorted({name for name, group in statuses.values() if group == "other"})
    if unmapped:
        print("Этапы без группы (попали в other), при желании добавь их в GROUP_OVERRIDES:")
        for n in unmapped:
            print(f"  - {n}")

    # 3. Вакансии
    vacancies: dict[int, str] = {}
    for v in paged(f"/accounts/{acc_id}/vacancies"):
        vacancies[v["id"]] = v.get("position", f"Вакансия {v['id']}")
    print(f"Вакансий: {len(vacancies)}")

    # 4. Кандидаты и их логи (с кэшем, чтобы час от часа качать только новое)
    cache: dict = {}
    if CACHE_FILE.exists():
        try:
            cache = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
        except Exception:
            cache = {}

    events: list[dict] = []
    new_cache: dict = {}
    total = fetched = 0

    for a in paged(f"/accounts/{acc_id}/applicants"):
        total += 1
        aid = str(a["id"])
        links = a.get("links") or []
        stamp = max(
            [str(l.get("updated") or l.get("changed") or "") for l in links] +
            [str(a.get("created") or "")]
        )
        key = f"{len(links)}:{stamp}"

        cached = cache.get(aid)
        if cached and cached.get("key") == key:
            new_cache[aid] = cached
            events.extend(cached.get("events", []))
            continue

        fetched += 1
        evs: list[dict] = []
        seen_add: set[int] = set()
        for lg in paged(f"/accounts/{acc_id}/applicants/{a['id']}/logs"):
            vac_id = lg.get("vacancy")
            if not vac_id:
                continue
            date = str(lg.get("created") or "")[:10]
            if not date:
                continue
            who = (lg.get("account_info") or {}).get("name") or "Система"
            lg_type = lg.get("type")

            if lg_type == "ADD":
                group = "added"
            elif lg_type == "STATUS" and lg.get("status") in statuses:
                group = statuses[lg["status"]][1]
                if group == "other":
                    continue
            else:
                continue

            # кандидат мог попасть на вакансию и через ADD, и через
            # статус "Новые", считаем его новым только один раз
            if group == "added":
                if vac_id in seen_add:
                    continue
                seen_add.add(vac_id)

            evs.append({
                "d": date,
                "g": group,
                "rec": who,
                "vac": vacancies.get(vac_id, f"Вакансия {vac_id}"),
            })

        new_cache[aid] = {"key": key, "events": evs}
        events.extend(evs)
        if fetched % 50 == 0:
            print(f"  обработано кандидатов: {total}, докачано логов: {fetched}")

    print(f"Кандидатов всего: {total}, свежих (докачаны логи): {fetched}, событий: {len(events)}")

    # 5. Сохранение
    CACHE_FILE.write_text(json.dumps(new_cache, ensure_ascii=False), encoding="utf-8")
    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUT_FILE.write_text(json.dumps({
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "account": acc_name,
        "unmapped_statuses": unmapped,
        "events": events,
    }, ensure_ascii=False), encoding="utf-8")
    print(f"Готово: {OUT_FILE}")


if __name__ == "__main__":
    main()
