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
  HUNTFLOW_API         базовый URL API, если стандартные не подошли (опционально)
"""

import json
import os
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import requests

# Кандидаты базового URL API: скрипт проверит их по очереди и возьмет
# первый рабочий. Если поддержка ХФ дала свой адрес, задай его
# переменной окружения HUNTFLOW_API в update.yml, он проверится первым.
API_CANDIDATES = [
    os.environ.get("HUNTFLOW_API"),
    "https://api.huntflow.ru/v2",
    "https://api.huntflow.ru/latest",
    "https://api.huntflow.ru",
    "https://api.huntflow.ai/v2",
]
API = ""  # выбирается автоматически в choose_base()
TOKEN = (os.environ.get("HUNTFLOW_TOKEN") or "").strip()
CACHE_FILE = Path("cache.json")   # кэш логов, чтобы не перекачивать всё каждый час
OUT_FILE = Path("docs/data.json")
PAGE_SIZE = 30                    # у эндпоинта кандидатов лимит 30 на страницу
RPS_DELAY = 0.1                   # пауза между запросами, лимиты ХФ позволяют

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
    "Исп. срок пройден":     "other",
    "Принятие решения":      "other",
}

# ---------------------------------------------------------------------
# Привязка вакансий к рекрутерам. По умолчанию скрипт сам определяет
# владельца вакансии: кто чаще всех двигал кандидатов на рекрутерских
# этапах (добавление, скрининг, интервью). Если кого-то определило
# неправильно, впиши вакансию сюда руками, имя точно как в ХФ:
VACANCY_RECRUITER_OVERRIDES = {
    # "Senior Backend (PHP)": "Имя Фамилия",
}

# ---------------------------------------------------------------------
# В дашборд попадают только эти рекрутеры. Совпадение по началу любого
# слова в имени пользователя ХФ, без учета регистра, чтобы не зависеть
# от точного написания ФИО. Наняли нового рекрутера: допиши сюда.
RECRUITER_WHITELIST = ["сон", "соф", "семён", "семен", "александ", "саш"]


def is_recruiter(name: str) -> bool:
    for word in (name or "").lower().split():
        if any(word.startswith(w) for w in RECRUITER_WHITELIST):
            return True
    return False

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


def api_get(path: str, params: dict | None = None, allow_missing: bool = False) -> dict:
    for attempt in range(1, 6):
        r = SESSION.get(API + path, params=params, timeout=30)
        if r.status_code == 401:
            sys.exit("Huntflow ответил 401: токен неверный или истек. "
                     "Проверь секрет HUNTFLOW_TOKEN в настройках репозитория.")
        if r.status_code == 404:
            if allow_missing:
                return {}
            sys.exit(f"Huntflow ответил 404 на {r.url}\n"
                     f"Тело ответа: {r.text[:300]}\n"
                     "Похоже, у API поменялись пути. Скинь этот лог в чат, поправим.")
        if r.status_code == 400:
            sys.exit(f"Huntflow ответил 400 на {r.url}\n"
                     f"Тело ответа: {r.text[:300]}")
        if r.status_code == 429 or r.status_code >= 500:
            wait = 5 * attempt
            print(f"  {r.status_code} по {path}, жду {wait}с и повторяю...")
            time.sleep(wait)
            continue
        r.raise_for_status()
        time.sleep(RPS_DELAY)
        return r.json()
    sys.exit(f"Huntflow стабильно не отвечает по {path}, прерываюсь.")


def paged(path: str, params: dict | None = None, allow_missing: bool = False):
    page = 1
    while True:
        data = api_get(path, {**(params or {}), "count": PAGE_SIZE, "page": page},
                       allow_missing)
        items = data.get("items", [])
        yield from items
        total_pages = data.get("total_pages")
        if total_pages is not None:
            if page >= int(total_pages):
                break
        elif len(items) < PAGE_SIZE:
            break
        page += 1


def choose_base() -> None:
    """Перебирает возможные адреса API и выбирает первый рабочий."""
    global API
    report = []
    for base in [b.rstrip("/") for b in API_CANDIDATES if b]:
        try:
            r = SESSION.get(base + "/accounts", timeout=30)
        except requests.RequestException as exc:
            report.append((base, f"сетевая ошибка: {exc}"))
            continue
        if r.status_code == 200 and '"items"' in r.text:
            items = r.json().get("items", [])
            if items:
                # проверяем, что база понимает пути v2, а не только /accounts
                probe = SESSION.get(
                    f"{base}/accounts/{items[0]['id']}/vacancies/statuses",
                    timeout=30,
                )
                if probe.status_code == 404:
                    report.append((base, "отдает /accounts, но пути v2 не знает"))
                    continue
            API = base
            print(f"Рабочий адрес API: {base}")
            return
        report.append((base, f"{r.status_code}: {r.text[:200]}"))
    print("Ни один адрес API не подошел. Что ответили серверы:")
    for base, msg in report:
        print(f"  {base} -> {msg}")
    print("Подсказка: 401 значит адрес живой, но токен не подошел.")
    sys.exit("Спроси у Службы заботы ХФ базовый URL API для вашего аккаунта "
             "и добавь его переменной HUNTFLOW_API в update.yml, "
             "либо скинь этот лог в чат.")


# ----------------------------- сбор ----------------------------------

def main() -> None:
    if not TOKEN:
        sys.exit("Не задан HUNTFLOW_TOKEN. Добавь секрет в репозиторий "
                 "(Settings, Secrets and variables, Actions).")

    SESSION.headers["Authorization"] = f"Bearer {TOKEN}"
    SESSION.headers["User-Agent"] = "PPM-Recruiting-Dashboard/1.0 (github actions)"
    print(f"Токен на месте, длина {len(TOKEN)} символов")

    choose_base()

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
    print(f"Этапов в справочнике: {len(statuses)}, маппинг:")
    for name, group in sorted(statuses.values(), key=lambda x: (x[1], x[0])):
        print(f"  [{group:9}] {name}")
    unmapped = sorted({name for name, group in statuses.values()
                       if group == "other" and name not in GROUP_OVERRIDES})
    if unmapped:
        print("Этапы без группы (попали в other), при желании добавь их в GROUP_OVERRIDES:")
        for n in unmapped:
            print(f"  - {n}")

    # 3. Вакансии
    vacancies: dict[int, str] = {}
    for v in paged(f"/accounts/{acc_id}/vacancies"):
        vacancies[v["id"]] = v.get("position", f"Вакансия {v['id']}")
    print(f"Вакансий: {len(vacancies)}")

    # 3b. Рекрутеры, назначенные на вакансии прямо в ХФ
    users: dict = {}
    for cw in paged(f"/accounts/{acc_id}/coworkers", allow_missing=True):
        name = cw.get("name") or ""
        if cw.get("id") is not None:
            users[cw["id"]] = name
        if cw.get("member") is not None:
            users.setdefault(cw["member"], name)
    hf_owner: dict[str, str] = {}
    for vid, vname in vacancies.items():
        detail = api_get(f"/accounts/{acc_id}/vacancies/{vid}", allow_missing=True)
        names = []
        for c in detail.get("coworkers") or []:
            if isinstance(c, dict):
                nm = c.get("name") or users.get(c.get("id")) or users.get(c.get("member"))
            else:
                nm = users.get(c)
            if nm:
                names.append(nm)
        wl = [n for n in names if is_recruiter(n)]
        if wl:
            hf_owner[vname] = wl[0]
    print(f"Вакансий с рекрутером, назначенным в ХФ: {len(hf_owner)}")

    # 4. Кандидаты и их логи (с кэшем, чтобы час от часа качать только новое)
    cache: dict = {}
    if CACHE_FILE.exists():
        try:
            cache = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
        except Exception:
            cache = {}
    if cache.get("_format") != 2:
        if cache:
            print("Формат кэша обновился (добавлены названия этапов), качаю логи заново")
        cache = {"_format": 2}

    def save_cache() -> None:
        CACHE_FILE.write_text(json.dumps(cache, ensure_ascii=False), encoding="utf-8")

    events: list[dict] = []
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
                st_name, group = "Новые", "added"
            elif lg_type == "STATUS" and lg.get("status") in statuses:
                st_name, group = statuses[lg["status"]]
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
                "st": st_name,
                "rec": who,
                "vac": vacancies.get(vac_id, f"Вакансия {vac_id}"),
            })

        cache[aid] = {"key": key, "events": evs}
        events.extend(evs)
        if fetched % 50 == 0:
            print(f"  обработано кандидатов: {total}, докачано логов: {fetched}")
        if fetched % 200 == 0:
            save_cache()  # промежуточное сохранение: обрыв не потеряет прогресс

    print(f"Кандидатов всего: {total}, свежих (докачаны логи): {fetched}, событий: {len(events)}")

    # 5. Привязка вакансий к рекрутерам: одна вакансия, один владелец.
    # Приоритет: ручной словарь > рекрутер, назначенный в ХФ > эвристика
    # по активности. Кэш хранит сырые данные, привязку можно менять
    # без перекачивания логов.
    group_of = {name: group for name, group in statuses.values()}
    group_of.setdefault("Новые", "added")

    weights: dict[str, Counter] = {}
    for e in events:
        if e["rec"] == "Система" or not is_recruiter(e["rec"]):
            continue
        if group_of.get(e["st"]) in ("added", "screening", "interview"):
            weights.setdefault(e["vac"], Counter())[e["rec"]] += 1
    owner: dict[str, str] = {
        vac: c.most_common(1)[0][0] for vac, c in weights.items()
    }
    owner.update(hf_owner)
    owner.update(VACANCY_RECRUITER_OVERRIDES)
    print("Привязка вакансий к рекрутерам:")
    for vac in sorted(owner):
        src = ("вручную" if vac in VACANCY_RECRUITER_OVERRIDES
               else "из ХФ" if vac in hf_owner else "по активности")
        print(f"  {vac} -> {owner[vac]} ({src})")
    dropped = sorted({e["vac"] for e in events} - set(owner))
    if dropped:
        print("Вакансии без рекрутера из RECRUITER_WHITELIST, в дашборд не попадут:")
        for vac in dropped:
            print(f"  - {vac}")

    # 6. Сохранение: компактный формат, события ссылаются на справочники
    stages_list = [name for name, _g in statuses.values()]
    if "Новые" not in stages_list:
        stages_list.insert(0, "Новые")
    stage_i = {n: i for i, n in enumerate(stages_list)}
    recs_list = sorted(set(owner.values()))
    rec_i = {n: i for i, n in enumerate(recs_list)}
    vacs_list = sorted(owner)
    vac_i = {n: i for i, n in enumerate(vacs_list)}
    out_events = [
        {"d": e["d"], "r": rec_i[owner[e["vac"]]],
         "v": vac_i[e["vac"]], "s": stage_i[e["st"]]}
        for e in events if e["vac"] in owner and e["st"] in stage_i
    ]

    save_cache()
    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUT_FILE.write_text(json.dumps({
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "account": acc_name,
        "unmapped_statuses": unmapped,
        "stages": stages_list,
        "stage_groups": group_of,
        "recruiters": recs_list,
        "vacancies": vacs_list,
        "events": out_events,
    }, ensure_ascii=False), encoding="utf-8")
    print(f"Готово: {OUT_FILE}")


if __name__ == "__main__":
    main()
