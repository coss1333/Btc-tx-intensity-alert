#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BTC High-Value Transaction Intensity Monitor -> Telegram alerts
---------------------------------------------------------------
Мониторит мемпул Bitcoin и отправляет сигнал в Telegram, когда
интенсивность "дорогих" транзакций (>= EXPENSIVE_THRESHOLD_BTC)
резко возрастает относительно скользящего среднего/стандартного отклонения.
Кроме того, в тексте сигнала показывается, сколько транзакций
с ЧРЕЗВЫЧАЙНО большим объёмом (>= LARGE_THRESHOLD_BTC) было замечено
за текущий временной интервал.

Источник данных по умолчанию: публичный Esplora API (mempool.space / blockstream.info совместим).
Без API‑ключей, но соблюдайте умеренную частоту запросов.
"""
import os
import time
import math
import json
import signal
import logging
import requests
from collections import deque, defaultdict
from datetime import datetime, timezone

# ---------------------- Конфигурация ----------------------
ESPLORA_BASE = os.getenv("ESPLORA_BASE", "https://mempool.space/api")
POLL_INTERVAL_SEC = int(os.getenv("POLL_INTERVAL_SEC", "20"))   # как часто опрашивать мемпул
WINDOW_SEC = int(os.getenv("WINDOW_SEC", "300"))                 # ширина окна (например, 5 минут)
HISTORY_WINDOWS = int(os.getenv("HISTORY_WINDOWS", "24"))        # сколько окон в истории для статистики (например, последние 2 часа при 5‑мин окне)
EXPENSIVE_THRESHOLD_BTC = float(os.getenv("EXPENSIVE_THRESHOLD_BTC", "0.1"))   # "дорогая" транзакция (для спайк‑метрики)
LARGE_THRESHOLD_BTC = float(os.getenv("LARGE_THRESHOLD_BTC", "0.5"))          # "очень большая" транзакция (для витринного счётчика в алерте)
Z_SCORE_TRIGGER = float(os.getenv("Z_SCORE_TRIGGER", "3.0"))    # на сколько σ текущая метрика должна превысить среднее
MIN_EXPENSIVE_COUNT = int(os.getenv("MIN_EXPENSIVE_COUNT", "5"))# минимальное число "дорогих" транз за окно, чтобы рассматривать как событие

# Telegram
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TELEGRAM_SILENT = os.getenv("TELEGRAM_SILENT", "false").lower() == "true"  # disable_notification

# Прочее
REQUEST_TIMEOUT = 15
USER_AGENT = "btc-intensity-monitor/1.0 (+https://mempool.space)"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO), format="%(asctime)s %(levelname)s: %(message)s")

# ---------------------- Утилиты ----------------------
def sat_to_btc(sat: int) -> float:
    return sat / 100_000_000.0

def send_telegram(msg: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logging.warning("TELEGRAM_BOT_TOKEN/CHAT_ID не заданы — алерты в чат не будут отправляться.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": msg,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
        "disable_notification": TELEGRAM_SILENT,
    }
    try:
        r = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT, headers={"User-Agent": USER_AGENT})
        if r.status_code != 200:
            logging.error("Ошибка отправки в Telegram: %s %s", r.status_code, r.text)
    except Exception as e:
        logging.exception("Исключение при отправке в Telegram: %s", e)

def fetch_recent_mempool_txids(limit: int = 200):
    """
    Получает список txid из мемпула (самые свежие первые).
    Esplora: /mempool/txids — может вернуть очень много txid.
    Мы берем первые `limit` для экономии запросов.
    """
    url = f"{ESPLORA_BASE}/mempool/txids"
    r = requests.get(url, timeout=REQUEST_TIMEOUT, headers={"User-Agent": USER_AGENT})
    r.raise_for_status()
    txids = r.json()
    if not isinstance(txids, list):
        return []
    return txids[:limit]

def fetch_tx_details(txid: str) -> dict:
    """
    Детали транзакции: /tx/:txid
    Возвращает JSON Esplora.
    """
    url = f"{ESPLORA_BASE}/tx/{txid}"
    r = requests.get(url, timeout=REQUEST_TIMEOUT, headers={"User-Agent": USER_AGENT})
    r.raise_for_status()
    return r.json()

def extract_outputs_btc(tx_json: dict) -> list:
    """
    Возвращает список сумм всех выводов (vout.value) в BTC.
    Esplora значения в сатошах: vout[i]["value"]
    """
    outs = []
    for vout in tx_json.get("vout", []):
        val_sat = vout.get("value", 0)
        outs.append(sat_to_btc(val_sat))
    return outs

def compute_window_metrics(txids: list, expensive_thr: float, large_thr: float) -> dict:
    """
    Считает метрики по набору txid:
      - count_expensive: количество транзакций, где ХОТЯ БЫ ОДИН вывод >= expensive_thr
      - volume_expensive: сумма по всем выводам >= expensive_thr (в BTC)
      - count_large: количество транзакций, где ХОТЯ БЫ ОДИН вывод >= large_thr
    Внимание: это эвристика "по выводам". Не пытаемся отделить изменение/чейндж.
    """
    count_expensive = 0
    volume_expensive = 0.0
    count_large = 0
    inspected = 0

    for txid in txids:
        try:
            tx = fetch_tx_details(txid)
            outs_btc = extract_outputs_btc(tx)
            # есть ли дорогие/большие выводы
            expensive_outs = [x for x in outs_btc if x >= expensive_thr]
            large_outs = [x for x in outs_btc if x >= large_thr]

            if expensive_outs:
                count_expensive += 1
                volume_expensive += sum(expensive_outs)
            if large_outs:
                count_large += 1
            inspected += 1
        except Exception as e:
            logging.debug("Проблема при обработке %s: %s", txid, e)
            continue

    return {
        "inspected": inspected,
        "count_expensive": count_expensive,
        "volume_expensive": volume_expensive,
        "count_large": count_large,
    }

def mean_std(values):
    if not values:
        return (0.0, 0.0)
    m = sum(values) / len(values)
    var = sum((x - m) ** 2 for x in values) / max(1, len(values) - 1)
    return (m, math.sqrt(var))

def format_alert(ts_utc: float, metrics: dict, baseline: dict, window_sec: int) -> str:
    ts = datetime.fromtimestamp(ts_utc, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    msg = (
        f"⚡️ <b>BTC: всплеск интенсивности дорогих транзакций</b>\n"
        f"Временное окно: <b>{window_sec//60} мин</b> • {ts}\n"
        f"— Транзакций с выводом ≥ {EXPENSIVE_THRESHOLD_BTC:.4f} BTC: <b>{metrics['count_expensive']}</b>\n"
        f"— Объём таких выводов (сумма): <b>{metrics['volume_expensive']:.4f} BTC</b>\n"
        f"— Транзакций с выводом ≥ {LARGE_THRESHOLD_BTC:.4f} BTC: <b>{metrics['count_large']}</b>\n"
        f"\n"
        f"База (по последним {HISTORY_WINDOWS} окнам ~ {HISTORY_WINDOWS*(window_sec//60)} мин):\n"
        f"— Среднее кол-во: <b>{baseline['mean_cnt']:.2f}</b> (σ={baseline['std_cnt']:.2f})\n"
        f"— Средний объём: <b>{baseline['mean_vol']:.4f}</b> BTC (σ={baseline['std_vol']:.4f})\n"
        f"\n"
        f"Порог срабатывания: Z ≥ {Z_SCORE_TRIGGER:.1f}, наблюдаемое:\n"
        f"— Z(count) = <b>{baseline['z_cnt']:.2f}</b>\n"
        f"— Z(volume) = <b>{baseline['z_vol']:.2f}</b>\n"
        f"\n"
        f"#bitcoin #mempool #onchain"
    )
    return msg

def graceful_exit(signum, frame):
    logging.info("Получен сигнал %s — корректное завершение.", signum)
    raise SystemExit(0)

for sig in (signal.SIGINT, signal.SIGTERM):
    signal.signal(sig, graceful_exit)

def main():
    logging.info("Старт мониторинга. Источник: %s", ESPLORA_BASE)
    history_counts = deque(maxlen=HISTORY_WINDOWS)
    history_volumes = deque(maxlen=HISTORY_WINDOWS)

    # Скользящее окно собираем батчом txid каждые POLL_INTERVAL_SEC.
    # Простейший метод: каждый тик берём N свежих txid и аккумулируем в окне.
    # Чтобы приблизить длительность окна, считаем сколько тиков укладывается в окно.
    ticks_per_window = max(1, WINDOW_SEC // POLL_INTERVAL_SEC)
    logging.info("Интервал опроса: %ss, окно: %ss (%d тиков), история: %d окон",
                 POLL_INTERVAL_SEC, WINDOW_SEC, ticks_per_window, HISTORY_WINDOWS)

    window_txids = deque()  # буфер txid, собранных в текущем окне
    last_alert_ts = 0

    while True:
        try:
            txids_batch = fetch_recent_mempool_txids(limit=200)
            # Добавим уникальности: не хотим один и тот же txid много раз.
            # Простой способ — хранить только первые K, а окно складывать из последних батчей.
            window_txids.append(txids_batch)

            # Когда набрали достаточно тиков, закрываем окно и считаем метрики
            if len(window_txids) >= ticks_per_window:
                # собрать уникальные txid из буфера (свежие сверху)
                flat = []
                seen = set()
                for batch in window_txids:
                    for txid in batch:
                        if txid not in seen:
                            seen.add(txid)
                            flat.append(txid)
                # ограничим количество детальных запросов, чтобы не ддосить API
                # Возьмем первые M самых свежих на анализ
                MAX_TX_ANALYZE = int(os.getenv("MAX_TX_ANALYZE", "400"))
                analyze_txids = flat[:MAX_TX_ANALYZE]

                metrics = compute_window_metrics(analyze_txids, EXPENSIVE_THRESHOLD_BTC, LARGE_THRESHOLD_BTC)
                history_counts.append(metrics["count_expensive"])
                history_volumes.append(metrics["volume_expensive"])

                mean_cnt, std_cnt = mean_std(list(history_counts)[:-1] if len(history_counts) > 1 else list(history_counts))
                mean_vol, std_vol = mean_std(list(history_volumes)[:-1] if len(history_volumes) > 1 else list(history_volumes))

                # z-score текущего окна по count/volume (если std == 0, то z = inf при превышении среднего)
                def z_score(current, mean, std):
                    if std == 0:
                        return float("inf") if current > mean and mean > 0 else (0.0 if current == mean else float("inf") if mean == 0 and current > 0 else -float("inf"))
                    return (current - mean) / std

                z_cnt = z_score(metrics["count_expensive"], mean_cnt, std_cnt)
                z_vol = z_score(metrics["volume_expensive"], mean_vol, std_vol)

                baseline = {
                    "mean_cnt": mean_cnt or 0.0,
                    "std_cnt": std_cnt or 0.0,
                    "mean_vol": mean_vol or 0.0,
                    "std_vol": std_vol or 0.0,
                    "z_cnt": z_cnt,
                    "z_vol": z_vol,
                }

                now_ts = time.time()
                should_alert = (
                    metrics["count_expensive"] >= MIN_EXPENSIVE_COUNT and
                    (z_cnt >= Z_SCORE_TRIGGER or z_vol >= Z_SCORE_TRIGGER)
                )

                logging.info("Окно готово: inspected=%d, cnt_exp=%d, vol_exp=%.4f, cnt_large=%d, z_cnt=%.2f, z_vol=%.2f",
                             metrics["inspected"], metrics["count_expensive"], metrics["volume_expensive"],
                             metrics["count_large"], z_cnt, z_vol)

                if should_alert and now_ts - last_alert_ts >= WINDOW_SEC:  # антифлуд: не чаще, чем раз в окно
                    msg = format_alert(now_ts, metrics, baseline, WINDOW_SEC)
                    send_telegram(msg)
                    last_alert_ts = now_ts

                # очистить текущее окно
                window_txids.clear()

            time.sleep(POLL_INTERVAL_SEC)
        except KeyboardInterrupt:
            break
        except SystemExit:
            break
        except Exception as e:
            logging.exception("Неожиданная ошибка: %s", e)
            time.sleep(5)

if __name__ == "__main__":
    main()
