#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from aiogram import Bot, Dispatcher, Router
    from aiogram.exceptions import TelegramUnauthorizedError
    from aiogram.filters import Command
    from aiogram.types import Message
except ModuleNotFoundError as e:
    raise SystemExit(
        "aiogram is not installed. Install with: "
        "python3 -m pip install -r DataBase/requirements-bot.txt"
    ) from e


@dataclass
class Config:
    token: str
    chat_id: int
    ops_log: Path
    links_csv: Path
    work_dir: Path
    state_file: Path
    poll_seconds: float


router = Router()
CTX: dict[str, Any] = {}


IMPORTANT_ACTIONS = {
    'run_start',
    'auto_merge',
    'final_merge',
    'worker_failed',
    'interrupted',
    'parse_start',
    'parse_done',
    'parse_failed',
    'run_finish',
}


def load_dotenv(dotenv_path: Path) -> None:
    if not dotenv_path.exists():
        return
    for raw in dotenv_path.read_text(encoding='utf-8').splitlines():
        line = raw.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        key, value = line.split('=', 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def append_ops_log(cfg: Config, level: str, action: str, message: str, **extra: Any) -> None:
    payload: dict[str, Any] = {
        'ts': now_iso(),
        'level': level.upper(),
        'action': action,
        'message': message,
    }
    if extra:
        payload['extra'] = extra

    cfg.ops_log.parent.mkdir(parents=True, exist_ok=True)
    with cfg.ops_log.open('a', encoding='utf-8') as f:
        f.write(json.dumps(payload, ensure_ascii=False) + '\n')


def read_json(path: Path, default: dict[str, Any]) -> dict[str, Any]:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        return default


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')


def iter_new_log_events(cfg: Config) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    state = read_json(cfg.state_file, {'position': 0, 'inode': None})
    events: list[dict[str, Any]] = []

    if not cfg.ops_log.exists():
        return events, state

    stat = cfg.ops_log.stat()
    inode = int(getattr(stat, 'st_ino', 0))
    size = int(stat.st_size)

    pos = int(state.get('position', 0))
    old_inode = state.get('inode')

    if old_inode != inode or pos > size:
        pos = 0

    with cfg.ops_log.open('r', encoding='utf-8', errors='ignore') as f:
        f.seek(pos)
        for raw in f:
            line = raw.strip()
            if not line:
                continue
            try:
                events.append(json.loads(line))
            except Exception:
                events.append({'ts': '-', 'level': 'INFO', 'action': 'raw', 'message': line})
        new_pos = f.tell()

    new_state = {'position': new_pos, 'inode': inode}
    return events, new_state


def ensure_state_initialized(cfg: Config) -> None:
    if cfg.state_file.exists():
        return
    if not cfg.ops_log.exists():
        write_json(cfg.state_file, {'position': 0, 'inode': None})
        return
    stat = cfg.ops_log.stat()
    inode = int(getattr(stat, 'st_ino', 0))
    write_json(cfg.state_file, {'position': int(stat.st_size), 'inode': inode})


def format_event(event: dict[str, Any]) -> str:
    ts = event.get('ts', '-')
    level = event.get('level', 'INFO')
    action = event.get('action', 'event')
    message = event.get('message', '')
    extra = event.get('extra')
    base = f"[{ts}] [{level}] {action}\n{message}"
    if extra:
        return base + '\n' + json.dumps(extra, ensure_ascii=False)
    return base


def should_notify(event: dict[str, Any]) -> bool:
    level = str(event.get('level', 'INFO')).upper()
    action = str(event.get('action', ''))
    if level in {'ERROR', 'WARN'}:
        return True
    if action in IMPORTANT_ACTIONS:
        return True
    return False


def _fmt_int(value: Any) -> str:
    try:
        return f"{int(value):,}".replace(",", "_")
    except Exception:
        return "-"


def human_message(event: dict[str, Any]) -> str:
    ts = str(event.get('ts', '-'))
    level = str(event.get('level', 'INFO')).upper()
    action = str(event.get('action', 'event'))
    message = str(event.get('message', ''))
    extra = event.get('extra') or {}

    if action in {'auto_merge', 'final_merge'}:
        workers_progress = extra.get('workers_progress') or {}
        workers_line = ', '.join(
            f"{k}:{_fmt_int(v)}"
            for k, v in sorted(workers_progress.items())
        ) or 'n/a'
        return (
            f"Merge update ({action})\n"
            f"Time: {ts}\n"
            f"Global INN: {_fmt_int(extra.get('global_max_inn'))}\n"
            f"Workers: {workers_line}\n"
            f"Details: {message}"
        )

    if action == 'progress':
        workers_progress = extra.get('workers_progress') or {}
        workers_line = ', '.join(
            f"{k}:{_fmt_int(v)}"
            for k, v in sorted(workers_progress.items())
        ) or 'n/a'
        return (
            f"Parsing progress\n"
            f"Time: {ts}\n"
            f"Global INN: {_fmt_int(extra.get('global_max_inn'))}\n"
            f"Workers: {workers_line}"
        )

    if level in {'ERROR', 'WARN'}:
        return f"Status: {level}\nAction: {action}\nMessage: {message}"

    if action in {'run_start', 'run_finish', 'parse_start', 'parse_done', 'parse_failed', 'worker_failed', 'interrupted'}:
        return f"Status update\nTime: {ts}\nAction: {action}\nMessage: {message}"

    return format_event(event)


def csv_count_rows(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open('r', encoding='utf-8', newline='') as f:
        reader = csv.reader(f)
        try:
            next(reader)
        except StopIteration:
            return 0
        return sum(1 for _ in reader)


def workers_status(work_dir: Path) -> dict[str, int]:
    out: dict[str, int] = {}
    for st in sorted(work_dir.glob('worker_*/state.json')):
        try:
            payload = json.loads(st.read_text(encoding='utf-8'))
            status = str(payload.get('status', 'unknown'))
        except Exception:
            status = 'invalid'
        out[status] = out.get(status, 0) + 1
    return out


def tail_events(path: Path, limit: int) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    lines = path.read_text(encoding='utf-8', errors='ignore').splitlines()
    out: list[dict[str, Any]] = []
    for line in lines[-limit:]:
        try:
            out.append(json.loads(line))
        except Exception:
            out.append({'ts': '-', 'level': 'INFO', 'action': 'raw', 'message': line})
    return out


@router.message(Command('start'))
async def cmd_start(message: Message) -> None:
    cfg: Config = CTX['cfg']
    if message.chat.id != cfg.chat_id:
        await message.answer('Этот бот привязан к другому chat_id.')
        return
    text = (
        'MakeBiz parser monitor bot online.\\n'
        '/status - текущий статус парса/merge\\n'
        '/tail - последние события логов\\n'
        '/ping - проверка бота'
    )
    await message.answer(text)


@router.message(Command('ping'))
async def cmd_ping(message: Message) -> None:
    cfg: Config = CTX['cfg']
    if message.chat.id != cfg.chat_id:
        return
    await message.answer('pong')


@router.message(Command('status'))
async def cmd_status(message: Message) -> None:
    cfg: Config = CTX['cfg']
    if message.chat.id != cfg.chat_id:
        return

    links = csv_count_rows(cfg.links_csv)
    statuses = workers_status(cfg.work_dir)
    status_text = ', '.join(f'{k}:{v}' for k, v in sorted(statuses.items())) if statuses else 'no workers yet'

    tail = tail_events(cfg.ops_log, 100)
    merges = [x for x in tail if x.get('action') in {'auto_merge', 'final_merge'}]
    errors = [x for x in tail if str(x.get('level', 'INFO')).upper() == 'ERROR']
    progress = [x for x in tail if x.get('action') == 'progress']
    last_merge = merges[-1] if merges else None
    last_progress = progress[-1] if progress else None

    text = [
        'MakeBiz parser status',
        f'Links CSV rows: {links}',
        f'Workers state: {status_text}',
        f'Errors in last 100 events: {len(errors)}',
    ]
    if last_progress:
        text.append('')
        text.append('Latest progress:')
        text.append(human_message(last_progress))
    if last_merge:
        text.append('')
        text.append('Last merge:')
        text.append(human_message(last_merge))

    await message.answer('\n'.join(text))


@router.message(Command('tail'))
async def cmd_tail(message: Message) -> None:
    cfg: Config = CTX['cfg']
    if message.chat.id != cfg.chat_id:
        return

    limit = 10
    parts = (message.text or '').split()
    if len(parts) >= 2:
        try:
            limit = max(1, min(30, int(parts[1])))
        except Exception:
            limit = 10

    events = tail_events(cfg.ops_log, limit)
    if not events:
        await message.answer('Логи пока пустые.')
        return

    chunks: list[str] = []
    current = ''
    for ev in events:
        row = format_event(ev) + '\n\n'
        if len(current) + len(row) > 3500:
            chunks.append(current)
            current = row
        else:
            current += row
    if current:
        chunks.append(current)

    for c in chunks:
        await message.answer(c)


async def monitor_loop(bot: Bot, cfg: Config) -> None:
    while True:
        try:
            events, state = iter_new_log_events(cfg)
            if events:
                write_json(cfg.state_file, state)

            for ev in events:
                if should_notify(ev):
                    await bot.send_message(cfg.chat_id, human_message(ev), disable_notification=True)
        except Exception as exc:
            logging.exception('monitor loop failed')
            append_ops_log(cfg, 'ERROR', 'telegram_monitor_error', str(exc))
            try:
                await bot.send_message(
                    cfg.chat_id,
                    f'[ERROR] telegram_monitor_error\\n{exc}',
                    disable_notification=True,
                )
            except Exception:
                pass

        await asyncio.sleep(cfg.poll_seconds)


def build_config(args: argparse.Namespace) -> Config:
    token = os.getenv('MAKEBIZ_TG_BOT_TOKEN', '').strip()
    chat_raw = os.getenv('MAKEBIZ_TG_CHAT_ID', '').strip()
    if not token:
        raise ValueError('MAKEBIZ_TG_BOT_TOKEN is required')
    if not chat_raw:
        raise ValueError('MAKEBIZ_TG_CHAT_ID is required')

    try:
        chat_id = int(chat_raw)
    except ValueError as e:
        raise ValueError('MAKEBIZ_TG_CHAT_ID must be integer') from e

    ops_log = Path(os.getenv('MAKEBIZ_OPS_LOG', str(args.ops_log))).resolve()
    links_csv = Path(os.getenv('MAKEBIZ_LINKS_CSV', str(args.links_csv))).resolve()
    work_dir = Path(os.getenv('MAKEBIZ_WORK_DIR', str(args.work_dir))).resolve()
    state_file = Path(os.getenv('MAKEBIZ_TG_STATE_FILE', str(args.state_file))).resolve()
    poll_seconds = float(os.getenv('MAKEBIZ_TG_POLL_SECONDS', str(args.poll_seconds)))

    return Config(
        token=token,
        chat_id=chat_id,
        ops_log=ops_log,
        links_csv=links_csv,
        work_dir=work_dir,
        state_file=state_file,
        poll_seconds=max(1.0, poll_seconds),
    )


async def async_main(cfg: Config) -> None:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')

    bot = Bot(token=cfg.token)
    dp = Dispatcher()
    dp.include_router(router)

    CTX['cfg'] = cfg

    ensure_state_initialized(cfg)
    monitor_task: asyncio.Task | None = None
    try:
        append_ops_log(cfg, 'INFO', 'telegram_bot_start', 'Telegram notifier bot started', chat_id=cfg.chat_id)
        await bot.send_message(cfg.chat_id, 'MakeBiz Telegram monitor bot started', disable_notification=True)
        monitor_task = asyncio.create_task(monitor_loop(bot, cfg))
        await dp.start_polling(bot)
    except TelegramUnauthorizedError as e:
        append_ops_log(cfg, 'ERROR', 'telegram_bot_auth_failed', str(e))
        raise SystemExit(
            'Telegram Unauthorized: проверь MAKEBIZ_TG_BOT_TOKEN в .env.telegram '
            '(получи новый токен у @BotFather).'
        ) from e
    finally:
        if monitor_task is not None:
            monitor_task.cancel()
        append_ops_log(cfg, 'INFO', 'telegram_bot_stop', 'Telegram notifier bot stopped')
        await bot.session.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='MakeBiz Telegram notifier bot (aiogram)')
    parser.add_argument('--dotenv', type=Path, default=Path('.env.telegram'))
    parser.add_argument('--ops-log', type=Path, default=Path('data/logs/operations.log'))
    parser.add_argument('--links-csv', type=Path, default=Path('data/orginfo_company_links.csv'))
    parser.add_argument('--work-dir', type=Path, default=Path('data/local_multi'))
    parser.add_argument('--state-file', type=Path, default=Path('data/logs/telegram_bot_state.json'))
    parser.add_argument('--poll-seconds', type=float, default=5.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    load_dotenv(args.dotenv)
    cfg = build_config(args)
    asyncio.run(async_main(cfg))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
