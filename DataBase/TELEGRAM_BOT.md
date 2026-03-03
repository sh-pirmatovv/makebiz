# MakeBiz Telegram Monitor Bot

## Что делает

- Читает `data/logs/operations.log` в реальном времени.
- Шлёт в Telegram уведомления о событиях:
  - `auto_merge`, `final_merge`
  - `worker_failed`, `parse_failed`
  - `run_start`, `run_finish`, `interrupted`
  - любые `WARN`/`ERROR`.
- Команды в Telegram:
  - `/status` — текущий статус ссылок и воркеров
  - `/tail [N]` — последние N событий из логов
  - `/ping`

## Настройка

1. Установи зависимости:

```bash
python3 -m pip install -r DataBase/requirements-bot.txt
```

2. Создай env-файл:

```bash
cp .env.telegram.example .env.telegram
```

3. Заполни:

- `MAKEBIZ_TG_BOT_TOKEN`
- `MAKEBIZ_TG_CHAT_ID`

## Запуск

```bash
python3 DataBase/telegram_notifier_bot.py
```

## Важно

- Бот не останавливает парсер.
- Состояние чтения логов хранится в `data/logs/telegram_bot_state.json`.
- При первом запуске бот начинает с конца лога (старые строки не спамит).
