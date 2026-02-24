# Sprint 1 — базовая подготовка проекта

В рамках Sprint 1 подготовлены базовые файлы для запуска торгового бота:

- `requirements.txt` — список Python-зависимостей.
- `config.example.json` — шаблон конфигурации.

## Как запустить

1. Создайте и активируйте виртуальное окружение.
2. Установите зависимости:
   ```bash
   pip install -r requirements.txt
   ```
3. Скопируйте шаблон конфигурации:
   ```bash
   cp config.example.json config.json
   ```
4. Заполните `config.json` реальными значениями.
5. Запустите бота:
   ```bash
   python bot_div_2.py
   ```

## Важно

- Не коммитьте `config.json` и сервисные ключи в репозиторий.
- Для Google Sheets требуется service account JSON-файл.
