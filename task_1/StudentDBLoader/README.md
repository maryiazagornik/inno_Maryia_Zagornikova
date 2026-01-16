# Student Database Loader / Загрузчик базы данных студентов

[![Python](https://img.shields.io/badge/python-3.7%2B-blue)](https://www.python.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13%2B-336791)](https://www.postgresql.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A Python application for loading student and room data into a PostgreSQL database and performing analytical queries.

Python приложение для загрузки данных о студентах и комнатах в базу данных PostgreSQL и выполнения аналитических запросов.

## Features / Особенности

- Loading data from JSON files / Загрузка данных из JSON файлов
- Data validation and error handling / Валидация данных и обработка ошибок
- Configurable database connection / Настраиваемое подключение к базе данных
- Multiple output formats (JSON, XML) / Поддержка нескольких форматов вывода (JSON, XML)
- Logging to file and console / Логирование в файл и консоль

## Requirements / Требования

- Python 3.7+
- PostgreSQL 13+
- pip package manager

## Installation / Установка

1. Clone the repository / Клонируйте репозиторий:
   ```bash
   git clone <your-repository>
   cd StudentDBLoader
   ```

2. Create and activate a virtual environment (recommended) / Создайте и активируйте виртуальное окружение (рекомендуется):
   ```bash
   python -m venv venv
   .\venv\Scripts\activate  # Windows
   source venv/bin/activate   # Linux/Mac
   ```

3. Install dependencies / Установите зависимости:
   ```bash
   pip install -r requirements.txt
   ```

4. Set up the database / Настройте базу данных:
   - Create a PostgreSQL database / Создайте базу данных PostgreSQL:
     ```sql
     CREATE DATABASE university_db;
     ```
   - Create a `.env` file from the example / Создайте файл `.env` из примера:
     ```bash
     copy .env.example .env
     ```
   - Update the `.env` file with your database credentials / Обновите файл `.env` с вашими учетными данными

## Configuration / Конфигурация

Edit the `.env` file to configure the application:

Отредактируйте файл `.env` для настройки приложения:

```ini
# Database Configuration
DB_NAME=university_db
DB_USER=postgres
DB_PASSWORD=your_secure_password
DB_HOST=localhost
DB_PORT=5432

# Application Settings
DEFAULT_FORMAT=json
LOG_LEVEL=INFO
MAX_RETRIES=3

# File Settings
DEFAULT_OUTPUT_FILE=output
FILE_ENCODING=utf-8

# Query Settings
TOP_N_RESULTS=5
BATCH_SIZE=1000
```

## Usage / Использование

### Main Script / Основной скрипт

```bash
python main.py --students students.json --rooms rooms.json --format json
```

Parameters / Параметры:
- `--students` - path to students JSON file (required) / путь к файлу с данными студентов (обязательный)
- `--rooms` - path to rooms JSON file (required) / путь к файлу с данными комнат (обязательный)
- `--format` - output format: `json` or `xml` (default: `json`) / формат вывода: `json` или `xml` (по умолчанию: `json`)

### Run Queries / Выполнение запросов

To run the analytical queries and see the results:

Для выполнения аналитических запросов и просмотра результатов:

```bash
python run_queries.py
```

## Project Structure / Структура проекта

```
StudentDBLoader/
├── .env.example           # Example environment configuration
├── .gitignore             # Git ignore file
├── README.md              # This file
├── requirements.txt       # Python dependencies
├── config.py             # Application configuration
├── data_loader.py        # Data loading functionality
├── db_connector.py       # Database connection handling
├── formatters.py         # Output formatting (JSON, XML)
├── main.py               # Main script
├── query_runner.py       # Query execution
├── run_queries.py        # Run analytical queries
├── sql_queries.py        # SQL queries
├── students.json         # Sample student data
└── rooms.json            # Sample room data
```

## Logging / Логирование

The application logs to both console and `app.log` file with the following format:

Приложение ведет логи в консоль и файл `app.log` в следующем формате:

```
2023-04-01 12:00:00,000 - data_loader - INFO - Successfully loaded 100 students
```

## Error Handling / Обработка ошибок

- Invalid data is skipped and logged / Некорректные данные пропускаются и логируются
- Database connection errors are handled gracefully / Ошибки подключения к базе данных обрабатываются корректно
- Detailed error messages are provided / Предоставляются подробные сообщения об ошибках

## License / Лицензия

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

Этот проект лицензирован в соответствии с лицензией MIT - подробности см. в файле [LICENSE](LICENSE).

### Утилита для выполнения запросов

```bash
python run_queries.py
```

Эта утилита выполняет все запросы и выводит результаты в консоль, а также сохраняет их в файл `query_results.json`.

## Структура проекта

- `main.py` - основной скрипт для загрузки данных
- `db_connector.py` - работа с базой данных
- `data_loader.py` - загрузка данных из JSON
- `query_runner.py` - выполнение аналитических запросов
- `formatters.py` - форматирование вывода
- `run_queries.py` - утилита для выполнения запросов

## Оптимизация запросов

Для ускорения работы запросов добавлены следующие индексы:
- `idx_students_room_id` - для ускорения поиска по ID комнаты
- `idx_students_birthday` - для ускорения расчёта возраста
- `idx_students_sex` - для ускорения поиска по полу

## Лицензия

MIT
