-- SQL скрипт для проверки данных в таблице cat_facts

-- Посмотреть все записи
SELECT * FROM cat_facts ORDER BY created_at DESC;

-- Посмотреть последние 10 фактов
SELECT
    id,
    fact,
    length,
    fetched_at,
    created_at
FROM cat_facts
ORDER BY created_at DESC
LIMIT 10;

-- Статистика по длине фактов
SELECT
    COUNT(*) as total_facts,
    MIN(length) as min_length,
    MAX(length) as max_length,
    AVG(length) as avg_length,
    MIN(created_at) as first_record,
    MAX(created_at) as last_record
FROM cat_facts;

-- Группировка по дням
SELECT
    DATE(created_at) as date,
    COUNT(*) as facts_count
FROM cat_facts
GROUP BY DATE(created_at)
ORDER BY date DESC;

-- Самый длинный факт
SELECT
    fact,
    length,
    created_at
FROM cat_facts
ORDER BY length DESC
LIMIT 1;

-- Самый короткий факт
SELECT
    fact,
    length,
    created_at
FROM cat_facts
ORDER BY length ASC
LIMIT 1;

