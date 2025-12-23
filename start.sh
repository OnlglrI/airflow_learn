#!/bin/bash

# Скрипт для быстрого запуска Airflow окружения

set -e

echo "🚀 Запуск Airflow + PostgreSQL + MinIO..."
echo ""

# Проверка наличия Docker
if ! command -v docker &> /dev/null; then
    echo "❌ Docker не установлен. Пожалуйста, установите Docker."
    exit 1
fi

# Проверка наличия Docker Compose
if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
    echo "❌ Docker Compose не установлен. Пожалуйста, установите Docker Compose."
    exit 1
fi

# Определение команды docker compose
if docker compose version &> /dev/null; then
    DOCKER_COMPOSE="docker compose"
else
    DOCKER_COMPOSE="docker-compose"
fi

# Создание необходимых директорий
echo "📁 Создание директорий..."
mkdir -p dags logs plugins config

# Установка AIRFLOW_UID если не задан
if ! grep -q "^AIRFLOW_UID=" .env 2>/dev/null; then
    echo "🔧 Настройка AIRFLOW_UID..."
    echo "AIRFLOW_UID=$(id -u)" >> .env
fi

# Запуск контейнеров
echo ""
echo "🐳 Запуск Docker контейнеров..."
$DOCKER_COMPOSE up -d

# Ожидание запуска сервисов
echo ""
echo "⏳ Ожидание запуска сервисов (это может занять несколько минут)..."
sleep 10

# Проверка статуса
echo ""
echo "📊 Статус сервисов:"
$DOCKER_COMPOSE ps

echo ""
echo "✅ Запуск завершен!"
echo ""
echo "📝 Доступ к сервисам:"
echo "  - Airflow UI:     http://localhost:8080 (airflow/airflow)"
echo "  - MinIO Console:  http://localhost:9001 (minioadmin/minioadmin)"
echo "  - MinIO API:      http://localhost:9000"
echo "  - Flower:         http://localhost:5555"
echo "  - PostgreSQL:     localhost:5432 (airflow/airflow)"
echo ""
echo "📖 Полезные команды:"
echo "  Просмотр логов:     $DOCKER_COMPOSE logs -f"
echo "  Остановка:          $DOCKER_COMPOSE down"
echo "  Перезапуск:         $DOCKER_COMPOSE restart"
echo "  Масштабирование:    $DOCKER_COMPOSE up -d --scale airflow-worker=3"
echo ""

