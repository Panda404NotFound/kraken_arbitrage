# Этап сборки
FROM rust:1.70-bullseye AS builder

# Устанавливаем необходимые системные пакеты
RUN apt-get update && apt-get install -y \
    libssl-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Создаем директорию для приложения
WORKDIR /Project

# Копируем Cargo.toml и Cargo.lock (если есть)
COPY Cargo.toml Cargo.lock* ./

# Cоздаем директорию src
RUN mkdir src

# Копируем реальный исходный код
COPY src ./src
COPY .env ./

# Собираем приложение
RUN cargo build --release

# Финальный этап
FROM debian:bullseye-slim

# Устанавливаем необходимые библиотеки
RUN apt-get update && apt-get install -y \
    libssl1.1 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Создаем рабочую директорию
WORKDIR /Project/kraken

# Копируем собранное приложение из этапа сборки
COPY --from=builder /Project/target/release/kraken /Project/kraken/kraken
COPY --from=builder /Project/.env /Project/kraken/.env

# Проверяем зависимости исполняемого файла
RUN ldd /Project/kraken/kraken

# Запускаем приложение
CMD ["/Project/kraken/./kraken"]