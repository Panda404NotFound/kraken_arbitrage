[![Static Badge](https://img.shields.io/badge/Telegram-Channel-Link?style=for-the-badge&logo=Telegram&logoColor=white&logoSize=auto&color=blue)](https://t.me/hidden_coding)

[![Static Badge](https://img.shields.io/badge/Telegram-Chat-yes?style=for-the-badge&logo=Telegram&logoColor=white&logoSize=auto&color=blue)](https://t.me/hidden_codding_chat)

[![Static Badge](https://img.shields.io/badge/Telegram-Bot%20Link-Link?style=for-the-badge&logo=Telegram&logoColor=white&logoSize=auto&color=blue)](https://t.me/PAWSOG_bot/PAWS?startapp=xDZm2M3t)

## [HIDDEN CODE MARKET](https://t.me/hcmarket_bot?start=referral_355876562)

#### - [Paws wallet connector](https://t.me/hcmarket_bot?start=referral_355876562-project_1016)
#### - [Premium notpixel](https://t.me/hcmarket_bot?start=referral_355876562-project_1015)
#### - [Blum wallet connector](https://t.me/hcmarket_bot?start=referral_355876562-project_1002)
#### - [Telegram warning up](https://t.me/hcmarket_bot?start=referral_355876562-project_1001)

# KRAKEN Мультивалютный арбитражный Бот

## Описание
Представляю вашему внимание мультивалютный арбитражный бот, который работает выполняет арбитражные операции с мультивалютными парами. Цепочки распределены от 4 до 5 например USDT -> BTC -> SOL -> (XPR опционально если цепочка состоит из 5 ребер) -> USDT где начальный и конечный токен всегда USDT чтобы предотвратить падение портфеля в процессе работы. 

Делюсь проектом потому что я не удовлетворен его доходностью, которая за время релизного теста не показала ожидаемых результатов. Бот расчитан на максимизацию прибыли и минимизацию рисков, ориентирован на доходность от 2,5% за вилку.

Я хочу заняться другим проектом для автоматизированной торговлю на спотовом и фьючерсном рынке с использованием машинного обучения и нейросетей.

### Принцип работы
- Работает с цепочками из 4-5 торговых пар
- Схема работы: USDT -> BTC -> SOL -> (XPR*) -> USDT 
- *XPR опционален для 5-звенных цепочек
- Начальный и конечный токен всегда USDT для защиты портфеля

## Техническая реализация
- Основная логика находится в `brain_bot.rs`
- Базовые конфигурации в `config.rs`
- Построение цепочек через рекурсивный DFS
- Асинхронная параллельная обработка всех цепочек
- Неблокирующий доступ к данным
- Защита от переполнения стека

## Установка

### Предварительные требования
- Linux-дистрибутив
- Docker
- Git

### Шаги установки
1. Создайте .env в папке проекта. Добавьте необходимые конфигурации в .env из примера .env_example.txt файла.

2. Клонирование репозитория:
```bash
git clone [repository-url]
```

3. Настройка прав и запуск скрипта:
```bash
cd kraken_cex && chmod +x key.sh && ./key.sh
```

4. Сборка и запуск Docker-контейнера:
```bash
sudo docker build -t kraken . && sudo docker run -it -d --name kraken_cont --restart unless-stopped kraken
```

## Тестирование

Для проведения тестирования необходимо выполнить следующие шаги:

### 1. Установка Cargo

Установите менеджер пакетов Cargo, выполнив следующую команду:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

> **Примечание**: Cargo устанавливается с официального сайта Rust. Следуйте инструкциям установщика в терминале.

### 2. Навигация в директорию проекта

Перейдите в корневую директорию проекта.

### 3. Запуск тестирования

Выполните следующую команду для запуска с включенным выводом отладочной информации:

```bash
RUST_LOG=debug cargo run --release
```

## Системные требования
- CPU: от 4 ядер
- RAM: от 8 GB
- SSD: 150 GB
- ОС: Linux

## Метрики производительности
- Обработка ~3000 цепочек за 25 мс
- Обработка WebSocket сообщений: 2-8 µs (среднее 4-5 µs)
- Обработка ордербука: 317 ns - 10.914 µs (среднее 1 µs)
- Отклик WebSocket: до 300 мс (до 5 мс в регионе Japan)

## Возможности бота

1. **Мониторинг и уведомления**
   - Ежечасная отправка статуса работы в Telegram (функция отключена в `math_graph.rs`, доступна в логах)
   - Отправка критических уведомлений в Telegram
   - Запись сделок, прибыльных цепочек и их "времени жизни" в файлы отладки
   - Уведомления о выборе цепочек и результатах сделок в Telegram

2. **Отказоустойчивость**
   - Автоматический перезапуск при проблемах с интернет-соединением
   - Критические остановки при аномальных ошибках
   - Многокомпонентная логика учета цепочек
   - Пересчет прибыльности на каждом шаге

3. **Оптимизация торговли**
   - Получение актуальных данных стаканов без задержек за наносекунды
   - Двухслойная проверка валидности цепочек
   - Выбор альтернативных цепочек для:
     - Максимизации прибыли
     - Минимизации убытков
   - Учет всех прибыльных цепочек с выбором наиболее выгодной

4. **Управление рисками**
   - Учет рыночного проскальзывания
   - Анализ "ложных" объемов маркет-мейкера

## Рекомендации
- Рекомендуется использовать регион Japan (Azure Cloud)
- Необходимо провести тестирование перед релизом
- Рекомендуется использовать VPN при проблемах с подключением
- Поддерживаются регионы: Азия, Европа, Америка

## Отключение Telegram-уведомлений
Для отключения функции сигналов в Telegram, закомментируйте следующие строки в `main.rs`:

```rust
mod telegram;
use crate::telegram::TelegramBot;

// Инициализация TelegramBot
let telegram_bot = Arc::new(TelegramBot::new(
    &std::env::var("TELEGRAM_TOKEN").expect("TELEGRAM_TOKEN не установлена"),
    std::env::var("CHAT_ID").expect("CHAT_ID не установлена").parse::<i64>().expect("Неверный формат CHAT_ID"),
    Arc::clone(&error_status),
    bot_action_sender.clone(),
));

// Запуск TelegramBot
let telegram_bot_clone = Arc::clone(&telegram_bot);
tokio::spawn(async move {
    telegram_bot_clone.run().await;
});
```

## Контакты
Telegram: @brahman_brahman
