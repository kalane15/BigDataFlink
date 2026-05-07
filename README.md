# BigDataFlink
Анализ больших данных - лабораторная работа №3 - Streaming processing с помощью Flink

Иванченко Макар Дмитриевич. М8О-310Б-23.
Запуск (полный):
1. docker compose up -d --build
2. docker cp flink-jobs/start_job.py flink-jobmanager:/tmp/start_job.py
3. docker exec flink-jobmanager flink run -py /tmp/start_job.py
4. Подождать...
5. Проверить Postgres через DBeaver

Информацию по подключению к бд можно найти в самой джобе flink-jobs/start_job.py.