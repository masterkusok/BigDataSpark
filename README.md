# BigDataSpark

Анализ больших данных - лабораторная работа №2 - ETL реализованный с помощью Spark

## Запуск

1. Для работы с `PostgreSQL` и `Clickhouse` необходимо загрузить `JDBC` драйвера при помощи:
``` sh
make init
```

2. Далее выполнить `docker-compose up -d` для запуска `Spark`, `Clickhouse`, `PostgreSQL` и мигратора.

3. Непосредственно `spark`-джобы запускаются при помощи `Makefile`:
```sh
make pg-job # Запуск PostgreSQL джобы
```

```sh
make ch-job # Запуск Clickhouse джобы
```

4. Витрины можно посмотреть при помощи `DBeaver` или `psql` + `clickhouse-client`