## 🎯 Как переключать уровни логирования:
#### Способ 1: Через аргументы JVM
```bash
# DEBUG уровень
java -Dlogging.level.org.fedorov.pgtablescaler=DEBUG -jar target/postgres-table-scaler-1.0.0-all.jar ...

# TRACE уровень
java -Dlogging.level.org.fedorov.pgtablescaler=TRACE -jar target/postgres-table-scaler-1.0.0-all.jar ...
```

#### Способ 2: Раскомментировать строки в logback.xml
Раскомментируйте соответствующие секции в файле src/main/resources/logback.xml

#### Способ 3: Изменить переменную окружения
```bash
export LOG_LEVEL=DEBUG
java -jar target/postgres-table-scaler-1.0.0-all.jar ...
```