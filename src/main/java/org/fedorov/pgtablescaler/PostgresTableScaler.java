package org.fedorov.pgtablescaler;

import java.sql.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.io.StringReader;
import java.io.PrintWriter;
import java.io.PipedOutputStream;
import java.io.PipedInputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.postgresql.PGConnection;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public class PostgresTableScaler {
    
    private static final Logger logger = LoggerFactory.getLogger(PostgresTableScaler.class);
    
    // Конфигурация подключения по умолчанию
    private static final String DEFAULT_HOST = "localhost";
    private static final String DEFAULT_PORT = "5432";
    private static final String DEFAULT_DB = "mydb";
    private static final String DEFAULT_USER = "postgres";
    private static final String DEFAULT_PASSWORD = "password";
    private static final String DEFAULT_SCHEMA = "public";
    
    // Параметры пула соединений
    private static final int DEFAULT_POOL_SIZE = 10;
    private static final int DEFAULT_CONNECTION_TIMEOUT = 3600000; // 60 минут
    private static final int DEFAULT_MAX_LIFETIME = 7200000; // 120 минут
    
    private static HikariDataSource dataSource;
    
    public static void main(String[] args) {
        ArgsParser parsedArgs = new ArgsParser(args);
        
        if (!parsedArgs.isValid()) {
            printUsage();
            return;
        }
        
        try {
            // Инициализируем DataSource
            initDataSource(parsedArgs);
            
            if (parsedArgs.hasGenerate()) {
                TableGenerator generator = new TableGenerator();
                generator.generateData(
                    parsedArgs.getTableIn(),
                    parsedArgs.getRows(),
                    parsedArgs.getThreads(),
                    parsedArgs.getBatchSize()
                );
            }
            
            if (parsedArgs.hasScale()) {
                TableScaler scaler = new TableScaler();
                scaler.scaleData(
                    parsedArgs.getTableIn(),
                    parsedArgs.getTableOut(),
                    parsedArgs.getScale(),
                    parsedArgs.getThreads(),
                    parsedArgs.getBatchSize()
                );
            }
            
        } catch (Exception e) {
            logger.error("Критическая ошибка в приложении", e);
        } finally {
            if (dataSource != null) {
                dataSource.close();
                logger.info("DataSource закрыт");
            }
        }
    }
    
    private static void initDataSource(ArgsParser args) {
        HikariConfig config = new HikariConfig();
        
        // Формируем URL с параметрами
        List<String> conProperties = new ArrayList<>();
        conProperties.add("prepareThreshold=1");
        conProperties.add("binaryTransfer=false");
        conProperties.add("stringtype=unspecified");
        
        String url = String.format("jdbc:postgresql://%s:%s/%s?%s",
            args.getHost(), args.getPort(), args.getDatabase(),
            String.join("&", conProperties));
        
        config.setJdbcUrl(url);
        config.setUsername(args.getUser());
        config.setPassword(args.getPassword());
        config.setAutoCommit(args.isAutoCommit());
        
        // Настройки кэширования подготовленных запросов
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        
        // Настройки пула
        config.setMaximumPoolSize(args.getPoolSize());
        config.setConnectionTimeout(args.getConnectionTimeout());
        config.setMaxLifetime(args.getMaxLifetime());
        config.setIdleTimeout(600000); // 10 минут
        config.setMinimumIdle(2);
        
        // Инициализационный SQL
        config.setConnectionInitSql(String.format("SET search_path TO %s", args.getSchema()));
        
        // Дополнительные настройки для производительности
        config.addDataSourceProperty("reWriteBatchedInserts", "true");
        config.addDataSourceProperty("defaultRowFetchSize", "10000");
        
        // Настройки валидации соединений
        config.setConnectionTestQuery("SELECT 1");
        config.setValidationTimeout(5000);
        config.setLeakDetectionThreshold(60000); // Обнаружение утечек через 60 сек
        
        // Настройки логирования HikariCP
        config.setPoolName("PostgresScaler-Pool");
        
        dataSource = new HikariDataSource(config);
        
        logger.info("DataSource инициализирован: {}:{}/{} (схема: {}, пул: {})", 
            args.getHost(), args.getPort(), args.getDatabase(), args.getSchema(), args.getPoolSize());
        
        logger.debug("Детали подключения: autoCommit={}, timeout={}ms, maxLifetime={}ms",
            args.isAutoCommit(), args.getConnectionTimeout(), args.getMaxLifetime());
    }
    
    private static Connection getConnection() throws SQLException {
        if (dataSource == null) {
            throw new SQLException("DataSource не инициализирован");
        }
        return dataSource.getConnection();
    }
    
    private static void printUsage() {
        System.out.println("""
            \nИспользование: java PostgresTableScaler [опции]
            
            Параметры подключения (опционально):
              --host <host>              Хост PostgreSQL (по умолчанию: localhost)
              --port <port>              Порт (по умолчанию: 5432)
              --db <database>            Имя базы данных (по умолчанию: mydb)
              --user <username>           Имя пользователя (по умолчанию: postgres)
              --password <password>       Пароль (по умолчанию: password)
              --schema <schema>           Схема (по умолчанию: public)
              --pool-size <size>          Размер пула соединений (по умолчанию: 10)
              --auto-commit <true/false>  AutoCommit (по умолчанию: false)
            
            Обязательные опции:
              -i, --table_in <таблица>   Имя входной таблицы
              
            Опции генерации:
              -g, --generate              Режим генерации данных
              -n, --rows <число>          Количество строк для генерации
              
            Опции масштабирования:
              -s, --scale <фактор>        Масштабировать данные (количество копирований)
              -o, --table_out <таблица>   Имя выходной таблицы (опционально)
              
            Общие опции:
              -t, --threads <число>       Количество потоков (по умолчанию: 1)
              -b, --batch_size <число>     Размер батча (по умолчанию: 1000)
              
            Примеры:
              java PostgresTableScaler --host localhost --db testdb --user postgres --schema dev -i users -g -n 1000000 -t 8 -b 10000
              java PostgresTableScaler --pool-size 20 -i users -o users_scaled -s 10 -t 4 -b 5000
            """);
    }
    
    // Класс для анализа структуры таблицы
    static class TableMetadata {
        private static final Logger logger = LoggerFactory.getLogger(TableMetadata.class);
        
        final String tableName;
        final List<ColumnInfo> columns = new ArrayList<>();
        final List<ColumnInfo> insertableColumns = new ArrayList<>();
        final String createTableSQL;
        final Map<String, List<String>> columnCheckConstraints = new HashMap<>();
        
        TableMetadata(Connection conn, String tableName) throws SQLException {
            this.tableName = tableName;
            
            DatabaseMetaData metaData = conn.getMetaData();
            
            // Получаем колонки таблицы
            try (ResultSet rs = metaData.getColumns(null, null, tableName, null)) {
                while (rs.next()) {
                    ColumnInfo col = new ColumnInfo();
                    col.name = rs.getString("COLUMN_NAME");
                    col.type = rs.getInt("DATA_TYPE");
                    col.typeName = rs.getString("TYPE_NAME");
                    col.isNullable = rs.getInt("NULLABLE") == DatabaseMetaData.columnNullable;
                    col.isAutoIncrement = rs.getBoolean("IS_AUTOINCREMENT");
                    
                    columns.add(col);
                    
                    // Исключаем auto-increment колонки из вставки
                    if (!col.isAutoIncrement) {
                        insertableColumns.add(col);
                    }
                }
            }
            
            // Загружаем check constraints
            loadCheckConstraints(conn, tableName);
            
            // Получаем SQL для создания таблицы (опционально)
            this.createTableSQL = extractCreateTableSQL(conn, tableName);
            
            if (columns.isEmpty()) {
                throw new SQLException("Таблица '" + tableName + "' не найдена или не содержит колонок");
            }
            
            logger.info("Таблица {}: {} колонок, {} check constraints", 
                tableName, columns.size(), columnCheckConstraints.size());
            logger.debug("Структура таблицы {}: {} колонок для вставки", 
                tableName, insertableColumns.size());
            
            if (logger.isTraceEnabled() && insertableColumns.size() <= 10) {
                for (ColumnInfo col : insertableColumns) {
                    logger.trace("  - {}: {} (nullable: {})", 
                        col.name, col.typeName, col.isNullable);
                }
            }
        }
        
        private void loadCheckConstraints(Connection conn, String tableName) throws SQLException {
        String sql = """
            SELECT 
                conname as constraint_name,
                pg_get_constraintdef(oid) as constraint_def
            FROM pg_constraint
            WHERE conrelid = ?::regclass
            AND contype = 'c'
            """;
        
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, tableName);
            try (ResultSet rs = pstmt.executeQuery()) {
                boolean found = false;
                while (rs.next()) {
                    found = true;
                    String constraintName = rs.getString("constraint_name");
                    String constraintDef = rs.getString("constraint_def");
                    logger.debug("Найден check constraint: {} = {}", constraintName, constraintDef);
                    parseCheckConstraint(constraintDef);
                }
                if (!found) {
                    logger.debug("Check constraints не найдены для таблицы {}", tableName);
                }
                
                // Выводим все найденные constraints для отладки
                if (!columnCheckConstraints.isEmpty()) {
                    logger.info("Загружены check constraints: {}", columnCheckConstraints);
                }
            }
        }
    }
        
        private void parseCheckConstraint(String constraintDef) {
            logger.debug("Парсим constraint: {}", constraintDef);
            
            // Убираем CHECK и лишние скобки
            String cleaned = constraintDef.replace("CHECK", "").trim();
            if (cleaned.startsWith("(") && cleaned.endsWith(")")) {
                cleaned = cleaned.substring(1, cleaned.length() - 1);
            }
            logger.debug("Очищенный constraint: {}", cleaned);
            
            // Ищем паттерн: column = ANY (ARRAY['value1', 'value2', ...])
            if (cleaned.contains("= ANY ((ARRAY[")) {
                // Извлекаем имя колонки (часть до = ANY)
                String[] parts = cleaned.split("= ANY", 2);
                String columnPart = parts[0].trim();
                
                // Извлекаем имя колонки, убирая ::text если есть
                String columnName = columnPart
                                    .replace("::text", "")
                                    .replace("(","")
                                    .replace(")","")
                                    .replace("[","")
                                    .replace("]","")
                                    .trim();
                
                // Извлекаем массив значений
                String arrayPart = cleaned.substring(
                    cleaned.indexOf("ARRAY[") + 6,
                    cleaned.lastIndexOf("]")
                );
                
                logger.debug("Массив значений: {}", arrayPart);
                
                // Парсим значения из массива
                List<String> values = new ArrayList<>();
                
                // Разбиваем по запятым, но учитываем кавычки
                StringBuilder currentValue = new StringBuilder();
                boolean inQuotes = false;
                
                for (int i = 0; i < arrayPart.length(); i++) {
                    char c = arrayPart.charAt(i);
                    
                    if (c == '\'') {
                        inQuotes = !inQuotes;
                    } else if (c == ',' && !inQuotes) {
                        // Конец значения
                        String value = currentValue.toString().trim();
                        if (!value.isEmpty()) {
                            // Очищаем от кавычек и типов
                            value = value.replace("'", "")
                                        .replace("::character varying", "")
                                        .replace("::text", "")
                                        .replace("(","")
                                        .replace(")","")
                                        .replace("[","")
                                        .replace("]","")
                                        .trim();
                            values.add(value);
                        }
                        currentValue = new StringBuilder();
                    } else {
                        currentValue.append(c);
                    }
                }
                
                // Добавляем последнее значение
                String lastValue = currentValue.toString().trim();
                if (!lastValue.isEmpty()) {
                    lastValue = lastValue.replace("'", "")
                                        .replace("::character varying", "")
                                        .replace("::text", "")
                                        .replace("(","")
                                        .replace(")","")
                                        .replace("[","")
                                        .replace("]","")
                                        .trim();
                    values.add(lastValue);
                }
                
                columnCheckConstraints.put(columnName, values);
                logger.info("Для колонки '{}' найдены допустимые значения: {}", columnName, values);
            }
            // Ищем паттерн: column IN ('value1', 'value2', ...)
            else if (cleaned.contains(" IN (")) {
                String[] parts = cleaned.split(" IN ", 2);
                String columnName = parts[0].trim();
                
                String valuesPart = parts[1].trim();
                if (valuesPart.startsWith("(") && valuesPart.endsWith(")")) {
                    valuesPart = valuesPart.substring(1, valuesPart.length() - 1);
                }
                
                List<String> values = new ArrayList<>();
                String[] valueItems = valuesPart.split(",");
                for (String item : valueItems) {
                    String value = item.trim();
                    if (value.startsWith("'") && value.endsWith("'")) {
                        value = value.substring(1, value.length() - 1);
                    }
                    values.add(value);
                }
                
                columnCheckConstraints.put(columnName, values);
                logger.info("Для колонки {} найдены допустимые значения: {}", columnName, values);
            } else {
                logger.warn("Не удалось распарсить constraint: {}", constraintDef);
            }
        }
        
        private String extractCreateTableSQL(Connection conn, String tableName) throws SQLException {
            // Для стандартного PostgreSQL возвращаем null, 
            // чтобы использовался CREATE TABLE LIKE
            logger.debug("Используется стандартный PostgreSQL, DDL не извлекается");
            return null;
        }
        
        String getColumnNamesForSelect() {
            return String.join(", ", columns.stream()
                .map(c -> c.name)
                .toArray(String[]::new));
        }
        
        String getColumnNamesForInsert() {
            return String.join(", ", insertableColumns.stream()
                .map(c -> c.name)
                .toArray(String[]::new));
        }
        
        String getCopyFromSQL() {
            return String.format("COPY %s (%s) FROM STDIN WITH (FORMAT CSV, NULL '\\N')", 
                tableName, getColumnNamesForInsert());
        }
        
        String getCopyToSQL() {
            return String.format("COPY (SELECT %s FROM %s) TO STDOUT WITH CSV", 
                getColumnNamesForSelect(), tableName);
        }
        
        void createTargetTable(Connection conn, String targetTable) throws SQLException {
            if (tableName.equals(targetTable)) {
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("TRUNCATE TABLE " + targetTable);
                    logger.info("Таблица {} очищена", targetTable);
                }
            } else {
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute(String.format("DROP TABLE IF EXISTS %s CASCADE", targetTable));
                    
                    // Используем CREATE TABLE LIKE (работает во всех версиях PostgreSQL)
                    stmt.execute(String.format(
                        "CREATE TABLE %s (LIKE %s INCLUDING ALL)", 
                        targetTable, tableName));
                    logger.info("Таблица {} создана (копия {})", targetTable, tableName);
                }
            }
        }
        
        static class ColumnInfo {
            String name;
            int type;
            String typeName;
            boolean isNullable;
            boolean isAutoIncrement;
            
            String generateRandomValue(Random random, long rowId, Map<String, List<String>> columnConstraints) {
                // Для NULL используем "\N" (это будет интерпретироваться как NULL в COPY с CSV форматом)
                if (isNullable && random.nextDouble() < 0.05) {
                    return "\\N";
                }
                
                // Если есть check constraint для этой колонки, используем допустимые значения
                if (columnConstraints != null && columnConstraints.containsKey(name)) {
                    List<String> allowedValues = columnConstraints.get(name);
                    String value = allowedValues.get(random.nextInt(allowedValues.size()));
                    // Значения уже должны быть в правильном регистре из constraint
                    logger.trace("Для колонки {} выбрано значение {} из допустимых: {}", 
                        name, value, allowedValues);
                    // Экранируем если нужно
                    if (value.contains(",") || value.contains("\"") || value.contains("\n")) {
                        value = "\"" + value.replace("\"", "\"\"") + "\"";
                    }
                    return value;
                }
                
                switch (type) {
                    case Types.INTEGER:
                    case Types.SMALLINT:
                        return String.valueOf(random.nextInt(1000000));
                        
                    case Types.BIGINT:
                        return String.valueOf(random.nextLong());
                        
                    case Types.DECIMAL:
                    case Types.NUMERIC:
                        return String.format("%.2f", random.nextDouble() * 10000);
                        
                    case Types.DOUBLE:
                    case Types.FLOAT:
                        return String.valueOf(random.nextDouble() * 10000);
                        
                    case Types.BOOLEAN:
                    case Types.BIT:
                        return random.nextBoolean() ? "t" : "f";
                        
                    case Types.DATE:
                        LocalDate date = LocalDate.now().minusDays(random.nextInt(3650));
                        return date.toString();
                        
                    case Types.TIME:
                        LocalTime time = LocalTime.of(
                            random.nextInt(24), 
                            random.nextInt(60), 
                            random.nextInt(60)
                        );
                        return time.toString();
                        
                    case Types.TIMESTAMP:
                        LocalDateTime timestamp = LocalDateTime.now()
                            .minusDays(random.nextInt(365))
                            .minusHours(random.nextInt(24))
                            .minusMinutes(random.nextInt(60))
                            .minusSeconds(random.nextInt(60));
                        return timestamp.toString();
                        
                    case Types.VARCHAR:
                    case Types.CHAR:
                    case Types.LONGVARCHAR:
                    default:
                        return generateRandomText(random, rowId);
                }
            }
            
            private String generateRandomText(Random random, long rowId) {
                // Проверяем название поля для специальных случаев
                if (name != null) {
                    String lowerName = name.toLowerCase();
                    
                    // Для кодов валют
                    if (lowerName.contains("currency") || lowerName.contains("curr")) {
                        String[] currencies = {"USD", "EUR", "RUB", "GBP", "JPY", "CNY", "CHF", "CAD", "AUD", "NZD"};
                        return currencies[random.nextInt(currencies.length)];
                    }
                    
                    // Для любых кодов (country_code, language_code, etc)
                    if (lowerName.contains("code")) {
                        int length = 2 + random.nextInt(3); // длина 2-4 символа
                        StringBuilder sb = new StringBuilder();
                        for (int i = 0; i < length; i++) {
                            char c = (char) ('A' + random.nextInt(26));
                            sb.append(c);
                        }
                        return sb.toString();
                    }
                    
                    // Для идентификаторов (id, guid, etc)
                    if (lowerName.contains("id") && !lowerName.contains("guid")) {
                        return String.valueOf(1000 + random.nextInt(9000));
                    }
                }
                
                // Определяем максимальную длину для текстовых полей
                int maxLength = 100; // значение по умолчанию
                
                // Если это varchar с ограничением, извлекаем длину из typeName
                if (typeName != null && typeName.contains("(") && typeName.contains(")")) {
                    try {
                        String lengthStr = typeName.substring(typeName.indexOf("(") + 1, typeName.indexOf(")"));
                        maxLength = Integer.parseInt(lengthStr);
                    } catch (Exception e) {
                        logger.debug("Не удалось извлечь длину из typeName: {}", typeName);
                    }
                }
                
                // Для CHAR фиксированной длины
                if (type == Types.CHAR && maxLength > 0) {
                    maxLength = Math.min(maxLength, 10);
                }
                
                // Генерируем текст с учетом максимальной длины
                int baseLength = 5 + random.nextInt(Math.min(10, maxLength - 1));
                int length = Math.min(baseLength, maxLength);
                
                StringBuilder sb = new StringBuilder();
                
                // Для очень коротких полей (<= 5 символов)
                if (maxLength <= 5) {
                    // Генерируем код из букв
                    for (int i = 0; i < maxLength; i++) {
                        char c = (char) ('A' + random.nextInt(26));
                        sb.append(c);
                    }
                } else {
                    sb.append("text").append(rowId % 1000).append("_");
                    while (sb.length() < length) {
                        char c = (char) ('a' + random.nextInt(26));
                        sb.append(c);
                    }
                }
                
                String result = sb.length() > maxLength ? sb.substring(0, maxLength) : sb.toString();
                
                // Экранирование для CSV
                if (result.contains(",") || result.contains("\"") || result.contains("\n")) {
                    result = "\"" + result.replace("\"", "\"\"") + "\"";
                }
                return result;
            }
        }
    }
    
    // Генератор данных (многопоточный)
    static class TableGenerator {
        private static final Logger logger = LoggerFactory.getLogger(TableGenerator.class);
        
        void generateData(String tableName, int totalRows, int threadCount, int batchSize) 
                throws SQLException, InterruptedException {
            
            logger.info("=== НАЧАЛО ГЕНЕРАЦИИ ДАННЫХ ===");
            logger.info("Таблица: {}, всего строк: {}, потоков: {}, батч: {}", 
                tableName, totalRows, threadCount, batchSize);
            
            long startTime = System.currentTimeMillis();
            
            // Получаем метаданные через временное соединение
            try (Connection conn = getConnection()) {
                TableMetadata metadata = new TableMetadata(conn, tableName);
                
                // Очищаем таблицу перед генерацией
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("TRUNCATE TABLE " + tableName);
                    logger.debug("Таблица {} очищена", tableName);
                }
            }
            
            // Распределяем работу по потокам
            List<GenerateTask> tasks = new ArrayList<>();
            long rowsPerThread = totalRows / threadCount;
            long remainingRows = totalRows % threadCount;
            long startOffset = 0;
            
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            
            for (int i = 0; i < threadCount; i++) {
                long threadRows = rowsPerThread + (i < remainingRows ? 1 : 0);
                
                if (threadRows > 0) {
                    tasks.add(new GenerateTask(
                        i, tableName, startOffset, threadRows, batchSize
                    ));
                    startOffset += threadRows;
                }
            }
            
            logger.debug("Распределение задач: {} потоков, по ~{} строк", 
                tasks.size(), rowsPerThread);
            
            // Запускаем потоки
            List<Future<GenerateResult>> futures = executor.invokeAll(tasks);
            
            // Собираем результаты
            long totalInserted = 0;
            
            for (Future<GenerateResult> future : futures) {
                try {
                    GenerateResult result = future.get();
                    totalInserted += result.rowsInserted;
                    
                    logger.debug("Поток {} завершен: +{} строк ({:.2f} rows/sec)",
                        result.threadId, result.rowsInserted, result.rowsPerSecond);
                    
                } catch (ExecutionException e) {
                    logger.error("Ошибка в потоке генерации", e.getCause());
                }
            }
            
            long totalTime = System.currentTimeMillis() - startTime;
            double speed = totalInserted / (totalTime / 1000.0);
            
            logger.info("=== ГЕНЕРАЦИЯ ЗАВЕРШЕНА ===");
            logger.info("Сгенерировано: {} строк за {} мс ({:.2f} rows/sec)", 
                totalInserted, totalTime, speed);
            
            executor.shutdown();
        }
        
        // Задача генерации для потока
        class GenerateTask implements Callable<GenerateResult> {
            private final int threadId;
            private final String tableName;
            private final long startOffset;
            private final long rowsToGenerate;
            private final int batchSize;
            private final Logger logger = LoggerFactory.getLogger(GenerateTask.class);
            
            GenerateTask(int threadId, String tableName, 
                        long startOffset, long rowsToGenerate, int batchSize) {
                this.threadId = threadId;
                this.tableName = tableName;
                this.startOffset = startOffset;
                this.rowsToGenerate = rowsToGenerate;
                this.batchSize = batchSize;
            }
            
            @Override
            public GenerateResult call() throws Exception {
                MDC.put("threadId", String.valueOf(threadId));
                
                logger.debug("Поток {} запущен: {} строк (offset: {})", 
                    threadId, rowsToGenerate, startOffset);
                
                long rowsInserted = 0;
                long startTime = System.currentTimeMillis();
                
                try (Connection threadConn = getConnection()) {
                    threadConn.setAutoCommit(false);
                    
                    // Получаем метаданные для этого потока
                    TableMetadata metadata = new TableMetadata(threadConn, tableName);
                    
                    for (long i = 0; i < rowsToGenerate; i += batchSize) {
                        int currentBatchSize = (int) Math.min(batchSize, rowsToGenerate - i);
                        
                        long batchStartTime = System.currentTimeMillis();
                        int inserted = generateBatch(threadConn, metadata, currentBatchSize, startOffset + i);
                        
                        rowsInserted += inserted;
                        
                        if (logger.isTraceEnabled()) {
                            long batchTime = System.currentTimeMillis() - batchStartTime;
                            logger.trace("Поток {} батч {}/{}: +{} строк за {} мс ({} rows/sec)",
                                threadId, i + inserted, rowsToGenerate, inserted, batchTime,
                                (int)(inserted / (batchTime / 1000.0)));
                        }
                    }
                    
                } catch (Exception e) {
                    logger.error("Ошибка в потоке {}", threadId, e);
                    throw e;
                } finally {
                    MDC.remove("threadId");
                }
                
                long totalTime = System.currentTimeMillis() - startTime;
                double rowsPerSecond = rowsInserted / (totalTime / 1000.0);
                
                return new GenerateResult(threadId, rowsInserted, totalTime, rowsPerSecond);
            }
            
            private int generateBatch(Connection threadConn, TableMetadata metadata, 
                                     int batchSize, long offset) throws SQLException, IOException {
                
                StringBuilder data = new StringBuilder(batchSize * 100);
                Random random = new Random(offset + threadId * 1000000L);
                
                for (int i = 0; i < batchSize; i++) {
                    long rowId = offset + i;
                    List<String> values = new ArrayList<>();
                    
                    for (TableMetadata.ColumnInfo col : metadata.insertableColumns) {
                        // Передаем map с constraints
                        values.add(col.generateRandomValue(random, rowId, metadata.columnCheckConstraints));
                    }
                    
                    data.append(String.join(",", values)).append("\n");
                }
                
                // Используем правильный синтаксис с указанием NULL для CSV формата
                String copySQL = String.format(
                    "COPY %s (%s) FROM STDIN WITH (FORMAT CSV, NULL '\\N')",
                    tableName, metadata.getColumnNamesForInsert()
                );
                
                // Получаем оригинальное PostgreSQL соединение из прокси
                PGConnection pgConnection;
                if (threadConn.isWrapperFor(PGConnection.class)) {
                    pgConnection = threadConn.unwrap(PGConnection.class);
                } else {
                    pgConnection = (PGConnection) threadConn;
                }
                
                CopyManager copyManager = null;
                
                try {
                    copyManager = new CopyManager((BaseConnection) pgConnection);
                    
                    // Для отладки логируем первые несколько строк
                    if (logger.isDebugEnabled() && offset == 0 && batchSize > 0) {
                        String[] lines = data.toString().split("\n");
                        logger.debug("Пример первых 3 строк данных для потока {}:", threadId);
                        for (int j = 0; j < Math.min(3, lines.length); j++) {
                            logger.debug("  Строка {}: {}", j+1, lines[j]);
                        }
                    }
                    
                    long inserted = copyManager.copyIn(
                        copySQL,
                        new StringReader(data.toString())
                    );
                    threadConn.commit();
                    
                    if (logger.isTraceEnabled()) {
                        logger.trace("Батч {} строк вставлен", inserted);
                    }
                    return (int) inserted;
                } catch (SQLException e) {
                    // При ошибке логируем первые 200 символов данных для диагностики
                    String errorData = data.substring(0, Math.min(200, data.length()));
                    logger.error("Ошибка COPY. Первые 200 символов данных: {}", errorData);
                    throw e;
                } finally {
                    if (copyManager != null) {
                        copyManager = null;
                    }
                }
            }
        }
        
        static class GenerateResult {
            final int threadId;
            final long rowsInserted;
            final long timeMs;
            final double rowsPerSecond;
            
            GenerateResult(int threadId, long rowsInserted, long timeMs, double rowsPerSecond) {
                this.threadId = threadId;
                this.rowsInserted = rowsInserted;
                this.timeMs = timeMs;
                this.rowsPerSecond = rowsPerSecond;
            }
        }
    }
    
    // Масштабировщик данных (многопоточный)
    static class TableScaler {
        private static final Logger logger = LoggerFactory.getLogger(TableScaler.class);
        
        void scaleData(String sourceTable, String targetTable, int scaleFactor, 
                      int threadCount, int batchSize) throws SQLException, InterruptedException {
            
            if (targetTable == null || targetTable.isEmpty()) {
                targetTable = sourceTable + "_scaled";
            }
            
            logger.info("=== НАЧАЛО МАСШТАБИРОВАНИЯ ===");
            logger.info("Источник: {}, цель: {}, фактор: {}, потоков: {}, батч: {}", 
                sourceTable, targetTable, scaleFactor, threadCount, batchSize);
            
            long startTime = System.currentTimeMillis();
            
            long sourceRows;
            TableMetadata sourceMetadata;
            
            // Получаем метаданные и количество строк
            try (Connection conn = getConnection()) {
                sourceMetadata = new TableMetadata(conn, sourceTable);
                sourceRows = getRowCount(conn, sourceTable);
                
                if (sourceRows == 0) {
                    throw new SQLException("Исходная таблица пуста");
                }
                
                // Создаем целевую таблицу
                sourceMetadata.createTargetTable(conn, targetTable);
            }
            
            long totalTargetRows = sourceRows * scaleFactor;
            logger.debug("Исходных строк: {}, целевых строк: {}", sourceRows, totalTargetRows);
            
            // Распределяем работу по потокам
            List<ScaleTask> tasks = new ArrayList<>();
            long totalOperations = sourceRows * scaleFactor;
            long rowsPerThread = totalOperations / threadCount;
            long remainingRows = totalOperations % threadCount;
            long startOffset = 0;
            
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            
            for (int i = 0; i < threadCount; i++) {
                long threadRows = rowsPerThread + (i < remainingRows ? 1 : 0);
                
                if (threadRows > 0) {
                    tasks.add(new ScaleTask(
                        i, sourceMetadata, targetTable, sourceRows,
                        startOffset, threadRows, batchSize
                    ));
                    startOffset += threadRows;
                }
            }
            
            logger.debug("Распределение задач: {} потоков, по ~{} строк", 
                tasks.size(), rowsPerThread);
            
            // Запускаем потоки
            List<Future<ScaleResult>> futures = executor.invokeAll(tasks);
            
            // Собираем результаты
            long totalInserted = 0;
            
            for (Future<ScaleResult> future : futures) {
                try {
                    ScaleResult result = future.get();
                    totalInserted += result.rowsInserted;
                    
                    logger.debug("Поток {} завершен: +{} строк ({:.2f} rows/sec)",
                        result.threadId, result.rowsInserted, result.rowsPerSecond);
                    
                } catch (ExecutionException e) {
                    logger.error("Ошибка в потоке масштабирования", e.getCause());
                }
            }
            
            long totalTime = System.currentTimeMillis() - startTime;
            double speed = totalInserted / (totalTime / 1000.0);
            
            try (Connection conn = getConnection()) {
                long finalCount = getRowCount(conn, targetTable);
                logger.info("=== МАСШТАБИРОВАНИЕ ЗАВЕРШЕНО ===");
                logger.info("Вставлено: {} строк за {} мс ({:.2f} rows/sec)", 
                    totalInserted, totalTime, speed);
                logger.info("Финальное количество строк в {}: {}", targetTable, finalCount);
            }
            
            executor.shutdown();
        }
        
        private long getRowCount(Connection conn, String tableName) throws SQLException {
            String sql = "SELECT COUNT(*) FROM " + tableName;
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                if (rs.next()) {
                    return rs.getLong(1);
                }
            }
            return 0;
        }
        
        // Задача для потока масштабирования
        class ScaleTask implements Callable<ScaleResult> {
            private final int threadId;
            private final TableMetadata metadata;
            private final String targetTable;
            private final long sourceRows;
            private final long startOffset;
            private final long rowsToInsert;
            private final int batchSize;
            private final Logger logger = LoggerFactory.getLogger(ScaleTask.class);
            
            ScaleTask(int threadId, TableMetadata metadata, String targetTable,
                     long sourceRows, long startOffset, long rowsToInsert, int batchSize) {
                this.threadId = threadId;
                this.metadata = metadata;
                this.targetTable = targetTable;
                this.sourceRows = sourceRows;
                this.startOffset = startOffset;
                this.rowsToInsert = rowsToInsert;
                this.batchSize = batchSize;
            }
            
            @Override
            public ScaleResult call() throws Exception {
                MDC.put("threadId", String.valueOf(threadId));
                
                logger.debug("Поток {} запущен: {} строк (offset: {})", 
                    threadId, rowsToInsert, startOffset);
                
                long rowsInserted = 0;
                long startTime = System.currentTimeMillis();
                
                try (Connection threadConn = getConnection()) {
                    threadConn.setAutoCommit(false);
                    
                    for (long i = 0; i < rowsToInsert; i += batchSize) {
                        int currentBatchSize = (int) Math.min(batchSize, rowsToInsert - i);
                        
                        long batchStartTime = System.currentTimeMillis();
                        int inserted = copyBatch(threadConn, currentBatchSize, 
                                               (startOffset + i) % sourceRows);
                        
                        rowsInserted += inserted;
                        
                        if (logger.isTraceEnabled()) {
                            long batchTime = System.currentTimeMillis() - batchStartTime;
                            logger.trace("Поток {} батч {}/{}: +{} строк за {} мс ({} rows/sec)",
                                threadId, i + inserted, rowsToInsert, inserted, batchTime,
                                (int)(inserted / (batchTime / 1000.0)));
                        }
                    }
                    
                } catch (Exception e) {
                    logger.error("Ошибка в потоке {}", threadId, e);
                    throw e;
                } finally {
                    MDC.remove("threadId");
                }
                
                long totalTime = System.currentTimeMillis() - startTime;
                double rowsPerSecond = rowsInserted / (totalTime / 1000.0);
                
                return new ScaleResult(threadId, rowsInserted, totalTime, rowsPerSecond);
            }
            
            private int copyBatch(Connection threadConn, int batchSize, long offset) 
                    throws SQLException {
                
                String selectSQL = String.format(
                    // "SELECT %s FROM %s ORDER BY id LIMIT %d OFFSET %d",
                    "SELECT %s FROM %s LIMIT %d",
                    metadata.getColumnNamesForSelect(),
                    metadata.tableName,
                    batchSize
                    // , offset
                );
                
                // Используем правильный синтаксис с указанием NULL для CSV формата
                String copySQL = String.format(
                    "COPY %s (%s) FROM STDIN WITH (FORMAT CSV, NULL '\\N')",
                    targetTable, metadata.getColumnNamesForInsert()
                );
                
                // Получаем оригинальное PostgreSQL соединение из прокси
                PGConnection pgConnection;
                if (threadConn.isWrapperFor(PGConnection.class)) {
                    pgConnection = threadConn.unwrap(PGConnection.class);
                } else {
                    pgConnection = (PGConnection) threadConn;
                }
                
                CopyManager copyManager = null;
                
                try (Statement stmt = threadConn.createStatement();
                     ResultSet rs = stmt.executeQuery(selectSQL);
                     PipedOutputStream pos = new PipedOutputStream();
                     PipedInputStream pis = new PipedInputStream(pos)) {
                    
                    Thread writerThread = new Thread(() -> {
                        try (PrintWriter writer = new PrintWriter(new OutputStreamWriter(pos))) {
                            int rowCount = 0;
                            while (rs.next()) {
                                List<String> values = new ArrayList<>();
                                for (TableMetadata.ColumnInfo col : metadata.insertableColumns) {
                                    Object obj = rs.getObject(col.name);
                                    if (obj == null) {
                                        values.add("\\N");  // NULL маркер для CSV формата
                                    } else {
                                        String str = obj.toString();
                                        if (str.contains(",") || str.contains("\"") || str.contains("\n")) {
                                            str = "\"" + str.replace("\"", "\"\"") + "\"";
                                        }
                                        values.add(str);
                                    }
                                }
                                writer.println(String.join(",", values));
                                rowCount++;
                                
                                if (rowCount % 1000 == 0) {
                                    writer.flush();
                                }
                            }
                            writer.flush();
                        } catch (SQLException e) {
                            throw new RuntimeException("Ошибка при чтении результата", e);
                        }
                    });
                    
                    writerThread.start();
                    
                    copyManager = new CopyManager((BaseConnection) pgConnection);
                    long inserted = copyManager.copyIn(copySQL, pis);
                    
                    writerThread.join();
                    threadConn.commit();
                    
                    if (logger.isTraceEnabled()) {
                        logger.trace("Батч {} строк скопирован", inserted);
                    }
                    return (int) inserted;
                    
                } catch (InterruptedException | IOException e) {
                    throw new SQLException("Ошибка при копировании батча", e);
                } finally {
                    if (copyManager != null) {
                        copyManager = null;
                    }
                }
            }
        }
        
        static class ScaleResult {
            final int threadId;
            final long rowsInserted;
            final long timeMs;
            final double rowsPerSecond;
            
            ScaleResult(int threadId, long rowsInserted, long timeMs, double rowsPerSecond) {
                this.threadId = threadId;
                this.rowsInserted = rowsInserted;
                this.timeMs = timeMs;
                this.rowsPerSecond = rowsPerSecond;
            }
        }
    }
    
    // Парсер аргументов с поддержкой параметров подключения
    static class ArgsParser {
        private static final Logger logger = LoggerFactory.getLogger(ArgsParser.class);
        
        // Параметры подключения
        private String host = DEFAULT_HOST;
        private String port = DEFAULT_PORT;
        private String database = DEFAULT_DB;
        private String user = DEFAULT_USER;
        private String password = DEFAULT_PASSWORD;
        private String schema = DEFAULT_SCHEMA;
        private int poolSize = DEFAULT_POOL_SIZE;
        private boolean autoCommit = false;
        private int connectionTimeout = DEFAULT_CONNECTION_TIMEOUT;
        private int maxLifetime = DEFAULT_MAX_LIFETIME;
        
        // Параметры операции
        private String tableIn = null;
        private String tableOut = null;
        private Integer rows = null;
        private Integer scale = null;
        private Integer threads = 1;
        private Integer batchSize = 1000;
        private boolean generate = false;
        
        ArgsParser(String[] args) {
            for (int i = 0; i < args.length; i++) {
                try {
                    switch (args[i]) {
                        // Параметры подключения
                        case "--host":
                            if (i + 1 < args.length) host = args[++i];
                            break;
                        case "--port":
                            if (i + 1 < args.length) port = args[++i];
                            break;
                        case "--db":
                            if (i + 1 < args.length) database = args[++i];
                            break;
                        case "--user":
                            if (i + 1 < args.length) user = args[++i];
                            break;
                        case "--password":
                            if (i + 1 < args.length) password = args[++i];
                            break;
                        case "--schema":
                            if (i + 1 < args.length) schema = args[++i];
                            break;
                        case "--pool-size":
                            if (i + 1 < args.length) poolSize = Integer.parseInt(args[++i]);
                            break;
                        case "--auto-commit":
                            if (i + 1 < args.length) autoCommit = Boolean.parseBoolean(args[++i]);
                            break;
                        case "--connection-timeout":
                            if (i + 1 < args.length) connectionTimeout = Integer.parseInt(args[++i]);
                            break;
                        case "--max-lifetime":
                            if (i + 1 < args.length) maxLifetime = Integer.parseInt(args[++i]);
                            break;
                            
                        // Параметры операции
                        case "-i":
                        case "--table_in":
                            if (i + 1 < args.length) tableIn = args[++i];
                            break;
                        case "-o":
                        case "--table_out":
                            if (i + 1 < args.length) tableOut = args[++i];
                            break;
                        case "-g":
                        case "--generate":
                            generate = true;
                            break;
                        case "-n":
                        case "--rows":
                            if (i + 1 < args.length) rows = Integer.parseInt(args[++i]);
                            break;
                        case "-s":
                        case "--scale":
                            if (i + 1 < args.length) scale = Integer.parseInt(args[++i]);
                            break;
                        case "-t":
                        case "--threads":
                            if (i + 1 < args.length) threads = Integer.parseInt(args[++i]);
                            break;
                        case "-b":
                        case "--batch_size":
                            if (i + 1 < args.length) batchSize = Integer.parseInt(args[++i]);
                            break;
                    }
                } catch (NumberFormatException e) {
                    logger.error("Неверный числовой параметр для {}", args[i], e);
                }
            }
            
            logger.debug("Параметры: host={}, port={}, db={}, user={}, schema={}, poolSize={}", 
                host, port, database, user, schema, poolSize);
        }
        
        boolean isValid() {
            return tableIn != null && (generate || scale != null);
        }
        
        boolean hasGenerate() {
            return generate && rows != null && rows > 0;
        }
        
        boolean hasScale() {
            return scale != null && scale > 0;
        }
        
        // Геттеры
        String getHost() { return host; }
        String getPort() { return port; }
        String getDatabase() { return database; }
        String getUser() { return user; }
        String getPassword() { return password; }
        String getSchema() { return schema; }
        int getPoolSize() { return poolSize; }
        boolean isAutoCommit() { return autoCommit; }
        int getConnectionTimeout() { return connectionTimeout; }
        int getMaxLifetime() { return maxLifetime; }
        
        String getTableIn() { return tableIn; }
        String getTableOut() { return tableOut; }
        int getRows() { return rows != null ? rows : 0; }
        int getScale() { return scale != null ? scale : 0; }
        int getThreads() { return threads; }
        int getBatchSize() { return batchSize; }
    }
}