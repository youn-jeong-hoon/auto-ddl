package com.example.ddl;

import com.example.ddl.constant.Jdbc;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.metamodel.EntityType;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.annotations.Nationalized;
import org.hibernate.boot.Metadata;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Environment;
import org.hibernate.mapping.Column;
import org.hibernate.mapping.Component;
import org.hibernate.mapping.ForeignKey;
import org.hibernate.mapping.KeyValue;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.mapping.Property;
import org.hibernate.mapping.Selectable;
import org.hibernate.mapping.Table.ForeignKeyKey;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class Starter implements ApplicationListener<ApplicationStartedEvent> {

  @Value("${spring.datasource.driver-class-name}")
  private String driver;
  @Value("${spring.datasource.url}")
  private String url;
  @Value("${spring.datasource.username}")
  private String username;
  @Value("${spring.datasource.password}")
  private String password;
  @Value("${spring.jpa.properties.hibernate.dialect}")
  private String dialect;
  @Value("${property.schema.global}")
  private String global;
  @Value("${property.schema.arh}")
  private String arh;

  private String databaseType;

  @Autowired
  private EntityManagerFactory entityManagerFactory;

  @Override
  public void onApplicationEvent(ApplicationStartedEvent event) {
    log.info("----------DML SERVICE START----------");
    Connection conn = null;
    StandardServiceRegistry registry = getRegistry();

    databaseType = getDatabaseType();

    try {
      conn = getConnection();
      if (conn == null) {
        log.error("Connection is null");
        return;
      }

      conn.setAutoCommit(false);

      // entity class metadata
      Metadata metadata = getMetadata(registry);
      // database metadata
      DatabaseMetaData dbMeta = conn.getMetaData();

      List<String> missingTables = getMissingTables(metadata, dbMeta);
      Map<String, List<ColumnDto>> missingColumns = getMissingColumns(metadata, dbMeta,
          missingTables);

      if (missingColumns.isEmpty() && missingTables.isEmpty()) {
        // 모든 테이블과 컬럼이 존재하는 경우 return
        log.info("No missing columns or tables found.");
        return;
      }

      createValidationReport(missingTables, missingColumns);

      runQuery(conn, metadata, missingTables, missingColumns);
      conn.commit();

    } catch (Exception e) {
      if (conn != null) {
        try {
          conn.rollback();
        } catch (SQLException ex) {
          log.error("Error rolling back transaction: {}", ex.getMessage());
        }
      }
    }
    finally {
      if (conn != null) {
        try {
          conn.close();
        } catch (SQLException e) {
          log.error("Error closing connection: {}", e.getMessage());
        }
      }
    }

    // registry close
    if (registry != null) {
      StandardServiceRegistryBuilder.destroy(registry);
    }

    log.info("----------DML SERVICE END----------");
  }

  private void runQuery(Connection conn, Metadata metadata, List<String> missingTables,
      Map<String, List<ColumnDto>> missingColumns) throws Exception {

    try (Statement stmt = conn.createStatement()) {
      for (PersistentClass entity : metadata.getEntityBindings()) {
        if (isCreate(entity, missingTables)) {
          // 테이블이 존재하지 않는 경우 (테이블을 생성해야 할 경우)
          createTable(stmt, entity);
          createSequence(stmt, entity);

          // index 생성 쿼리 실행
          createIndex(stmt, entity);
        }

        // add column
        alterTable(stmt, entity, missingColumns);
      }

      // FK는 테이블을 다 생성한 후에 생성해야 함.
      for (PersistentClass entity : metadata.getEntityBindings()) {
        if (isCreate(entity, missingTables)) {
          createFk(stmt, metadata, entity, missingTables);
        }
      }


    } catch (Exception e) {
      log.error("Error executing SQL script", e);
      throw e;
    }

    log.info("SQL script executed successfully.");
  }

  /**
   * foreign key를 생성한다.
   */
  private void createFk(Statement stmt, Metadata metadata, PersistentClass entity,
      List<String> missingTables) throws Exception {

    if (!missingTables.contains(getFullTableName(entity))) {
      return;
    }

    // 테이블을 생성해야 하는 경우 외래키 조회
    Set<Entry<ForeignKeyKey, ForeignKey>> foreignKeys = entity.getTable().getForeignKeys()
        .entrySet();

    if (foreignKeys.isEmpty()) {
      return;
    }

    for (Entry<ForeignKeyKey, ForeignKey> entry : foreignKeys) {
      ForeignKey fk = entry.getValue();
      String referencedColumnNames;

      if (!fk.getReferencedColumns().isEmpty()) {
        referencedColumnNames = fk.getReferencedColumns().stream()
            .map(Selectable::getText)
            .collect(Collectors.joining(", "));
      } else {
        PersistentClass referencedEntity = metadata.getEntityBinding(fk.getReferencedEntityName());
        List<String> tmp = new ArrayList<>();

        for (Column col : referencedEntity.getTable().getPrimaryKey().getColumns()) {
          tmp.add(col.getName());
        }
        referencedColumnNames = tmp.stream()
            .collect(Collectors.joining(", "));
      }

      String foreignKeyName = fk.getName();
      String referencedTableName = fk.getReferencedTable().getName();

      StringBuilder sb = new StringBuilder();
      sb.append("ALTER TABLE ").append(getFullTableName(entity));
      sb.append(" ADD CONSTRAINT ").append(foreignKeyName);
      sb.append(" FOREIGN KEY (").append(fk.getColumns().stream()
          .map(Selectable::getText)
          .collect(Collectors.joining(", ")));
      sb.append(") REFERENCES ").append(referencedTableName).append("(");
      sb.append(referencedColumnNames);
      sb.append(")");

      try {
        log.info("Creating foreign key: {}", sb.toString());
        stmt.execute(sb.toString());
      } catch (Exception e) {
        log.error("Error creating foreign key: {}", e.getMessage());
        throw e;
      }
    }
  }

  /**
   * 컬럼 추가. 운영 중인 고객사의 경우 컬럼 삭제나 변경은 위험할 수 있으므로, 컬럼 추가만 지원함.
   */
  private void alterTable(Statement stmt, PersistentClass entity,
      Map<String, List<ColumnDto>> missingColumns)
      throws NoSuchFieldException, SQLException {

    String fullTableName = getFullTableName(entity);

    List<ColumnDto> columns = missingColumns.get(fullTableName);

    if (columns == null || columns.isEmpty()) {
      return;
    }

    for (ColumnDto dto : columns) {
      StringBuilder sb = new StringBuilder();

      sb.append("ALTER TABLE ").append(fullTableName);
      sb.append(" ADD").append("maria".equalsIgnoreCase(databaseType) ? "COLUMN" : "");
      sb.append(" ").append(getColumnName(dto.column())); /* @Column annotation 의 name */
      sb.append(" ").append(getSQLType(entity, dto.property()));

      log.info(sb.toString());
      stmt.addBatch(sb.toString());
    }

    stmt.executeBatch();
    stmt.clearBatch();
  }

  private void createIndex(Statement stmt, PersistentClass entity) throws Exception {

    Map<String, org.hibernate.mapping.Index> indexes = entity.getTable().getIndexes();

    if (indexes == null || indexes.isEmpty()) {
      return;
    }

    // 인덱스 생성 쿼리
    for (Entry<String, org.hibernate.mapping.Index> entry : indexes.entrySet()) {
      String indexName = entry.getKey();
      org.hibernate.mapping.Index index = entry.getValue();

      String columnList = index.getSelectables()
          .stream()
          .map(Selectable::getText)
          .collect(Collectors.joining(", "));

      if (index.isUnique()) {
        // unique index인 경우
        String uniqueIndexSQL = "ALTER TABLE " + getFullTableName(entity) +
            " ADD CONSTRAINT " + indexName + " UNIQUE (" + index.getSelectables()
            .stream()
            .map(Selectable::getText)
            .collect(Collectors.joining(", ")) + ")";
        try {
          log.info("Creating unique index: {}", uniqueIndexSQL);
          stmt.execute(uniqueIndexSQL);
        } catch (Exception e) {
          log.error("Error creating unique index: {}", e.getMessage());
          throw e;
        }
      } else {
        // 일반 인덱스인 경우
        String createIndexSQL =
            "CREATE INDEX " + indexName + " ON " + getFullTableName(entity) + "(" + columnList
                + ")";
        try {
          log.info("Creating index: {}", createIndexSQL);
          stmt.execute(createIndexSQL);
        } catch (Exception e) {
          log.error("Error creating index: {}", e.getMessage());
          throw e;
        }
      }
    }
  }

  /**
   * create sequence
   */
  private void createSequence(Statement stmt, PersistentClass entity) throws SQLException {
    StringBuilder sb = new StringBuilder();
    Class<?> entityClass = entity.getMappedClass();

    if (!entityClass.isAnnotationPresent(SequenceGenerator.class)) {
      return;
    }

    // entity에 @GeneratedValue(strategy = GenerationType.IDENTITY) 컬럼이 존재하는 경우
    List<Property> primaryProperties = getPrimaryProperties(entity);

    for (Property property : primaryProperties) {
      Field field;
      try {
        field = entityClass.getDeclaredField(property.getName());
      } catch (NoSuchFieldException e) {
        throw new RuntimeException(e);
      }
      if (field.isAnnotationPresent(GeneratedValue.class)) {
        GeneratedValue gv = field.getAnnotation(GeneratedValue.class);
        if (gv.strategy() == GenerationType.IDENTITY && ("maria".equalsIgnoreCase(databaseType)
            || "mssql".equalsIgnoreCase(databaseType))) {
          // mariadb, mssql의 경우 @GeneratedValue(strategy = GenerationType.IDENTITY)인 경우 시퀀스 생성하지 않음
          return;
        }
      }
    }

    SequenceGenerator sg = entityClass.getAnnotation(SequenceGenerator.class);

    String name = sg.name();
    String tmpSequenceName = sg.sequenceName();
    String sequenceName = getSequenceName(tmpSequenceName);
    String catalog = getSequenceCatalog(entity);
    String schema = getSequenceSchema(entity);

    String sequenceFullName = getSequenceFullName(catalog, schema, sequenceName);

    // sequence 존재 여부 조회
    if (existSequence(stmt, sequenceName)) {
      log.debug("시퀀스 {} 존재", sequenceName);
      return;
    }

    int allocationSize = sg.allocationSize();
    int initialValue = sg.initialValue();

    log.debug(">>> @SequenceGenerator Found:");
    log.debug("Name: {}", name);
    log.debug("Sequence Name: {}", sequenceName);
    log.debug("Allocation Size: {}", allocationSize);
    log.debug("Initial Value: {}", initialValue);

    // mariadb, mssql의 경우 use database 추가
    if (databaseType.contains("maria") || databaseType.contains("mssql")) {
      sb.append("USE ").append(catalog).append(";\n");
    }

    sb.append("CREATE SEQUENCE ").append(sequenceFullName).append("\n");

    sb.append("  START WITH ").append(initialValue).append("\n");
    sb.append("  INCREMENT BY 1\n");

    if (allocationSize > 1) {
      sb.append("  CACHE ").append(allocationSize).append("\n");
    }

    log.info("create sequence: {}", sb.toString());

    stmt.execute(sb.toString());
  }

  /**
   * 시퀀스 존재 여부 확인
   *
   * @param stmt         Statement
   * @param sequenceName schema.tableName 형태가 아닌 tableName 형태로 시퀀스 이름만 전달
   */
  private boolean existSequence(Statement stmt, String sequenceName) throws SQLException {
    String sql;

    if ("mssql".equalsIgnoreCase(databaseType)) {
      sql = "SELECT COUNT(*) FROM sys.sequences WHERE name = '" + sequenceName + "'";
    } else if ("oracle".equalsIgnoreCase(databaseType)) {
      sql = "SELECT COUNT(*) FROM user_sequences WHERE sequence_name = '" + sequenceName + "'";
    } else {
      sql = "SHOW CREATE SEQUENCE " + sequenceName;
    }

    try {
      ResultSet rs = stmt.executeQuery(sql);
      if (rs.next()) {
        return rs.getInt(1) > 0;
      }
    } catch (SQLException e) {
      if (databaseType.contains("maria") && e.getMessage().contains("doesn't exist")) {
        // mariadb의 경우 SHOW CREATE SEQUENCE 쿼리에서 시퀀스가 존재하지 않는 경우 exception 발생
        return false;
      }

      throw e;
    }

    return false;
  }

  private String getSequenceFullName(String catalog, String schema, String sequenceName) {
    if (databaseType.contains("mssql")) {
      // dbo.시퀀스명
      return schema + "." + sequenceName;
    }

    if (databaseType.contains("oracle")) {
      return catalog + "." + sequenceName;
    }

    return sequenceName;
  }

  private String getSequenceName(String tmpSequenceName) {
    if (tmpSequenceName.contains(".")) {
      String[] split = tmpSequenceName.split("\\.");
      return split[split.length - 1];
    }

    return tmpSequenceName;
  }

  private String getSequenceSchema(PersistentClass entity) {
    Class<?> entityClass = entity.getMappedClass();
    SequenceGenerator sg = entityClass.getAnnotation(SequenceGenerator.class);

    String tmpSequenceName = sg.sequenceName();
    String schema;

    // mssql의 경우 무조건 dbo
    if (databaseType.contains("mssql")) {
      return "dbo";
    }

    if (tmpSequenceName.contains(".")) {
      // 1순위: sequenceName에 schema가 포함되어 있는 경우
      String[] split = tmpSequenceName.split("\\.");
      schema = split[0];
    } else if (sg.schema() != null && !sg.schema().isEmpty()) {
      // 2순위: @SequenceGenerator에 schema가 있는 경우
      schema = sg.schema();
    } else {
      // 3순위: entity.getTable().getSchema()에 schema가 있는 경우
      schema = entity.getTable().getSchema();
    }

    return schema;
  }

  private String getSequenceCatalog(PersistentClass entity) {
    Class<?> entityClass = entity.getMappedClass();
    SequenceGenerator sg = entityClass.getAnnotation(SequenceGenerator.class);

    String tmpSequenceName = sg.sequenceName();
    String catalog;

    if (tmpSequenceName.contains(".")) {
      // 1순위: sequenceName에 catalog가 포함되어 있는 경우
      String[] split = tmpSequenceName.split("\\.");
      catalog = split[0];
    } else if (sg.catalog() != null && !sg.catalog().isEmpty()) {
      // 2순위: @SequenceGenerator에 catalog가 있는 경우
      catalog = sg.catalog();
    } else {
      // 3순위: entity.getTable().getCatalog()에 catalog가 있는 경우
      catalog = entity.getTable().getCatalog();
    }

    if ("global".equalsIgnoreCase(catalog)) {
      catalog = global;
    } else if ("arh".equalsIgnoreCase(catalog)) {
      catalog = arh;
    }

    return catalog;
  }

  /**
   * create table 쿼리를 구한다.
   */
  private void createTable(Statement stmt, PersistentClass entity)
      throws NoSuchFieldException, SQLException {

    stmt.execute("CREATE TABLE " + getFullTableName(entity) + " (\n"
        + getPrimaryColumns(entity)
        + getNormalColumns(entity)
        + getPrimaryKey(entity)
        + "\n)");
  }

  /**
   * PRIMARY KEY 문자열을 구한다.
   * @return PRIMARY KEY 문자열
   */
  private String getPrimaryKey(PersistentClass entity) {
    StringBuilder sb = new StringBuilder();
    List<String> primaryKeyColumns = new ArrayList<>();

    entity.getTable().getPrimaryKey().getColumns()
        .forEach(column -> primaryKeyColumns.add(column.getName().toUpperCase()));

    sb.append("    PRIMARY KEY (");

    for (String columnName : primaryKeyColumns) {
      sb.append(columnName).append(", ");
    }

    // 마지막 ", " 제거
    if (sb.length() > 2) {
      sb.setLength(sb.length() - 2);
    }
    sb.append(")");

    return sb.toString();
  }

  /**
   * 일반 컬럼을 구한다.
   * @param entity entity class
   * @return 컬럼 목록
   * @throws NoSuchFieldException
   */
  private String getNormalColumns(PersistentClass entity) throws NoSuchFieldException {
    StringBuilder sb = new StringBuilder();

    for (Property property : entity.getPropertyClosure()) {

      if (property.getValue() instanceof Component) {
        continue;
      }

      org.hibernate.mapping.Value value = property.getValue();

      for (Selectable selectable : value.getSelectables()) {
        if (selectable instanceof Column column) {

          // @JoinColumn 이 붙은 경우는 제외
          Field field = getField(entity, property);
          if (field.isAnnotationPresent(JoinColumn.class)) {
            continue;
          }

          String columnName = getColumnName(column);
          String columnType = getSQLType(entity, property);

          sb.append("    ").append(columnName).append(" ").append(columnType).append(",\n");
        }
      }
    }
    return sb.toString();
  }

  /**
   * 컬럼의 길이를 구한다.
   */
  private int getColumnLength(Property property) {
    int columnLength = 255; // 기본 길이 설정
    org.hibernate.mapping.Value value = property.getValue();

    for (Selectable selectable : value.getSelectables()) {
      if (selectable instanceof Column column) {
        columnLength = column.getColumnSize();
      }
    }

    // max 값 1024
    return Math.min(columnLength, 1024);
  }

  /**
   * DB 유형을 구한다.
   * @return oracle / mssql / maria
   */
  private String getDatabaseType() {
    if (url.contains("oracle")) {
      return "oracle";
    } else if (url.contains("sqlserver")) {
      return "mssql";
    } else if (url.contains("mariadb")) {
      return "maria";
    }
    return "";
  }

  /**
   * 컬럼의 SQL 타입을 구한다.
   */
  private String getSQLType(PersistentClass entity, Property property)
      throws NoSuchFieldException {

    int length = getColumnLength(property);

    String propertyName = property.getName();

    Class<?> entityClass = entity.getMappedClass();

    Field field;

    try {
      field = entityClass.getDeclaredField(propertyName);
    } catch (NoSuchFieldException ignore) {
      return "";
    }

    String fieldType = field.getType().getSimpleName().toLowerCase();

    switch (fieldType) {
      case "localdatetime", "instant":
        return switch (databaseType) {
          case "oracle" -> "DATE";
          default -> "DATETIME";
        };
      case "int", "integer":
        if ("oracle".equalsIgnoreCase(databaseType)) {
          return "NUMBER(10)";
        }
        return "INT";
      case "long":
        if ("oracle".equalsIgnoreCase(databaseType)) {
          return "NUMBER(19)";
        }
        return "BIGINT";
      case "string":
        String sqlType;

        if ("maria".equalsIgnoreCase(databaseType)) {
          sqlType = "VARCHAR";
        } else {
          sqlType = field.isAnnotationPresent(Nationalized.class) ? "NVARCHAR" : "VARCHAR";
        }

        return switch (databaseType) {
          case "mssql" -> sqlType + "(" + length + ")";
          case "oracle" -> sqlType + "2(" + length + ")";
          case "maria" -> {
            if (length > 1000) {
              yield "TEXT";
            }
            yield sqlType + "(" + length + ")";
          }
          default -> sqlType + "(" + length + ")";
        };
      case "boolean":
        return switch (databaseType) {
          case "mssql" -> "BIT";
          case "oracle" -> "NUMBER(1)";
          case "maria" -> "BOOLEAN";
          default -> "BOOLEAN";
        };
      case "date":
        return switch (databaseType) {
          case "mssql", "maria" -> "DATETIME";
          case "oracle" -> "DATE";
          default -> "DATETIME";
        };
      case "double":
        return switch (databaseType) {
          case "oracle" -> "NUMBER(19, 4)";
          case "mssql" -> "FLOAT";
          default -> "DOUBLE";
        };
      case "float":
        return switch (databaseType) {
          case "oracle" -> "NUMBER(19, 4)";
          default -> "FLOAT";
        };
      default:
        return "VARCHAR(" + length + ")";
    }
  }

  /**
   * property 로 Field 객체를 구한다.
   */
  private Field getField(PersistentClass entity, Property property) {
    Class<?> entityClass = entity.getMappedClass();
    String propertyName = property.getName();

    Field field;

    try {
      field = entityClass.getDeclaredField(propertyName);
    } catch (NoSuchFieldException e) {
      throw new RuntimeException(e);
    }

    return field;
  }

  /**
   * 테이블의 PK를 구한다.
   */
  private List<Property> getPrimaryProperties(PersistentClass entity) {
    List<Property> props = new ArrayList<>();

    // @Id 단일 필드
    if (entity.getIdentifierProperty() != null) {
      props.add(entity.getIdentifierProperty());
    } else {
      // @EmbeddedId or composite key (복합 키인 경우)
      KeyValue id = entity.getIdentifier();
      if (id instanceof Component component) {
        component.getPropertyIterator()
            .forEachRemaining(props::add); // Hibernate 6.x 기준
      }
    }

    return props;
  }

  private String getPrimaryColumns(PersistentClass entity) {
    StringBuilder sb = new StringBuilder();

    getPrimaryProperties(entity)
        .forEach(property -> property.getColumns()
            .forEach(column -> {
              String columnType;
              try {
                columnType = getSQLType(entity, property);
              } catch (NoSuchFieldException e) {
                throw new RuntimeException(e);
              }

              sb.append("    ").append(column.getName()).append(" ")
                  .append(columnType).append(" ")
                  .append(getIdentityKeyword(entity, property)) /* @GeneratedValue 가 붙은 컬럼의 경우 */
                  .append(" NOT NULL")
                  .append(",\n")
              ;
            }));

    return sb.toString();
  }

  /**
   * mssql, maria의 경우 auto_increment, identity(1, 1) 속성을 추가한다.
   */
  private String getIdentityKeyword(PersistentClass entity, Property property) {
    // @GeneratedValue 가 붙은 컬럼의 경우
    String propertyName = property.getName();
    Class<?> entityClass = entity.getMappedClass();

    Field field = null;

    try {
      field = entityClass.getDeclaredField(propertyName);
    } catch (NoSuchFieldException ignore) {
      // ignore
    }

    if (field != null && field.isAnnotationPresent(GeneratedValue.class)) {
      // GeneratedValue 속성의 값이 IDENTITY인 경우
      GeneratedValue gv = field.getAnnotation(GeneratedValue.class);
      if (gv.strategy() != GenerationType.SEQUENCE) {
        return switch (databaseType) {
          case "mssql" -> "IDENTITY(1, 1)";
          case "maria" -> "AUTO_INCREMENT";
          default -> "";
        };
      }
    }

    return "";
  }

  /**
   * 테이블을 생성할지 여부
   * @return true: 테이블을 생성해야 함, false: 테이블을 생성하지 않아야 함
   */
  private boolean isCreate(PersistentClass entity, List<String> missingTables) {
    String fullTableName = getFullTableName(entity);

    boolean isCreate = false;

    // create table, create sequence
    log.debug("DB table name: {}", fullTableName);
    for (String table : missingTables) {
      log.debug("missing table: {}", table);

      if (table.equalsIgnoreCase(fullTableName)) {
        // missingTables에 존재하는 경우 create query를 생성해야 함.
        isCreate = true;
      }
    }

    return isCreate;
  }

  /**
   * 테이블과 컬럼이 존재하지 않는 경우 리포트를 생성한다.
   *
   * @param missingTables  존재하지 않는 테이블 목록
   * @param missingColumns 존재하지 않는 컬럼 목록
   */
  private void createValidationReport(List<String> missingTables,
      Map<String, List<ColumnDto>> missingColumns) {

    // 결과 리포트 생성
    log.info("=====================================");
    log.info("📝 Schema Validation Report");
    log.info("Generated at: {}\n", new Date());

    log.info("📌 missing columns:");
    for (Entry<String, List<ColumnDto>> entry : missingColumns.entrySet()) {
      log.info("- {}", entry.getKey());
      for (ColumnDto dto : entry.getValue()) {
        log.info("   - {}", dto.column().getName());
      }
    }

    log.info("\n📌 missing tables:");
    for (String table : missingTables) {
      log.info("- {}", table);
    }

    log.info("=====================================");
  }

  private Map<String, List<ColumnDto>> getMissingColumns(Metadata metadata, DatabaseMetaData dbMeta,
      List<String> missingTables) {
    Map<String, List<ColumnDto>> missingColumns = new LinkedHashMap<>();

    for (PersistentClass entity : metadata.getEntityBindings()) {
      String fullTableName = getFullTableName(entity);
      // missingTables에 있는 경우는 skip
      if (missingTables.contains(fullTableName)) {
        log.debug("skip table: {}", fullTableName);
        continue;
      }

      // DB의 실제 컬럼 목록
      Set<String> actualColumns = getActualColumns(dbMeta, entity);

      // print actualColumns
      log.info("DB real column list: {}", actualColumns);

      for (Property property : entity.getPropertyClosure()) {
        org.hibernate.mapping.Value value = property.getValue();
        for (Selectable selectable : value.getSelectables()) {
          if (selectable instanceof Column column) {
            String columnName = column.getName().toUpperCase();

            log.debug("entity column : {}", getColumnName(column));

            // oracle인 경우 getColumnName에 콤마가 포함되어 넘어오는 경우 있음
            if (!actualColumns.contains(getColumnName(column).replace("\"", ""))) {
              missingColumns.computeIfAbsent(fullTableName, k -> new ArrayList<>())
                  .add(new ColumnDto(property, column));
              log.info("❌ 누락된 컬럼: {}.{}", fullTableName, columnName);
            }
          }
        }
      }
    }

    log.info("missing columns: {}", missingColumns);

    return missingColumns;
  }

  /**
   * 컬럼 이름을 가져온다.
   *
   * @param column 컬럼
   * @return 컬럼 이름 - 대문자
   */
  private String getColumnName(Column column) {
    return column.getName().toUpperCase();
  }

  /**
   * 실제 DB 테이블에 존재하는 컬럼 목록을 가져온다.
   *
   * @param dbMeta db metadata
   * @param entity entity class
   * @return 컬럼 목록
   */
  private Set<String> getActualColumns(DatabaseMetaData dbMeta, PersistentClass entity) {

    Set<String> actualColumns = new HashSet<>();

    log.info("{}, {}, {}", getCatalog(entity), getSchema(entity), getTableName(entity));

    try (ResultSet rs = dbMeta.getColumns(getCatalog(entity), getSchema(entity),
        getTableName(entity), null)) {
      while (rs.next()) {
        actualColumns.add(rs.getString("COLUMN_NAME").toUpperCase());
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

    return actualColumns;
  }

  /**
   * entity class의 metadata를 가져와서 db에 존재하지 않는 테이블을 찾는다.
   *
   * @param metadata entity class의 metadata
   * @param dbMeta   db metadata
   * @return 존재하지 않는 테이블 목록
   */
  private List<String> getMissingTables(Metadata metadata, DatabaseMetaData dbMeta) {
    List<String> missingTables = new ArrayList<>();

    for (PersistentClass entity : metadata.getEntityBindings()) {
      // 테이블이 존재하지 않는 경우
      log.debug("search table:{}.{}.{}", getCatalog(entity), getSchema(entity), getTableName(
          entity));
      try (ResultSet rs = dbMeta.getTables(getCatalog(entity), getSchema(entity),
          getTableName(entity),
          new String[]{"TABLE"})) {
        if (!rs.next()) {
          missingTables.add(getFullTableName(entity));
        }
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }

    log.info("missing tables: {}", missingTables);

    return missingTables;
  }

  private StandardServiceRegistry getRegistry() {
    return new StandardServiceRegistryBuilder()
        .applySetting(Jdbc.DRIVER, driver)
        .applySetting(Jdbc.URL, url)
        .applySetting(Jdbc.USER, username)
        .applySetting(Jdbc.PASS, password)
        .applySetting(Environment.DIALECT, dialect)
        .build();
  }

  private String getTableName(PersistentClass entity) {
    String tableName = entity.getTable().getName();
    if (url.contains("oracle")) {
      tableName = tableName.toUpperCase();
    }
    return tableName;
  }

  /**
   * 현재 프로젝트에 등록된 모든 entity class의 metadata를 가져온다.
   */
  public Metadata getMetadata(StandardServiceRegistry registry) {
    MetadataSources sources = new MetadataSources(registry);

    for (Class<?> entityClass : getAllEntityClasses()) {
      log.debug("Entity Class: {}", entityClass.getName());
      sources.addAnnotatedClass(entityClass);
    }

    return sources.buildMetadata();
  }

  /**
   * 현재 프로젝트에 등록된 모든 entity class를 가져온다.
   */
  public Set<Class<?>> getAllEntityClasses() {
    return entityManagerFactory
        .getMetamodel()
        .getEntities()
        .stream()
        .map(EntityType::getJavaType)
        .collect(Collectors.toSet());
  }

  public Connection getConnection() {
    try {
      return DriverManager.getConnection(url, username, password);
    } catch (SQLException e) {
      log.error("Error getting connection: {}", e.getMessage());
    }
    return null;
  }

  /**
   * catalog 이름을 가져온다.
   *
   * @return oracle: null, mssql/mariadb: catalog name
   */
  private String getCatalog(PersistentClass entity) {

    // oracle인 경우 catalog를 사용하지 않음
    if (url.contains("oracle")) {
      return null;
    }

    String catalog = entity.getTable().getCatalog();

    if ("global".equalsIgnoreCase(catalog)) {
      catalog = global;
    } else if ("arh".equalsIgnoreCase(catalog)) {
      catalog = arh;
    }

    return catalog;
  }

  /**
   * schema 이름을 가져온다.
   *
   * @return mssql: "dbo", oracle: catalog name, mariadb: null
   */
  private String getSchema(PersistentClass entity) {

    // mssql인 경우 schema는 무조건 dbo
    if (url.contains("sqlserver")) {
      return "dbo";
    }

    if (url.contains("oracle")) {
      // oracle인 경우 catalog 값으로 schema를 가져와야 함
      String catalog = entity.getTable().getCatalog();

      if ("global".equals(catalog)) {
        return global;
      } else if ("arh".equals(catalog)) {
        return arh;
      }
    }

    // mariadb인 경우 schema를 사용하지 않음
    return null;
  }

  /**
   * 테이블의 전체 이름을 가져온다.
   *
   * @return mssql: catalog.schema.table, oracle: schema.table, mariadb: catalog.table
   */
  private String getFullTableName(PersistentClass entity) {
    if (url.contains("sqlserver")) {
      return getCatalog(entity) + "." + getSchema(entity) + "." + getTableName(entity);
    }
    if (url.contains("oracle")) {
      return getSchema(entity) + "." + getTableName(entity);
    }

    return getCatalog(entity) + "." + getTableName(entity);
  }
}
