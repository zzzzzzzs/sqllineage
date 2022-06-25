package com.zzzzzzzs.sqllineage.core;

import cn.hutool.core.io.file.FileReader;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.jparams.store.Store;
import com.jparams.store.memory.MemoryStore;
import com.zzzzzzzs.sqllineage.bean.ColumnInfo;
import com.zzzzzzzs.sqllineage.bean.SqlInfo;
import com.zzzzzzzs.sqllineage.bean.TableInfo;
import com.zzzzzzzs.sqllineage.tuple.Tuple2;
import lombok.SneakyThrows;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.*;
import java.util.function.Supplier;

public class ParseSql {
  FrameworkConfig config;
  ObjectMapper jsonSql = new ObjectMapper();
  ArrayNode sqlNodes = jsonSql.createArrayNode();
  LinkedHashSet<ColumnInfo> lastColumnInfos = new LinkedHashSet<>(); // 记录上一次的列信息
  //  String lastTableInfo; // 记录上一次的表名
  // name, alias
  Tuple2<String, String> lastTableInfo = null; // 记录上一次的表名
  // 有限状态机
  private Map<Tuple2<String, String>, HandlerSql<String, String, Table<String, String, TableInfo>>>
      stateMachine = new HashMap<>();

  // 有限状态机
  public ParseSql() {
    stateMachine.put(
        Tuple2.of("INIT", "table"),
        // real table, tableInfos
        (var1, tableInfos) -> {
          return var1.toString();
        });

    stateMachine.put(
        Tuple2.of("table", "real"),
        // real table, tableInfos
        (var1, tableInfos) -> {
          int level = 0;
          if (tableInfos.containsRow(lastTableInfo.f0)) {
            level =
                tableInfos.row(lastTableInfo.f0).values().stream().findFirst().get().getLevel() + 1;
          } else {
            level = 1;
          }
          TableInfo tableInfo =
              TableInfo.builder()
                  .tableName(var1)
                  .alias("")
                  .columns(new LinkedHashSet<>())
                  .level(level)
                  .build();
          tableInfos.put(var1, "", tableInfo);
          lastTableInfo.f0 = var1;
          return var1.toString();
        });
    stateMachine.put(
        Tuple2.of("real", "as"),
        // alias, tableInfos
        (var1, tableInfos) -> {
          TableInfo tableInfo = tableInfos.row(lastTableInfo.f0).values().stream().findFirst().get();
          tableInfo.setAlias(var1);
          lastTableInfo.f1 = var1;
          tableInfos.remove(lastTableInfo.f0, lastTableInfo.f1);
          tableInfos.put(lastTableInfo.f0, lastTableInfo.f1, tableInfo);
          return var1.toString();
        });

    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    config =
        Frameworks.newConfigBuilder()
            .parserConfig(
                SqlParser.configBuilder()
                    .setParserFactory(SqlParserImpl.FACTORY)
                    .setCaseSensitive(false)
                    .setQuoting(Quoting.BACK_TICK)
                    .setQuotedCasing(Casing.UNCHANGED)
                    .setUnquotedCasing(Casing.UNCHANGED)
                    .setConformance(SqlConformanceEnum.DEFAULT)
                    .build())
            .build();
  }

  public void init() {
    lastTableInfo = new Tuple2<>();
    lastColumnInfos.clear();
    sqlNodes.removeAll();
  }

  // parse select
  @SneakyThrows
  public String parseSelect(String sql) {
    // TODO: 高并发下有问题
    init();
    if (sql == null || sql.isEmpty()) {
      FileReader fileReader = new FileReader("sql/aquery009.sql");
      sql = fileReader.readString();
    }
    sql = sql.trim();
    if (sql.endsWith(";")) {
      sql = sql.substring(0, sql.length() - 1);
    }
    SqlParser parser = SqlParser.create(sql, config.getParserConfig());
    SqlNode sqlNode = parser.parseStmt();
    // table,alias,TableInfo
//    Table<String, String, TableInfo> tableInfos = HashBasedTable.create();
    Store<SqlInfo> sqlInfoStore = new MemoryStore<>();
    sqlInfoStore.index("tableName", SqlInfo::getTableName)

    ;
    // 默认真实名字
    handlerSql(sqlNode, tableInfos, new LinkedList<>(List.of("INIT")));
    //    String ret = map2Json(tableInfos);

    //    System.out.println("tableInfos" + jsonSql.writeValueAsString(tableInfos));
    // System.out.println(jsonSql.writerWithDefaultPrettyPrinter().writeValueAsString(tableInfos));
    //    return jsonSql.writerWithDefaultPrettyPrinter().writeValueAsString(tableInfos);
    return tableInfos.toString();
    //    return ret;
  }

  //  private String map2Json(Table<String, String, TableInfo> tableInfos) {
  //    String ret = null;
  //    try {
  //      int lastLevel = 1;
  //      int lastTop = -100;
  //      int lastLeft = 0;
  //
  //      for (Map.Entry<String, TableInfo> entry : tableInfos.entrySet()) {
  //        String key = entry.getKey();
  //        TableInfo value = entry.getValue();
  //
  //        String node =
  //            SqlJson.nodeStr
  //                .replace("$tableName", value.getTableName())
  //                .replace(
  //                    "$columns",
  //                    // jsonSql.writeValueAsString(value.getColumns()));
  //                    jsonSql.writeValueAsString(
  //                        value.getColumns().stream()
  //                            .map(
  //                                ele -> {
  //                                  try {
  //                                    return jsonSql.readTree(
  //                                        SqlJson.columnStr.replace(
  //                                            "$name",
  //                                            ele.getAlias() == null
  //                                                ? ele.getName()
  //                                                : (ele.getName() + "|" + ele.getAlias())));
  //                                  } catch (JsonProcessingException e) {
  //                                    e.printStackTrace();
  //                                  }
  //                                  return null;
  //                                })
  //                            .toArray()));
  //
  //        if (1 == value.getLevel()) {
  //          node = node.replace("$type", "Origin");
  //        } else if ("res" == value.getTableName()) {
  //          node = node.replace("$type", "RS");
  //        } else {
  //          node = node.replace("$type", "Middle");
  //        }
  //        if (lastLevel == value.getLevel()) {
  //          lastTop += 200;
  //        } else {
  //          lastLevel = value.getLevel();
  //          lastTop = 100;
  //          lastLeft += 150;
  //        }
  //        node =
  //            node.replace("$top", String.valueOf(lastTop))
  //                .replace("$left", String.valueOf(lastLeft));
  //        sqlNodes.add(jsonSql.readTree(node));
  //      }
  //      // edge
  //      ArrayNode sqlEdges = jsonSql.createArrayNode();
  //      lastLevel = 2;
  //      // column, table
  //      // 上一次
  //      Multimap<String, String> lastColTable = ArrayListMultimap.create();
  //      // 本次
  //      Multimap<String, String> colTable = ArrayListMultimap.create();
  //      for (Map.Entry<String, TableInfo> entry : tableInfos.entrySet()) {
  //        String key = entry.getKey();
  //        TableInfo value = entry.getValue();
  //
  //        if (1 == value.getLevel()) {
  //          value.getColumns().forEach(el -> lastColTable.put(el.getName(),
  // value.getTableName()));
  //          continue;
  //        }
  //        if ((lastLevel == value.getLevel())) {
  //          value.getColumns().forEach(el -> colTable.put(el.getName(), value.getTableName()));
  //        } else {
  //          lastColTable.clear();
  //          lastColTable.putAll(colTable);
  //          colTable.clear();
  //          value.getColumns().forEach(el -> colTable.put(el.getName(), value.getTableName()));
  //          lastLevel++;
  //        }
  //        for (ColumnInfo el : value.getColumns()) {
  //          if (lastColTable.containsKey(el.getName())) {
  //            for (String s : lastColTable.get(el.getName())) {
  //              System.out.println(s + " " + el.getName() + " " + value.getTableName());
  //              sqlEdges.add(
  //                  jsonSql.readTree(
  //                      SqlJson.edgeStr
  //                          .replace("$1", el.getName())
  //                          .replace("$2", s)
  //                          .replace("$3", el.getName())
  //                          .replace("$4", value.getTableName())));
  //            }
  //          }
  //        }
  //      }
  //      ret =
  //          SqlJson.res.replace("$edges", sqlEdges.toString()).replace("$nodes",
  // sqlNodes.toString());
  //    } catch (JsonProcessingException e) {
  //      e.printStackTrace();
  //    }
  //    return ret;
  //  }

  // handle sqlnode
  private void handlerSql(
      SqlNode sqlNode, Table<String, String, TableInfo> tableInfos, LinkedList<String> flags) {
    if (sqlNode == null) return;
    SqlKind kind = sqlNode.getKind();
    switch (kind) {
      case INSERT:
        handlerInsert(sqlNode);
        break;
      case SELECT:
        handlerSelect(sqlNode, tableInfos, flags);
        break;
      case JOIN:
        handlerJoin(sqlNode, tableInfos, flags);
        break;
      case AS:
        handlerAs(sqlNode, tableInfos, flags);
        break;
      case UNION:
        break;
      case ORDER_BY:
        handlerOrderBy(sqlNode, tableInfos, flags);
        break;
      case WITH:
        handleWith(sqlNode, tableInfos, flags);
        break;
      case WITH_ITEM:
        handleWithItem(sqlNode, tableInfos, flags);
        break;
      case IDENTIFIER:
        // 表名
        handlerIdentifier(sqlNode, tableInfos, flags);
        break;
      case OTHER:
        // 列名
        handlerOther(sqlNode, tableInfos, flags);
        break;
      default:
        break;
    }
  }

  // handle with
  private void handleWith(
      SqlNode sqlNode, Table<String, String, TableInfo> tableInfos, LinkedList<String> flags) {
    SqlWith with = (SqlWith) sqlNode;
    List<@Nullable SqlNode> withList = with.withList.getList();
    for (SqlNode node : withList) {
      handlerSql(node, tableInfos, flags);
    }
    handlerSql(with.body, tableInfos, null);
  }

  // handler with item
  private void handleWithItem(
      SqlNode sqlNode, Table<String, String, TableInfo> tableInfos, LinkedList<String> flags) {
    SqlWithItem withItem = (SqlWithItem) sqlNode;
    handlerSql(withItem.query, tableInfos, flags);
    handlerSql(withItem.name, tableInfos, null);
  }

  // handle order by
  // TODO 后期可以从 orderBy 中获取到列的名称补全列名
  private void handlerOrderBy(
      SqlNode sqlNode, Table<String, String, TableInfo> tableInfos, LinkedList<String> flags) {
    SqlOrderBy orderBy = (SqlOrderBy) sqlNode;
    SqlNode query = orderBy.query;
    handlerSql(query, tableInfos, flags);
  }

  // handle join
  private void handlerJoin(
      SqlNode sqlNode, Table<String, String, TableInfo> tableInfos, LinkedList<String> flags) {
    SqlJoin join = (SqlJoin) sqlNode;
    SqlNode left = join.getLeft();
    SqlNode right = join.getRight();
    flags.add("join");
    handlerSql(left, tableInfos, flags);
    handlerSql(right, tableInfos, flags);
  }

  // handle select
  private void handlerSelect(
      SqlNode sqlNode, Table<String, String, TableInfo> tableInfos, LinkedList<String> flags) {
    SqlSelect select = (SqlSelect) sqlNode;
    SqlNode from = select.getFrom();
    flags.addAll(List.of("table", "real"));
    handlerSql(from, tableInfos, flags);
    SqlNode where = select.getWhere();
    handlerSql(where, null, null);
    //    try {
    //      from.getClass().getField("names");
    //    } catch (NoSuchFieldException e) {
    //      System.out.println("no names");
    //    }
    flags.clear();
    flags.addAll(List.of("INIT", "column", "real"));
    SqlNode selectList = select.getSelectList();
    handlerSql(selectList, tableInfos, flags);
    // TODO 后期处理
    //    SqlNode groupBy = select.getGroup();
    //    handlerSql(groupBy, null, null);
    SqlNode having = select.getHaving();
    handlerSql(having, null, null);
    SqlNodeList orderList = select.getOrderList();
    handlerSql(orderList, null, null);
  }

  // handle insert
  private void handlerInsert(SqlNode sqlNode) {
    SqlInsert sqlInsert = (SqlInsert) sqlNode;
    SqlNode insertList = sqlInsert.getTargetTable();
    handlerSql(insertList, null, null);
    SqlNode source = sqlInsert.getSource();
    handlerSql(source, null, null);
  }

  private void handlerAs(
      SqlNode sqlNode, Table<String, String, TableInfo> tableInfos, LinkedList<String> flags) {
    SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
    List<SqlNode> operandList = sqlBasicCall.getOperandList();

    SqlNode left = operandList.get(0);
    handlerSql(left, tableInfos, flags);
    SqlNode right = operandList.get(1);
    flags.add("as");
    handlerSql(right, tableInfos, flags);

    //    if (Flag.COLUMN.equals(flag)) {
    //      ColumnInfo columnInfo =
    //          ColumnInfo.builder().name(left.toString()).alias(right.toString()).build();
    //      // 获取最后一个表名
    //      tableInfos.row(lastTableInfo.f0).values().stream()
    //          .findFirst()
    //          .get()
    //          .getColumns()
    //          .add(columnInfo);
    //      //      tableInfos.get(tableInfos.lastKey()).getColumns().add(columnInfo);
    //    } else {
    //      handlerSql(left, tableInfos, Flag.REAL);
    //      // 左是名字，那么右就是别名
    //      if (SqlKind.IDENTIFIER.equals(left.getKind())) {
    //        handlerSql(right, tableInfos, Flag.ALIAS);
    //      } else {
    //        handlerSql(right, tableInfos, Flag.REAL);
    //      }
    //    }
  }

  /**
   * 列名，但是包含别名
   *
   * @param sqlNode
   * @param tableInfos
   */
  private void handlerOther(
      SqlNode sqlNode, Table<String, String, TableInfo> tableInfos, LinkedList<String> flags) {
    List<@Nullable SqlNode> list = ((SqlNodeList) sqlNode).getList();
    TableInfo tableInfo = null;
    //    if (Flag.WITH_BODY.equals(flag)) {
    //      //      tableInfo = tableInfos.get("res");
    //    } else {
    //      tableInfo = tableInfos.row(lastTableInfo.f0).values().stream().findFirst().get();
    //    }
    for (int i = 0, size = flags.size() - 1; i < size; i++) {
      System.out.println(flags.get(i) + ":" + flags.get(i + 1));
    }
    for (SqlNode node : list) {
      handlerSql(node, tableInfos, flags);
    }
    //    lastColumnInfos.clear();
    //    for (SqlNode node : list) {
    //      if (SqlKind.AS.equals(node.getKind())) { // 处理列别名
    //        handlerSql(node, tableInfos, null);
    //      } else {
    //        ColumnInfo columnInfo = ColumnInfo.builder().name(node.toString()).build();
    //        lastColumnInfos.add(columnInfo);
    //        tableInfo.getColumns().add(columnInfo);
    //      }
    //    }
  }

  /**
   * 表名
   *
   * @param sqlNode
   * @param tableInfos
   * @param flags 名字标识符
   */
  private void handlerIdentifier(
      SqlNode sqlNode, Table<String, String, TableInfo> tableInfos, LinkedList<String> flags) {
    SqlIdentifier sqlIdentifier = (SqlIdentifier) sqlNode;
    TableInfo tableInfo = null;
    //    int level = 0;
    //    if (tableInfos.containsRow(lastTableInfo.f0)) {
    //      level = tableInfos.row(lastTableInfo.f0).values().stream().findFirst().get().getLevel()
    // + 1;
    //    } else {
    //      level = 1;
    //    }
    for (int i = 0, size = flags.size() - 1; i < size; i++) {
      System.out.println(flags.get(i) + ":" + flags.get(i + 1));
      stateMachine
          .get(Tuple2.of(flags.get(i), flags.get(i + 1)))
          .handler(sqlIdentifier.getSimple(), tableInfos);
    }

    //    String nextState = stateMachine.get(Tuple2.of(flags., event.eventType)).get();
    //    if (Flag.REAL.equals(flag)) {
    //      // 第一次遇到真实表命名
    //      if (tableInfos.isEmpty() || tableInfos.row(sqlIdentifier.getSimple()).size() == 0) {
    //        tableInfo =
    //            TableInfo.builder()
    //                .tableName(sqlIdentifier.getSimple())
    //                .alias(new String())
    //                .columns(new LinkedHashSet<>())
    //                .level(level)
    //                .build();
    //        tableInfos.put(sqlIdentifier.getSimple(), "", tableInfo);
    //      }
    //      lastTableInfo.f0 = sqlIdentifier.getSimple();
    //    } else if (Flag.ALIAS.equals(flag)) {
    //      tableInfo = tableInfos.row(lastTableInfo.f0).values().stream().findFirst().get();
    //      tableInfo.setAlias(sqlIdentifier.getSimple());
    //      lastTableInfo.f1 = sqlIdentifier.getSimple();
    //      tableInfos.remove(lastTableInfo.f0, lastTableInfo.f1);
    //      tableInfos.put(lastTableInfo.f0, lastTableInfo.f1, tableInfo);
    //    } else if (Flag.WITH_ITEM.equals(flag)) {
    //      tableInfo =
    //          TableInfo.builder()
    //              .tableName(sqlIdentifier.getSimple())
    //              .alias(new String())
    //              .columns(new LinkedHashSet<>(lastColumnInfos))
    //              .level(level)
    //              .build();
    //      //      tableInfos.put(sqlIdentifier.getSimple(), tableInfo);
    //    } else if (Flag.WITH_BODY.equals(flag)) {
    //      //      tableInfo =
    //      //          TableInfo.builder()
    //      //              .tableName("res")
    //      //              .alias(new String())
    //      //              .columns(new LinkedHashSet<>())
    //      //              .level(level)
    //      //              .build();
    //      //      tableInfos.put("res", tableInfo);
    //    }
    //        lastTableName = sqlIdentifier.getSimple();
  }

  public static void main(String[] args) {
    ParseSql parseSql = new ParseSql();
    parseSql.parseSelect("");
  }
}
