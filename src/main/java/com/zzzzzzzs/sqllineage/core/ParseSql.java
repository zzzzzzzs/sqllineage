package com.zzzzzzzs.sqllineage.core;

import cn.hutool.core.io.file.FileReader;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.zzzzzzzs.sqllineage.bean.*;
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

public class ParseSql {
  FrameworkConfig config;
  ObjectMapper jsonSql = new ObjectMapper();
  ArrayNode sqlNodes = jsonSql.createArrayNode();
  LinkedHashSet<ColumnInfo> lastColumnInfos = new LinkedHashSet<>(); // 记录上一次的列信息

  // 有限状态机
  public ParseSql() {

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
    lastColumnInfos.clear();
    sqlNodes.removeAll();
  }

  // parse select
  @SneakyThrows
  public String parseSelect(String sql) {
    String uuid = UUID.randomUUID().toString();
    Table<SqlInfo> table = new Table();
    table.createTable(SqlInfo.class);
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

    // 默认真实名字
    handlerSql(sqlNode, table, uuid, Flag.REAL);
    //    String ret = map2Json(tableInfos);

    //    System.out.println("tableInfos" + jsonSql.writeValueAsString(tableInfos));
    // System.out.println(jsonSql.writerWithDefaultPrettyPrinter().writeValueAsString(tableInfos));
    //    return jsonSql.writerWithDefaultPrettyPrinter().writeValueAsString(tableInfos);
    //    return tableInfos.toString();
    //    return ret;
    table.print();
    return table.toString();
  }

  //  private String map2Json(Table<SqlInfo> table) {
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
  private void handlerSql(SqlNode sqlNode, Table<SqlInfo> table, String uuid, Flag flags) {
    if (sqlNode == null) return;
    SqlKind kind = sqlNode.getKind();
    switch (kind) {
      case INSERT:
        handlerInsert(sqlNode);
        break;
      case SELECT:
        handlerSelect(sqlNode, table, uuid, flags);
        break;
      case JOIN:
        handlerJoin(sqlNode, table, uuid, flags);
        break;
      case AS:
        handlerAs(sqlNode, table, uuid, flags);
        break;
      case UNION:
        break;
      case ORDER_BY:
        handlerOrderBy(sqlNode, table, uuid, flags);
        break;
      case WITH:
        handleWith(sqlNode, table, uuid, flags);
        break;
      case WITH_ITEM:
        handleWithItem(sqlNode, table, uuid, flags);
        break;
      case IDENTIFIER:
        // 表名
        handlerIdentifier(sqlNode, table, uuid, flags);
        break;
      case OTHER:
        // 列名
        handlerOther(sqlNode, table, uuid, flags);
        break;
      default:
        break;
    }
  }

  // handle with
  private void handleWith(SqlNode sqlNode, Table<SqlInfo> table, String uuid, Flag flags) {
    SqlWith with = (SqlWith) sqlNode;
    List<@Nullable SqlNode> withList = with.withList.getList();
    for (SqlNode node : withList) {
      handlerSql(node, table, uuid, flags);
    }
    handlerSql(with.body, table, uuid, Flag.WITH_BODY);
  }

  // handler with item
  private void handleWithItem(SqlNode sqlNode, Table<SqlInfo> table, String uuid, Flag flags) {
    SqlWithItem withItem = (SqlWithItem) sqlNode;
    handlerSql(withItem.query, table, uuid, Flag.WITH_ITEM);
    handlerSql(withItem.name, table, uuid, Flag.WITH_NAME);
  }

  // handle order by
  // TODO 后期可以从 orderBy 中获取到列的名称补全列名
  private void handlerOrderBy(SqlNode sqlNode, Table<SqlInfo> table, String uuid, Flag flags) {
    SqlOrderBy orderBy = (SqlOrderBy) sqlNode;
    SqlNode query = orderBy.query;
    handlerSql(query, table, uuid, flags);
  }

  // handle join
  private void handlerJoin(SqlNode sqlNode, Table<SqlInfo> table, String uuid, Flag flags) {
    SqlJoin join = (SqlJoin) sqlNode;
    SqlNode left = join.getLeft();
    SqlNode right = join.getRight();
    handlerSql(left, table, uuid, flags);
    handlerSql(right, table, uuid, flags);
  }

  // handle select
  private void handlerSelect(SqlNode sqlNode, Table<SqlInfo> table, String uuid, Flag flags) {
    SqlSelect select = (SqlSelect) sqlNode;
    SqlNode from = select.getFrom();
    handlerSql(from, table, uuid, flags);
    SqlNode where = select.getWhere();
    handlerSql(where, table, uuid, null);
    //    try {
    //      from.getClass().getField("names");
    //    } catch (NoSuchFieldException e) {
    //      System.out.println("no names");
    //    }
    SqlNode selectList = select.getSelectList();
    handlerSql(selectList, table, uuid, flags);
    // TODO 后期处理
    //    SqlNode groupBy = select.getGroup();
    //    handlerSql(groupBy, null, null);
    SqlNode having = select.getHaving();
    handlerSql(having, table, uuid, null);
    SqlNodeList orderList = select.getOrderList();
    handlerSql(orderList, table, uuid, null);
  }

  // handle insert
  private void handlerInsert(SqlNode sqlNode) {
    SqlInsert sqlInsert = (SqlInsert) sqlNode;
    SqlNode insertList = sqlInsert.getTargetTable();
    handlerSql(insertList, null, null, null);
    SqlNode source = sqlInsert.getSource();
    handlerSql(source, null, null, null);
  }

  private void handlerAs(SqlNode sqlNode, Table<SqlInfo> table, String uuid, Flag flags) {
    SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
    List<SqlNode> operandList = sqlBasicCall.getOperandList();

    SqlNode left = operandList.get(0);
    SqlNode right = operandList.get(1);

    if (Flag.COLUMN.equals(flags)) { // 列名 & REAL
      handlerSql(left, table, uuid, Flag.COLUMN_REAL);
      handlerSql(right, table, uuid, Flag.COLUMN_ALIAS);
    } else {
      handlerSql(left, table, uuid, Flag.REAL);
      // 左是名字，那么右就是别名，否则就是子函数
      if (SqlKind.IDENTIFIER.equals(left.getKind())) {
        handlerSql(right, table, uuid, Flag.ALIAS);
      } else {
        handlerSql(right, table, uuid, Flag.REAL);
      }
    }
  }

  /**
   * 列名，但是包含别名
   *
   * @param sqlNode
   * @param table
   */
  private void handlerOther(SqlNode sqlNode, Table<SqlInfo> table, String uuid, Flag flags) {
    List<@Nullable SqlNode> list = ((SqlNodeList) sqlNode).getList();
    for (SqlNode node : list) {
      if (SqlKind.AS.equals(node.getKind())) { // 处理列别名
        handlerSql(node, table, uuid, Flag.COLUMN);
      } else {
        findLastInfo(table, uuid)
            .map(
                info -> {
                  if (info.getColumnName() == null) {
                    info.setColumnName(node.toString());
                    return null;
                  } else {
                    SqlInfo sqlInfo =
                        SqlInfo.builder()
                            .uuid(uuid)
                            .tableName(info.getTableName())
                            .tableAlias(info.getTableAlias())
                            .columnName(node.toString())
                            .level(info.getLevel())
                            .build();
                    return sqlInfo;
                  }
                })
            .ifPresent(table::insert);
      }
    }
  }

  /**
   * 表名 & 列名
   *
   * @param sqlNode
   * @param table
   * @param flag 名字标识符
   */
  private void handlerIdentifier(SqlNode sqlNode, Table<SqlInfo> table, String uuid, Flag flag) {
    SqlIdentifier sqlIdentifier = (SqlIdentifier) sqlNode;
    // table 获取最大 level
    int level =
        table.selectWhere("uuid", uuid).stream().mapToInt(c -> c.getLevel()).max().orElse(0);

    if (Flag.REAL.equals(flag)) {
      SqlInfo sqlInfo =
          SqlInfo.builder()
              .tableName(sqlIdentifier.getSimple())
              .level(level + 1)
              .uuid(uuid)
              .build();
      table.insert(sqlInfo);
    } else if (Flag.ALIAS.equals(flag)) { // 别名
      findLastInfo(table, uuid).ifPresent(c -> c.setTableAlias(sqlIdentifier.getSimple()));
    } else if (Flag.COLUMN_REAL.equals(flag)) {
      findLastInfo(table, uuid)
          .map(
              c -> {
                if (c.getColumnName() == null) {
                  c.setColumnName(sqlIdentifier.getSimple());
                  return null;
                } else {
                  SqlInfo sqlInfo =
                      SqlInfo.builder()
                          .uuid(uuid)
                          .tableName(c.getTableName())
                          .tableAlias(c.getTableAlias())
                          .columnName(sqlIdentifier.getSimple())
                          .level(level)
                          .build();
                  return sqlInfo;
                }
              })
          .ifPresent(table::insert);
    } else if (Flag.COLUMN_ALIAS.equals(flag)) {
      findLastInfo(table, uuid).ifPresent(c -> c.setColumnAlias(sqlIdentifier.getSimple()));
    } else if (Flag.WITH_ITEM.equals(flag)) {
      SqlInfo sqlInfo =
          SqlInfo.builder()
              .tableName(sqlIdentifier.getSimple())
              .level(level + 1)
              .uuid(uuid)
              .build();
      table.insert(sqlInfo);
    } else if (Flag.WITH_NAME.equals(flag)) {
      findLastInfoALL(table, uuid).stream()
          .map(
              c -> {
                SqlInfo info =
                    SqlInfo.builder()
                        .tableName(sqlIdentifier.getSimple())
                        .columnName(c.getColumnName())
                        .columnAlias(c.getColumnAlias())
                        .level(level + 1)
                        .uuid(uuid)
                        .build();
                return info;
              })
          .forEach(table::insert);
    } else if (Flag.WITH_BODY.equals(flag)) {
      SqlInfo sqlInfo =
          SqlInfo.builder()
              .tableName(sqlIdentifier.getSimple())
              .level(level + 1)
              .uuid(uuid)
              .build();
      table.insert(sqlInfo);
    }
  }

  // 查找上一条数据
  private Optional<SqlInfo> findLastInfo(Table<SqlInfo> table, String uuid) {
    return table.selectWhere("uuid", uuid).stream().max(Comparator.comparing(SqlInfo::getId));
  }

  // 查找上一个表的所有记录
  private List<SqlInfo> findLastInfoALL(Table<SqlInfo> table, String uuid) {
    Optional<SqlInfo> lastInfoOpt = findLastInfo(table, uuid);
    return table.selectWhere(
        "uuid,tableName,tableAlias",
        uuid,
        lastInfoOpt.get().getTableName(),
        lastInfoOpt.get().getTableAlias());
  }

  public static void main(String[] args) {
    ParseSql parseSql = new ParseSql();
    parseSql.parseSelect("");
  }
}
