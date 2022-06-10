package com.zzzzzzzs.sqllineage.core;

import cn.hutool.core.io.file.FileReader;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.zzzzzzzs.sqllineage.bean.ColumnInfo;
import com.zzzzzzzs.sqllineage.bean.Flag;
import com.zzzzzzzs.sqllineage.bean.SqlJson;
import com.zzzzzzzs.sqllineage.bean.TableInfo;
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
import org.apache.commons.collections4.OrderedMap;
import org.apache.commons.collections4.map.ListOrderedMap;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ParseSql {
  FrameworkConfig config;
  ObjectMapper jsonSql = new ObjectMapper();
  ArrayNode sqlNodes = jsonSql.createArrayNode();
  LinkedHashSet<ColumnInfo> lastColumnInfos = new LinkedHashSet<>(); // 记录上一次的列信息
  String lastTableName; // 记录上一次的表名

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
    lastTableName = "";
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
    // table, TableInfo
    OrderedMap<String, TableInfo> tableInfoMaps = new ListOrderedMap<>();
    // 默认真实名字
    handlerSql(sqlNode, tableInfoMaps, Flag.REAL);
    String ret = map2Json(tableInfoMaps);

    //    System.out.println("tableInfoMaps" + jsonSql.writeValueAsString(tableInfoMaps));
    // System.out.println(jsonSql.writerWithDefaultPrettyPrinter().writeValueAsString(tableInfoMaps));
    //    return jsonSql.writerWithDefaultPrettyPrinter().writeValueAsString(tableInfoMaps);
    return ret;
  }

  private String map2Json(OrderedMap<String, TableInfo> tableInfoMaps) {
    String ret = null;
    try {
      int lastLevel = 1;
      int lastTop = -100;
      int lastLeft = 0;

      for (Map.Entry<String, TableInfo> entry : tableInfoMaps.entrySet()) {
        String key = entry.getKey();
        TableInfo value = entry.getValue();

        String node =
            SqlJson.nodeStr
                .replace("$tableName", value.getTableName())
                .replace(
                    "$columns",
                    // jsonSql.writeValueAsString(value.getColumns()));
                    jsonSql.writeValueAsString(
                        value.getColumns().stream()
                            .map(
                                ele -> {
                                  try {
                                    return jsonSql.readTree(
                                        SqlJson.columnStr.replace(
                                            "$name",
                                            ele.getAlias() == null
                                                ? ele.getName()
                                                : (ele.getName() + "|" + ele.getAlias())));
                                  } catch (JsonProcessingException e) {
                                    e.printStackTrace();
                                  }
                                  return null;
                                })
                            .toArray()));

        if (1 == value.getLevel()) {
          node = node.replace("$type", "Origin");
        } else if ("res" == value.getTableName()) {
          node = node.replace("$type", "RS");
        } else {
          node = node.replace("$type", "Middle");
        }
        if (lastLevel == value.getLevel()) {
          lastTop += 200;
        } else {
          lastLevel = value.getLevel();
          lastTop = 100;
          lastLeft += 150;
        }
        node =
            node.replace("$top", String.valueOf(lastTop))
                .replace("$left", String.valueOf(lastLeft));
        sqlNodes.add(jsonSql.readTree(node));
      }
      // edge
      ArrayNode sqlEdges = jsonSql.createArrayNode();
      lastLevel = 2;
      // column, table
      // 上一次
      Multimap<String, String> lastColTable = ArrayListMultimap.create();
      // 本次
      Multimap<String, String> colTable = ArrayListMultimap.create();
      for (Map.Entry<String, TableInfo> entry : tableInfoMaps.entrySet()) {
        String key = entry.getKey();
        TableInfo value = entry.getValue();

        if (1 == value.getLevel()) {
          value.getColumns().forEach(el -> lastColTable.put(el.getName(), value.getTableName()));
          continue;
        }
        if ((lastLevel == value.getLevel())) {
          value.getColumns().forEach(el -> colTable.put(el.getName(), value.getTableName()));
        } else {
          lastColTable.clear();
          lastColTable.putAll(colTable);
          colTable.clear();
          value.getColumns().forEach(el -> colTable.put(el.getName(), value.getTableName()));
          lastLevel++;
        }
        for (ColumnInfo el : value.getColumns()) {
          if (lastColTable.containsKey(el.getName())) {
            for (String s : lastColTable.get(el.getName())) {
              System.out.println(s + " " + el.getName() + " " + value.getTableName());
              sqlEdges.add(
                  jsonSql.readTree(
                      SqlJson.edgeStr
                          .replace("$1", el.getName())
                          .replace("$2", s)
                          .replace("$3", el.getName())
                          .replace("$4", value.getTableName())));
            }
          }
        }
      }
      ret =
          SqlJson.res.replace("$edges", sqlEdges.toString()).replace("$nodes", sqlNodes.toString());
    } catch (JsonProcessingException e) {
      e.printStackTrace();
    }
    return ret;
  }

  // handle sqlnode
  private void handlerSql(SqlNode sqlNode, OrderedMap<String, TableInfo> tableInfoMaps, Flag flag) {
    if (sqlNode == null) return;
    SqlKind kind = sqlNode.getKind();
    switch (kind) {
      case INSERT:
        handlerInsert(sqlNode);
        break;
      case SELECT:
        handlerSelect(sqlNode, tableInfoMaps, flag);
        break;
      case JOIN:
        handlerJoin(sqlNode, tableInfoMaps);
        break;
      case AS:
        handlerAs(sqlNode, tableInfoMaps, flag);
        break;
      case UNION:
        break;
      case ORDER_BY:
        handlerOrderBy(sqlNode, tableInfoMaps, flag);
        break;
      case WITH:
        handleWith(sqlNode, tableInfoMaps, flag);
        break;
      case WITH_ITEM:
        handleWithItem(sqlNode, tableInfoMaps, flag);
        break;
      case IDENTIFIER:
        // 表名
        handlerIdentifier(sqlNode, tableInfoMaps, flag);
        break;
      case OTHER:
        // 列名
        handlerOther(sqlNode, tableInfoMaps, flag);
        break;
      default:
        break;
    }
  }

  // handle with
  private void handleWith(SqlNode sqlNode, OrderedMap<String, TableInfo> tableInfoMaps, Flag flag) {
    SqlWith with = (SqlWith) sqlNode;
    List<@Nullable SqlNode> withList = with.withList.getList();
    for (SqlNode node : withList) {
      handlerSql(node, tableInfoMaps, flag);
    }
    handlerSql(with.body, tableInfoMaps, Flag.WITH_BODY);
  }

  // handler with item
  private void handleWithItem(
      SqlNode sqlNode, OrderedMap<String, TableInfo> tableInfoMaps, Flag flag) {
    SqlWithItem withItem = (SqlWithItem) sqlNode;
    handlerSql(withItem.query, tableInfoMaps, flag);
    handlerSql(withItem.name, tableInfoMaps, Flag.WITH_ITEM);
  }

  // handle order by
  // TODO 后期可以从 orderBy 中获取到列的名称补全列名
  private void handlerOrderBy(
      SqlNode sqlNode, OrderedMap<String, TableInfo> tableInfoMaps, Flag flag) {
    SqlOrderBy orderBy = (SqlOrderBy) sqlNode;
    SqlNode query = orderBy.query;
    handlerSql(query, tableInfoMaps, flag);
  }

  // handle join
  private void handlerJoin(SqlNode sqlNode, OrderedMap<String, TableInfo> tableInfoMaps) {
    SqlJoin join = (SqlJoin) sqlNode;
    SqlNode left = join.getLeft();
    SqlNode right = join.getRight();

    handlerSql(left, tableInfoMaps, Flag.REAL);
    handlerSql(right, tableInfoMaps, Flag.REAL);
  }

  // handle select
  private void handlerSelect(
      SqlNode sqlNode, OrderedMap<String, TableInfo> tableInfoMaps, Flag flag) {
    SqlSelect select = (SqlSelect) sqlNode;
    SqlNode from = select.getFrom();
    handlerSql(from, tableInfoMaps, flag);
    //    level.getAndIncrement();
    SqlNode where = select.getWhere();
    handlerSql(where, null, null);
    //    try {
    //      from.getClass().getField("names");
    //    } catch (NoSuchFieldException e) {
    //      System.out.println("no names");
    //    }
    SqlNode selectList = select.getSelectList();
    handlerSql(selectList, tableInfoMaps, flag);
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

  private void handlerAs(SqlNode sqlNode, OrderedMap<String, TableInfo> tableInfoMaps, Flag flag) {
    SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
    List<SqlNode> operandList = sqlBasicCall.getOperandList();

    SqlNode left = operandList.get(0);
    SqlNode right = operandList.get(1);

    if (Flag.COLUMN.equals(flag)) {
      // 获取最后一个表名
      ColumnInfo columnInfo =
          ColumnInfo.builder().name(left.toString()).alias(right.toString()).build();
      tableInfoMaps.get(tableInfoMaps.lastKey()).getColumns().add(columnInfo);
    } else {
      handlerSql(left, tableInfoMaps, Flag.REAL);
      // 左是名字，那么右就是别名
      if (SqlKind.IDENTIFIER.equals(left.getKind())) {
        handlerSql(right, tableInfoMaps, Flag.ALIAS);
      } else {
        handlerSql(right, tableInfoMaps, Flag.REAL);
      }
    }
  }

  /**
   * 列名，但是包含别名
   *
   * @param sqlNode
   * @param tableInfoMaps
   */
  private void handlerOther(
      SqlNode sqlNode, OrderedMap<String, TableInfo> tableInfoMaps, Flag flag) {
    List<@Nullable SqlNode> list = ((SqlNodeList) sqlNode).getList();
    TableInfo tableInfo = null;
    if (Flag.WITH_BODY.equals(flag)) {
      tableInfo = tableInfoMaps.get("res");
    } else {
      tableInfo = tableInfoMaps.get(lastTableName);
    }
    lastColumnInfos.clear();
    for (SqlNode node : list) {
      if (SqlKind.AS.equals(node.getKind())) { // 处理列别名
        handlerSql(node, tableInfoMaps, Flag.COLUMN);
      } else {
        ColumnInfo columnInfo = ColumnInfo.builder().name(node.toString()).build();
        lastColumnInfos.add(columnInfo);
        tableInfo.getColumns().add(columnInfo);
      }
    }
  }

  /**
   * 目前看起来是表名
   *
   * @param sqlNode
   * @param tableInfoMaps
   * @param flag 名字标识符
   */
  private void handlerIdentifier(
      SqlNode sqlNode, OrderedMap<String, TableInfo> tableInfoMaps, Flag flag) {
    SqlIdentifier sqlIdentifier = (SqlIdentifier) sqlNode;
    TableInfo tableInfo = null;
    int level =
        Optional.ofNullable(tableInfoMaps.get(lastTableName)).map(TableInfo::getLevel).orElse(0)
            + 1;
    if (Flag.REAL.equals(flag)) {
      if (tableInfoMaps.size() == 0 || !tableInfoMaps.containsKey(sqlIdentifier.getSimple())) {
        tableInfo =
            TableInfo.builder()
                .tableName(sqlIdentifier.getSimple())
                .alias(new String())
                .columns(new LinkedHashSet<>())
                .level(level)
                .build();
        tableInfoMaps.put(sqlIdentifier.getSimple(), tableInfo);
      }
    } else if (Flag.ALIAS.equals(flag)) {
      tableInfo = tableInfoMaps.get(tableInfoMaps.lastKey());
      tableInfo.setAlias(sqlIdentifier.getSimple());
    } else if (Flag.WITH_ITEM.equals(flag)) {
      tableInfo =
          TableInfo.builder()
              .tableName(sqlIdentifier.getSimple())
              .alias(new String())
              .columns(new LinkedHashSet<>(lastColumnInfos))
              .level(level)
              .build();
      tableInfoMaps.put(sqlIdentifier.getSimple(), tableInfo);
    } else if (Flag.WITH_BODY.equals(flag)) {
      tableInfo =
          TableInfo.builder()
              .tableName("res")
              .alias(new String())
              .columns(new LinkedHashSet<>())
              .level(level)
              .build();
      tableInfoMaps.put("res", tableInfo);
    }
    lastTableName = sqlIdentifier.getSimple();
  }

  public static void main(String[] args) {
    ParseSql parseSql = new ParseSql();
    parseSql.parseSelect("");
  }
}
