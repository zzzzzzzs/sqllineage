package com.zzzzzzzs.sqllineage.core;

import cn.hutool.core.io.file.FileReader;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.zzzzzzzs.sqllineage.bean.ColumnInfo;
import com.zzzzzzzs.sqllineage.bean.Flag;
import com.zzzzzzzs.sqllineage.bean.SqlJson;
import com.zzzzzzzs.sqllineage.bean.TableInfo;
import lombok.SneakyThrows;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.commons.collections4.OrderedMap;
import org.apache.commons.collections4.map.ListOrderedMap;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ParseSql {
  FrameworkConfig config;
  ObjectMapper jsonSql = new ObjectMapper();
  ArrayNode sqlNodes = jsonSql.createArrayNode();

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

  // parse select
  @SneakyThrows
  public String parseSelect(String sql) throws SqlParseException {
    sqlNodes.removeAll();
    if (sql == null || sql.isEmpty()) {
      FileReader fileReader = new FileReader("sql/aquery008.sql");
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
    handlerSql(sqlNode, tableInfoMaps, null, new AtomicInteger(1));
    String res = SqlJson.res.replace("$nodes", sqlNodes.toString());

    System.out.println(res);
    //    System.out.println("tableInfoMaps" + jsonSql.writeValueAsString(tableInfoMaps));
    System.out.println(jsonSql.writerWithDefaultPrettyPrinter().writeValueAsString(tableInfoMaps));
    return jsonSql.writerWithDefaultPrettyPrinter().writeValueAsString(tableInfoMaps);
  }

  // handle sqlnode
  private void handlerSql(
      SqlNode sqlNode,
      OrderedMap<String, TableInfo> tableInfoMaps,
      Flag flag,
      AtomicInteger level) {
    if (sqlNode == null) return;
    SqlKind kind = sqlNode.getKind();
    switch (kind) {
      case INSERT:
        handlerInsert(sqlNode, level);
        break;
      case SELECT:
        handlerSelect(sqlNode, tableInfoMaps, level);
        break;
      case JOIN:
        handlerJoin(sqlNode, tableInfoMaps, level);
        break;
      case AS:
        handlerAs(sqlNode, tableInfoMaps, flag, level);
        break;
      case UNION:
        break;
      case ORDER_BY:
        handlerOrderBy(sqlNode, tableInfoMaps, flag, level);
        break;
      case WITH:
        handleWith(sqlNode, tableInfoMaps, flag, level);
        break;
      case WITH_ITEM:
        handleWithItem(sqlNode, tableInfoMaps, flag, level);
        break;
      case IDENTIFIER:
        // 表名
        handlerIdentifier(sqlNode, tableInfoMaps, flag, level);
        break;
      case OTHER:
        // 列名
        handlerOther(sqlNode, tableInfoMaps);
        break;
      default:
        break;
    }
  }

  // handle with
  private void handleWith(
      SqlNode sqlNode,
      OrderedMap<String, TableInfo> tableInfoMaps,
      Flag flag,
      AtomicInteger level) {
    SqlWith with = (SqlWith) sqlNode;
    List<@Nullable SqlNode> withList = with.withList.getList();
    for (SqlNode node : withList) {
      handlerSql(node, tableInfoMaps, flag, level);
    }
    handlerSql(with.body, tableInfoMaps, flag, level);
  }

  // handler with item
  private void handleWithItem(
      SqlNode sqlNode,
      OrderedMap<String, TableInfo> tableInfoMaps,
      Flag flag,
      AtomicInteger level) {
    SqlWithItem withItem = (SqlWithItem) sqlNode;
    handlerSql(withItem.query, tableInfoMaps, flag, level);
    handlerSql(withItem.name, tableInfoMaps, Flag.ALIAS, level);
  }

  // handle order by
  // TODO 后期可以从 orderBy 中获取到列的名称补全列名
  private void handlerOrderBy(
      SqlNode sqlNode,
      OrderedMap<String, TableInfo> tableInfoMaps,
      Flag flag,
      AtomicInteger level) {
    SqlOrderBy orderBy = (SqlOrderBy) sqlNode;
    SqlNode query = orderBy.query;
    handlerSql(query, tableInfoMaps, flag, level);
    //    List<SqlNode> orderByList = orderBy.orderList;
    //    System.out.println("orderByList" + orderByList);
    //    for (SqlNode sqlNode : orderByList) {
    //      handlerSql(sqlNode);
    //    }
  }

  // handle join
  private void handlerJoin(
      SqlNode sqlNode, OrderedMap<String, TableInfo> tableInfoMaps, AtomicInteger level) {
    SqlJoin join = (SqlJoin) sqlNode;
    SqlNode left = join.getLeft();
    SqlNode right = join.getRight();

    handlerSql(left, tableInfoMaps, Flag.REAL, level);
    handlerSql(right, tableInfoMaps, Flag.REAL, level);
  }

  // handle select
  private void handlerSelect(
      SqlNode sqlNode, OrderedMap<String, TableInfo> tableInfoMaps, AtomicInteger level) {
    SqlSelect select = (SqlSelect) sqlNode;
    SqlNode from = select.getFrom();
    handlerSql(from, tableInfoMaps, Flag.REAL, level);
    level.getAndIncrement();
    SqlNode where = select.getWhere();
    handlerSql(where, null, null, level);
    //    try {
    //      from.getClass().getField("names");
    //    } catch (NoSuchFieldException e) {
    //      System.out.println("no names");
    //    }
    SqlNode selectList = select.getSelectList();
    handlerSql(selectList, tableInfoMaps, null, level);
    // TODO 后期处理
    //    SqlNode groupBy = select.getGroup();
    //    handlerSql(groupBy, null, null, level);
    SqlNode having = select.getHaving();
    handlerSql(having, null, null, level);
    SqlNodeList orderList = select.getOrderList();
    handlerSql(orderList, null, null, level);
  }

  // handle insert
  private void handlerInsert(SqlNode sqlNode, AtomicInteger level) {
    SqlInsert sqlInsert = (SqlInsert) sqlNode;
    SqlNode insertList = sqlInsert.getTargetTable();
    handlerSql(insertList, null, null, level);
    SqlNode source = sqlInsert.getSource();
    handlerSql(source, null, null, level);
  }

  private void handlerAs(
      SqlNode sqlNode,
      OrderedMap<String, TableInfo> tableInfoMaps,
      Flag flag,
      AtomicInteger level) {
    SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
    List<SqlNode> operandList = sqlBasicCall.getOperandList();

    SqlNode left = operandList.get(0);
    SqlNode right = operandList.get(1);

    if (Flag.COLUMN.equals(flag)) {
      // 获取最后一个表名
      ColumnInfo columnInfo =
          ColumnInfo.builder().columnName(left.toString()).alias(right.toString()).build();
      tableInfoMaps.get(tableInfoMaps.lastKey()).getColumns().add(columnInfo);
    } else {
      handlerSql(left, tableInfoMaps, Flag.REAL, level);
      // 左是名字，那么右就是别名
      if (SqlKind.IDENTIFIER.equals(left.getKind())) {
        handlerSql(right, tableInfoMaps, Flag.ALIAS, level);
      } else {
        handlerSql(right, tableInfoMaps, Flag.REAL, level);
      }
    }
  }

  /**
   * 列名，但是包含别名
   *
   * @param sqlNode
   * @param tableInfoMaps
   */
  private void handlerOther(SqlNode sqlNode, OrderedMap<String, TableInfo> tableInfoMaps) {
    List<@Nullable SqlNode> list = ((SqlNodeList) sqlNode).getList();
    TableInfo tableInfo = tableInfoMaps.get(tableInfoMaps.lastKey());
    for (SqlNode node : list) {
      if (SqlKind.AS.equals(node.getKind())) { // 处理列别名
        handlerSql(node, tableInfoMaps, Flag.COLUMN, new AtomicInteger(10000));
      } else {
        ColumnInfo columnInfo = ColumnInfo.builder().columnName(node.toString()).build();
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
      SqlNode sqlNode,
      OrderedMap<String, TableInfo> tableInfoMaps,
      Flag flag,
      AtomicInteger level) {
    SqlIdentifier sqlIdentifier = (SqlIdentifier) sqlNode;
    TableInfo tableInfo;
    if (Flag.REAL.equals(flag)) {
      tableInfo =
          TableInfo.builder()
              .tableName(sqlIdentifier.getSimple())
              .alias(new String())
              .columns(new ArrayList<>())
              .level(level.get())
              .build();
      if (tableInfoMaps.size() == 0 || !tableInfoMaps.containsKey(sqlIdentifier.getSimple())) {
        tableInfoMaps.put(sqlIdentifier.getSimple(), tableInfo);
      }
    } else if (Flag.ALIAS.equals(flag)) {
      tableInfo = tableInfoMaps.get(tableInfoMaps.lastKey());
      tableInfo.setAlias(sqlIdentifier.getSimple());
    }
  }

  public static void main(String[] args) throws SqlParseException {
    ParseSql parseSql = new ParseSql();
    parseSql.parseSelect("");
  }
}
