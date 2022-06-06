package com.zzzzzzzs.sqllineage.core;

import cn.hutool.core.io.file.FileReader;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.collect.LinkedHashMultimap;
import com.zzzzzzzs.sqllineage.bean.ColumnInfo;
import com.zzzzzzzs.sqllineage.bean.Flag;
import com.zzzzzzzs.sqllineage.bean.SqlJson;
import com.zzzzzzzs.sqllineage.bean.TableInfo;
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
  public String parseSelect(String sql) throws SqlParseException {
    sqlNodes.removeAll();
    if (sql == null || sql.isEmpty()) {
      FileReader fileReader = new FileReader("./sql/query002.sql");
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
    System.out.println("tableInfoMaps" + tableInfoMaps);
    return res;
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
        //        handlerOrderBy(sqlNode);
        break;
      case WITH:
        //        hanlderWith(sqlNode);
        break;
      case IDENTIFIER:
        // 表名
        handlerIdentifier(sqlNode, tableInfoMaps);
        break;
      case OTHER:
        // 列名
        handlerOther(sqlNode, tableInfoMaps);
        break;
      default:
        break;
    }
  }

  //  // handle with
  //  private void hanlderWith(SqlNode sqlNode) {
  //    SqlWith with = (SqlWith) sqlNode;
  //    List<SqlNode> withList = with.getWithList();
  //    for (SqlNode sqlNode1 : withList) {
  //      handlerSql(sqlNode1);
  //    }
  //  }
  //
  //  // handle order by
  //  private void handlerOrderBy(SqlNode sqlNode) {
  //    SqlOrderBy orderBy = (SqlOrderBy) sqlNode;
  //    List<SqlNode> orderByList = orderBy.getOrderList();
  //    for (SqlNode sqlNode1 : orderByList) {
  //      handlerSql(sqlNode1);
  //    }
  //  }

  // handle join
  private void handlerJoin(
      SqlNode sqlNode, OrderedMap<String, TableInfo> tableInfoMaps, AtomicInteger level) {
    SqlJoin join = (SqlJoin) sqlNode;
    SqlNode left = join.getLeft();
    SqlNode right = join.getRight();

    handlerSql(left, tableInfoMaps, null, level);
    handlerSql(right, tableInfoMaps, null, level);
  }

  // handle select
  private void handlerSelect(
      SqlNode sqlNode, OrderedMap<String, TableInfo> tableInfoMaps, AtomicInteger level) {
    SqlSelect select = (SqlSelect) sqlNode;
    SqlNode from = select.getFrom();
    handlerSql(from, tableInfoMaps, null, level);
    level.getAndIncrement();
    SqlNode where = select.getWhere();
    handlerSql(where, null, null, level);
    try {
      from.getClass().getField("names");
    } catch (NoSuchFieldException e) {
      System.out.println("no names");
    }
    SqlNode selectList = select.getSelectList();
    handlerSql(selectList, tableInfoMaps, null, level);
    SqlNode groupBy = select.getGroup();
    handlerSql(groupBy, null, null, level);
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
      handlerSql(left, tableInfoMaps, null, level);
      handlerSql(right, tableInfoMaps, null, level);
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
    for (SqlNode node : list) {
      if (SqlKind.AS.equals(node.getKind())) { // 列别名
        handlerSql(node, tableInfoMaps, Flag.COLUMN, new AtomicInteger(10000));
      } else {
        encapColumn(((SqlNodeList) sqlNode).getList(), tableInfoMaps);
      }
    }
    //    ((SqlNodeList) sqlNode)
    //        .getList()
    //        .forEach(column -> handlerSql(column, tableInfoMaps, new AtomicInteger(1000)));
    //    if (SqlKind.AS.equals(sqlNode.getKind())) {
    //      SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
    //      List<SqlNode> operandList = sqlBasicCall.getOperandList();
    //      SqlNode left = operandList.get(0);
    //      SqlNode right = operandList.get(1);
    //      handlerSql(left, tableInfoMaps, new AtomicInteger(1000));
    //      handlerSql(right, tableInfoMaps, new AtomicInteger(1000));
    //    }
    //    encapColumn(((SqlNodeList) sqlNode).getList(), tableInfoMaps);
  }

  // 封装
  private void encapColumn(List<SqlNode> columns, OrderedMap<String, TableInfo> tableInfoMaps) {
    TableInfo tableInfo = tableInfoMaps.get(tableInfoMaps.lastKey());
    for (SqlNode column : columns) {
      ColumnInfo columnInfo = ColumnInfo.builder().columnName(column.toString()).build();
      tableInfo.getColumns().add(columnInfo);
      //      tableInfoMaps.get(tableInfoMaps.size() - 1).getColumns().add(columnInfo);
    }
    //    ArrayNode sqlColumns = jsonSql.createArrayNode();
    //    columns.forEach(
    //        column -> {
    //          try {
    //            String columnRes = SqlJson.columnStr.replace("$name", column.toString());
    //            JsonNode columnNode = jsonSql.readTree(columnRes);
    //            sqlColumns.add(columnNode);
    //          } catch (JsonProcessingException e) {
    //            e.printStackTrace();
    //          }
    //        });
    //      String node =
    //          SqlJson.nodeStr
    //              .replace("$tableName", tableName)
    //              .replace("$columns", sqlColumns.toString());
    //      sqlNodes.add(jsonSql.readTree(node));
  }

  /**
   * 目前看起来是表名
   *
   * @param sqlNode
   * @param tableInfoMaps
   * @param flag 名字标识符
   */
  private void handlerIdentifier(SqlNode sqlNode, OrderedMap<String, TableInfo> tableInfoMaps) {
    SqlIdentifier sqlIdentifier = (SqlIdentifier) sqlNode;
    TableInfo tableInfo;
    if (!tableInfoMaps.containsKey(sqlIdentifier.getSimple())) {
      tableInfo =
          TableInfo.builder()
              .tableName(sqlIdentifier.getSimple())
              .alias(new String())
              .columns(new ArrayList<>())
              .level(1000)
              .build();
      tableInfoMaps.put(sqlIdentifier.getSimple(), tableInfo);
    }
  }

  public static void main(String[] args) throws SqlParseException {
    ParseSql parseSql = new ParseSql();
    parseSql.parseSelect("");
  }
}
