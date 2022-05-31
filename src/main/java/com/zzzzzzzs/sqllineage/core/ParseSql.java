package com.zzzzzzzs.sqllineage.core;

import cn.hutool.core.io.file.FileReader;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.zzzzzzzs.sqllineage.bean.ColumnInfo;
import com.zzzzzzzs.sqllineage.bean.SqlJson;
import com.zzzzzzzs.sqllineage.bean.TableInfo;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

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
                    //                    .setConformance(SqlConformanceEnum.DEFAULT)
                    .build())
            .build();
  }

  // parse select
  public String parseSelect(String sql) throws SqlParseException {
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
    List<TableInfo> tableInfos = new ArrayList<>();
    // 默认真实名字
    handlerSql(sqlNode, tableInfos, 1);
    String res = SqlJson.res.replace("$nodes", sqlNodes.toString());
    sqlNodes.removeAll();
    System.out.println(res);
    System.out.println("tableInfos" + tableInfos);
    return res;
  }

  // handle sqlnode
  private void handlerSql(SqlNode sqlNode, List<TableInfo> tableInfos, int flag) {
    if (sqlNode == null) return;
    SqlKind kind = sqlNode.getKind();
    switch (kind) {
      case INSERT:
        handlerInsert(sqlNode);
        break;
      case SELECT:
        handlerSelect(sqlNode, tableInfos);
        break;
      case JOIN:
        handlerJoin(sqlNode, tableInfos);
        break;
      case AS:
        handlerAs(sqlNode, tableInfos);
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
        handlerIdentifier(sqlNode, tableInfos, flag);
        break;
      case OTHER:
        // 列名
        handlerOther(sqlNode, tableInfos);
        break;
      default:
        break;
    }
  }

  //  private void handlerIdentifier(SqlNode sqlNode) {
  //    SqlIdentifier identifier = (SqlIdentifier) sqlNode;
  //    System.out.println("处理的名字：" + identifier.names);
  //  }
  //
  //  private void handlerOther(SqlNode sqlNode) {
  //    // TODO AS
  //    System.out.println("处理的名字：" + sqlNode.toString());
  //  }

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
  private void handlerJoin(SqlNode sqlNode, List<TableInfo> tableInfos) {
    SqlJoin join = (SqlJoin) sqlNode;
    SqlNode left = join.getLeft();
    SqlNode right = join.getRight();
    handlerSql(left, tableInfos, 1);
    handlerSql(right, tableInfos, 1);
  }

  // handle select
  private void handlerSelect(SqlNode sqlNode, List<TableInfo> tableInfos) {
    SqlSelect select = (SqlSelect) sqlNode;
    SqlNode from = select.getFrom();
    handlerSql(from, tableInfos, 1);
    SqlNode selectList = select.getSelectList();
    handlerSql(selectList, tableInfos, 1);
    SqlNode where = select.getWhere();
    handlerSql(where, null, 1);
    SqlNode groupBy = select.getGroup();
    handlerSql(groupBy, null, 1);
    SqlNode having = select.getHaving();
    handlerSql(having, null, 1);
    SqlNodeList orderList = select.getOrderList();
    handlerSql(orderList, null, 1);
  }

  // handle insert
  private void handlerInsert(SqlNode sqlNode) {
    SqlInsert sqlInsert = (SqlInsert) sqlNode;
    SqlNode insertList = sqlInsert.getTargetTable();
    handlerSql(insertList, null, 1);
    SqlNode source = sqlInsert.getSource();
    handlerSql(source, null, 1);
  }

  //  private void hanlerTable(SqlNode sqlNode) {
  //    SqlNodeList<SqlNode> nodeList = sqlNode.getNodeList();
  //    handlerField(nodeList);
  //  }

  //  private void hanlerSelect(SqlNode sqlNode) {
  //    SqlSelect sqlSelect = (SqlSelect) sqlNode;
  //    SqlNode query = sqlSelect.query;
  //    hanlerSQL(query);
  //    SqlNodeList<SqlNode> selectList = sqlSelect.getSelectList();
  //    handlerField(selectList);
  //    SqlNodeList<SqlNode> fromList = sqlSelect.getFrom();
  //    handlerField(fromList);
  //    SqlNodeList<SqlNode> whereList = sqlSelect.getWhere();
  //    handlerField(whereList);
  //    SqlNodeList<SqlNode> groupByList = sqlSelect.getGroupBy();
  //    handlerField(groupByList);
  //    SqlNodeList<SqlNode> havingList = sqlSelect.getHaving();
  //    handlerField(havingList);
  //    SqlNodeList<SqlNode> orderByList = sqlSelect.getOrderBy();
  //    handlerField(orderByList);
  //    SqlNode limit = sqlSelect.getLimit();
  //    hanlerSQL(limit);
  //  }

  //  private void handlerField(SqlNodeList nodeList) {
  //    for (SqlNode node : nodeList) {
  //      handlerSql(node);
  //    }
  //  }
  //
  //  private void handlerJoin(SqlNode sqlNode) {
  //    SqlJoin sqlJoin = (SqlJoin) sqlNode;
  //    SqlNode left = sqlJoin.getLeft();
  //    handlerSql(left);
  //    SqlNode right = sqlJoin.getRight();
  //    handlerSql(right);
  //    SqlNode condition = sqlJoin.getCondition();
  //    handlerSql(condition);
  //  }
  //
  //  private void handlerInsert(SqlNode sqlNode) {
  //    SqlInsert sqlInsert = (SqlInsert) sqlNode;
  //    SqlNode query = sqlInsert.getQuery();
  //    handlerSql(query);
  //    SqlNodeList<SqlNode> columnList = sqlInsert.getColumnList();
  //    handlerField(columnList);
  //    SqlNodeList<SqlNode> valueList = sqlInsert.getValueList();
  //    handlerField(valueList);
  //  }
  //
  //  private void handlerSelect(SqlNode sqlNode) {
  //    SqlSelect sqlSelect = (SqlSelect) sqlNode;
  //    SqlNode query = sqlSelect.getQuery();
  //    handlerSql(query);
  //    SqlNodeList<SqlNode> selectList = sqlSelect.getSelectList();
  //    handlerField(selectList);
  //    SqlNodeList<SqlNode> fromList = sqlSelect.getFrom();
  //    handlerField(fromList);
  //    SqlNodeList<SqlNode> whereList = sqlSelect.getWhere();
  //    handlerField(whereList);
  //    SqlNodeList<SqlNode> groupByList = sqlSelect.getGroupBy();
  //    handlerField(groupByList);
  //    SqlNodeList<SqlNode> havingList = sqlSelect.getHaving();
  //    handlerField(havingList);
  //    SqlNodeList<SqlNode> orderByList = sqlSelect.getOrderBy();
  //    handlerField(orderByList);
  //    SqlNode limit = sqlSelect.getLimit();
  //    handlerSql(limit);
  //  }
  //
  //  private void handlerUpdate(SqlNode sqlNode) {
  //    SqlUpdate sqlUpdate = (SqlUpdate) sqlNode;
  //    SqlNode query = sqlUpdate.getQuery();
  //    handlerSql(query);
  //    SqlNodeList<SqlNode> setList = sqlUpdate.getSetList();
  //    handlerField(setList);
  //    SqlNodeList<SqlNode> whereList = sqlUpdate.getWhere();
  //    handlerField(whereList);
  //  }
  //
  //  private void handlerDelete(SqlNode sqlNode) {
  //    SqlDelete sqlDelete = (SqlDelete) sqlNode;
  //    SqlNode query = sqlDelete.getQuery();
  //    handlerSql(query);
  //    SqlNodeList<SqlNode> whereList = sqlDelete.getWhere();
  //    handlerField(whereList);
  //  }
  //
  //  private void handlerCall(SqlNode sqlNode) {
  //    SqlCall sqlCall = (SqlCall) sqlNode;
  //    SqlNodeList<SqlNode> operandList = sqlCall.getOperandList();
  //    handlerField(operandList);
  //  }
  //
  //  private void handlerSql(SqlNode sqlNode) {
  //    if (sqlNode instanceof SqlSelect) {
  //      handlerSelect(sqlNode);
  //    } else if (sqlNode instanceof SqlUpdate) {
  //      handlerUpdate(sqlNode);
  //    } else if (sqlNode instanceof SqlDelete) {
  //      handlerDelete(sqlNode);
  //    } else if (sqlNode instanceof SqlInsert) {
  //      handlerInsert(sqlNode);
  //    } else if (sqlNode instanceof SqlCall) {
  //      handlerCall(sqlNode);
  //    } else if (sqlNode instanceof SqlJoin) {
  //      handlerJoin(sqlNode);
  //    } else if (sqlNode instanceof SqlOrderBy) {
  //      handlerOrderBy(sqlNode);
  //    } else if (sqlNode instanceof SqlWith) {
  //      hanlderWith(sqlNode);
  //    } else if (sqlNode instanceof SqlNodeList) {
  //      handlerField(sqlNode);
  //    } else if (sqlNode instanceof SqlIdentifier) {
  //      handlerIdentifier(sqlNode);
  //    }
  //  }
  //
  //
  //  private void handlerInsert(SqlNode sqlNode) {
  //    SqlInsert sqlInsert = (SqlInsert) sqlNode;
  ////    SqlNode target = sqlInsert.getTarget();
  //    SqlNode targetTable = sqlInsert.getTargetTable();
  ////    handlerSql(target);
  //    SqlNode source = sqlInsert.getSource();
  //    handlerSql(source);
  //  }
  //
  //  private void handlerSelect(SqlNode sqlNode) {
  //    SqlSelect sqlSelect = (SqlSelect) sqlNode;
  //    SqlNode query = sqlSelect.query;
  //
  //    handlerSql(query);
  //    SqlNodeList selectList = sqlSelect.getSelectList();
  //    handlerField(selectList);
  //    SqlNodeList<SqlNode> fromList = sqlSelect.getFrom();
  //    handlerField(fromList);
  //    SqlNodeList<SqlNode> whereList = sqlSelect.getWhere();
  //    handlerField(whereList);
  //    SqlNodeList<SqlNode> groupByList = sqlSelect.getGroupBy();
  //    handlerField(groupByList);
  //    SqlNodeList<SqlNode> havingList = sqlSelect.getHaving();
  //    handlerField(havingList);
  //    SqlNodeList<SqlNode> orderByList = sqlSelect.getOrderBy();
  //    handlerField(orderByList);
  //    SqlNode limit = sqlSelect.getLimit();
  //    handlerSql(limit);
  //  }
  //
  //  private void handlerJoin(SqlNode sqlNode) {
  //    SqlJoin sqlJoin = (SqlJoin) sqlNode;
  //    SqlNode left = sqlJoin.getLeft();
  //    handlerSql(left);
  //    SqlNode right = sqlJoin.getRight();
  //    handlerSql(right);
  //  }
  //
  private void handlerAs(SqlNode sqlNode, List<TableInfo> tableInfos) {
    SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
    List<SqlNode> operandList = sqlBasicCall.getOperandList();
    TableInfo tableInfo =
        TableInfo.builder()
            .tableName(new String())
            .alias(new String())
            .columns(new ArrayList<>())
            .build();
    tableInfos.add(tableInfo);
    SqlNode left = operandList.get(0);
    handlerSql(left, tableInfos, 1);
    SqlNode right = operandList.get(1);
    handlerSql(right, tableInfos, 2);
  }

  // 目前看起来是列名
  private void handlerOther(SqlNode sqlNode, List<TableInfo> tableInfos) {
    encapColumn(((SqlNodeList) sqlNode).getList(), tableInfos);
  }

  private void encapColumn(List<SqlNode> columns, List<TableInfo> tableInfos) {
    for (SqlNode column : columns) {
      ColumnInfo columnInfo = ColumnInfo.builder().columnName(column.toString()).build();
      tableInfos.get(tableInfos.size() - 1).getColumns().add(columnInfo);
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
   * @param tableInfos
   * @param flag 名字表示符
   */
  private void handlerIdentifier(SqlNode sqlNode, List<TableInfo> tableInfos, int flag) {
    SqlIdentifier sqlIdentifier = (SqlIdentifier) sqlNode;
    if (1 == flag) { // 真实名字
      tableInfos.get(tableInfos.size() - 1).setTableName(sqlIdentifier.getSimple());
    } else if (2 == flag) { // 别名
      tableInfos.get(tableInfos.size() - 1).setAlias(sqlIdentifier.getSimple());
    }
  }

  public static void main(String[] args) throws SqlParseException {
    ParseSql parseSql = new ParseSql();
    parseSql.parseSelect("");
  }
}
