package com.zzzzzzzs.sqllineage.bean;

// 用来表示返回前端的结果
public class SqlJson {
    public final static String res = """
            {
              "edges": $edges,
              "nodes": $nodes
            }
            """;

    // 边
    public final static String edgeStr = """
                {
                  "from": {
                    "column": "$1",
                    "tbName": "$2"
                  },
                  "to": {
                    "column": "$3",
                    "tbName": "$4"
                  }
                }
                """;

    // 列
    public final static String columnStr = """
                {
                  "name": "$name"
                }
                """;
    // 节点
    public final static  String nodeStr = """
                {
                  "id": "$tableName",
                  "name": "$tableName",
                  "type": "$type",
                  "columns": $columns,
                  "top": $top,
                  "left": $left
                }
                """;
}
