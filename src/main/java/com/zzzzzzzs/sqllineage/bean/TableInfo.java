package com.zzzzzzzs.sqllineage.bean;

import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
public class TableInfo {
  String tableName; // 表名
  String alias; // 表别名
  List<ColumnInfo> columns; // 列信息
  int level; // 层级
}
