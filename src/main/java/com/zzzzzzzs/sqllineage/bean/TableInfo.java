package com.zzzzzzzs.sqllineage.bean;

import lombok.Builder;
import lombok.Data;

import java.util.LinkedHashSet;

@Data
@Builder
public class TableInfo {
  String tableName; // 表名
  String alias; // 表别名
  LinkedHashSet<ColumnInfo> columns; // 列信息
  int level; // 层级
}
