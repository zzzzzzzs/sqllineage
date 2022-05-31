package com.zzzzzzzs.sqllineage.bean;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ColumnInfo {
  String columnName; // 列名
  String alias; // 列别名
}
