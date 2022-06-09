package com.zzzzzzzs.sqllineage.bean;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@Builder
@EqualsAndHashCode
public class ColumnInfo {
  String name; // 列名
  @EqualsAndHashCode.Exclude String alias; // 列别名
}
