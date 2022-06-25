package com.zzzzzzzs.sqllineage.bean;

import lombok.Data;

/**
 * Sql Info
 *
 * @author zs
 * @date 2022/6/25
 */
@Data
public class SqlInfo {
  String tableName;
  String tableAlias;
  String columnName;
  String columnAlias;
  Integer level;
}
