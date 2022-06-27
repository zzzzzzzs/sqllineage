package com.zzzzzzzs.sqllineage.bean;

import com.zzzzzzzs.sqllineage.core.AutoInc;
import lombok.Builder;
import lombok.Data;
import lombok.ToString;

/**
 * Sql Info
 *
 * @author zs
 * @date 2022/6/25
 */
@Data
@Builder
@ToString
public class SqlInfo {
  @AutoInc Integer id;
  String tableName;
  String tableAlias;
  String columnName;
  String columnAlias;
  Integer level;
  // 唯一标识符
  String uuid;
}
