package com.zzzzzzzs.sqllineage.bean;

import com.zzzzzzzs.sqllineage.core.AutoInc;
import lombok.Builder;
import lombok.Data;
import lombok.ToString;
import lombok.experimental.Accessors;

import java.util.List;

/**
 * Sql Info
 *
 * @author zs
 * @date 2022/6/25
 */
@Data
@Builder
@Accessors(chain = true)
@ToString
public class SqlInfo {
  @AutoInc Integer id;
  String tableName;
  String tableAlias;
  String columnName;
  String columnAlias;
  Integer level;

  // 用来标记 with 语句中的列名
  Boolean withFlag;

  // 引用的表名
  List<String> quoTable;

  // 唯一标识符
  String uuid;
}
