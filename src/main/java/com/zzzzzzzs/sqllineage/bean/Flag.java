package com.zzzzzzzs.sqllineage.bean;

// 标识符
public enum Flag {
  TABLE_REAL, // 表&真实名字
  COLUMN_REAL, // 列&真实名字
  TABLE_ALIAS, // 表&别名
  COLUMN_ALIAS, // 列&别名
  COLUMN, // 列
  REAL, // 真实名字
  ALIAS, // 别名
  WITH_ITEM, // WITH_ITEM
  WITH_BODY, // WITH BODY
}
