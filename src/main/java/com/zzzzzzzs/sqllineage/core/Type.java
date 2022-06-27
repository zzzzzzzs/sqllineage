package com.zzzzzzzs.sqllineage.core;

public enum Type {
  // 与 java.lang.Integer 对应
  INT("java.lang.Integer", "int"),
  // 与 java.lang.String 对应
  STRING("java.lang.String", "string");
  public String cn; // className
  public String tn; // typeName

  Type(String s, String s1) {
    this.cn = s;
    this.tn = s1;
  }
}
