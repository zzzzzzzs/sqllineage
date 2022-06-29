package com.zzzzzzzs.sqllineage.core;

import com.zzzzzzzs.sqllineage.tuple.Tuple2;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

public class Table<V> {
  // colName,<index, Type>
  private LinkedHashMap<String, Tuple2<Integer, String>> colNames = new LinkedHashMap<>(); // 存储列名
  // colName, num
  private Tuple2<String, Integer> autoInc = null;
  private List<V> colVs = new ArrayList<>(); // 存储列值
  private boolean sorted = false; // 是否已排序

  public Table() {}

  private String getType(Field type) {
    return type.getType().getSimpleName();
  }

  // 初始化表结构，colname以半角的逗号(,)分隔
  void createTable(Class clazz) {
    Field[] fields = clazz.getDeclaredFields();
    for (int i = 0; i < fields.length; i++) {
      // 获取 ele 中带有 AutoInc 注解的将被自动增长的列名
      if (fields[i].isAnnotationPresent(AutoInc.class)) {
        autoInc = new Tuple2<>(fields[i].getName(), 0);
      }
      if (this.colNames.containsKey(fields[i].getName())) {
        throw new IllegalArgumentException("列名重复 : " + fields[i].getName());
      }
      this.colNames.put(fields[i].getName(), Tuple2.of(i, getType(fields[i])));
    }
  }

  // 指定类名与值得合法性校验
  private void checkCol(String colName, Object... colVal) {
    // colName 判断不为空且有值
    if (colName == null || colName.trim().length() == 0) {
      throw new IllegalArgumentException("列名不能为空");
    }
    // colName 按照逗号切割 且个数与值的个数一致
    String[] colNames = colName.split(",");
    if (colNames.length != colVal.length) {
      throw new IllegalArgumentException("列名与值不对应");
    }
    // 列名必须在表中存在
    for (String col : colNames) {
      if (!this.colNames.containsKey(col.trim())) {
        throw new IllegalArgumentException("列名不存在 : " + col);
      }
    }
    // 校验类型
    for (int i = 0; i < colNames.length; i++) {
      if (colVal[i] == null) break;
      if (!this.colNames.get(colNames[i].trim()).f1.equals(colVal[i].getClass().getSimpleName())) {
        throw new IllegalArgumentException(
                "类型不对应 : " + colNames[i] + " | " + colVal[i].getClass().getSimpleName());
      }
    }
  }

  // 清除表结构以及数据
  public void dropTable() {
    this.colNames.clear();
    this.colVs.clear();
  }

  // 插入一条记录，可以缺省一些列名，同时列名不一定按建表时的顺序
  public void insert(V ele) {
    try {
      if (autoInc != null) {
        Field incFiled = ele.getClass().getDeclaredField(autoInc.f0);
        incFiled.setAccessible(true);
        incFiled.set(ele, ++autoInc.f1);
      }
      this.colVs.add(ele);
      this.sorted = false;
    } catch (NoSuchFieldException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }
  }

  // 取表行数
  public int getRowCount() {
    return this.colVs.size();
  }
  // 取表列数
  public int getColCount() {
    return this.colNames.size();
  }

  // 查找记录
  public List<V> selectWhere(String colName, Object... colVal) {
    if (sorted) {
      return selectWhere2();
    } else {
      return selectWhere1(colName, colVal);
    }
  }
  // 遍历查找 V 中的记录
  private List<V> selectWhere1(String colName, Object... colVal) {
    checkCol(colName, colVal);
    String[] colN = colName.split(",");
    ArrayList<V> res = new ArrayList<>();
    try {
      for (V ele : this.colVs) {
        boolean isMatch = true;
        for (int i = 0; i < colN.length; i++) {
          Field field = ele.getClass().getDeclaredField(colN[i]);
          field.setAccessible(true);
          Object oo = field.get(ele);
          // oo == null 时，不能用 equals 比较，否则会报错
          if (oo == null) {
            if (colVal[i] != null) {
              isMatch = false;
              break;
            }
          } else {
            if (!oo.equals(colVal[i])) {
              isMatch = false;
              break;
            }
          }
        }
        if (isMatch) {
          res.add(ele);
        }
      }
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (NoSuchFieldException e) {
      e.printStackTrace();
    }
    return res;
  }

  // 二分查找记录，可返回多行
  private List<V> selectWhere2() {
    return null;
  }

  // 查询所有数据
  public List<V> selectAll() {
    return this.colVs;
  }

  // 删除记录
  public void deleteWhere(String colName, Object... colVal) {
    if (sorted) {
      deletewhere2();
    } else {
      deletewhere1(colName, colVal);
    }
  }

  // 遍历删除 colVs 记录，可删除多行
  private void deletewhere1(String colName, Object... colVal) {
    checkCol(colName, colVal);
    String[] colN = colName.split(",");
    ArrayList<V> res = new ArrayList<>();
    try {
      for (V ele : this.colVs) {
        boolean isMatch = true;
        for (int i = 0; i < colN.length; i++) {
          Object oo = ele.getClass().getDeclaredField(colN[i]).get(ele);
          // oo == null 时，不能用 equals 比较，否则会报错
          if (oo == null) {
            if (colVal[i] != null) {
              isMatch = false;
              break;
            }
          } else {
            if (!oo.equals(colVal[i])) {
              isMatch = false;
              break;
            }
          }
        }
        if (isMatch) {
          res.add(ele);
        }
      }
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (NoSuchFieldException e) {
      e.printStackTrace();
    }
    this.colVs.removeAll(res);
  }

  private void deletewhere2() {}

  // 更新记录
  public void updateWhere(String oColName, Object[] oColVs, String colName, Object[] colVs) {
    if (sorted) {
      updateWhere2();
    } else {
      updateWhere1(oColName, oColVs, colName, colVs);
    }
  }

  // 遍历更新记录，可更新多行
  // 找到 oColName 对应的 colVal，然后更新 colName 对应的 colVal
  private void updateWhere1(String oColName, Object[] oColVs, String colName, Object[] colVs) {
    checkCol(oColName, oColVs);
    checkCol(colName, colVs);
    String[] oColN = oColName.split(",");
    String[] colN = colName.split(",");
    try {
      for (V ele : this.colVs) {
        boolean isMatch = true;
        for (int i = 0; i < oColN.length; i++) {
          Field field = ele.getClass().getDeclaredField(colN[i]);
          field.setAccessible(true);
          Object oo = field.get(ele);
          // oo == null 时，不能用 equals 比较，否则会报错
          if (oo == null) {
            if (oColVs[i] != null) {
              isMatch = false;
              break;
            }
          } else {
            if (!oo.equals(oColVs[i])) {
              isMatch = false;
              break;
            }
          }
        }
        if (isMatch) {
          for (int i = 0; i < colN.length; i++) {
            ele.getClass().getDeclaredField(colN[i]).set(ele, colVs[i]);
          }
        }
      }
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (NoSuchFieldException e) {
      e.printStackTrace();
    }
  }

  private void updateWhere2() {}

  // 将colVs中的元素排序（不排自增主键）
  public void sort() {}

  /**
   * Vs : 列值
   *
   * @param
   */
  public void print(ArrayList<V> Vs) {
    System.out.println(colNames);
    for (V ele : Vs) {
      System.out.println(ele);
    }
  }

  void print() {
    System.out.println(
            "==================================== table data ==========================================");
    System.out.println(this.colNames);
    for (V ele : this.colVs) {
      System.out.println(ele);
    }
    System.out.println(
            "==================================== table EOF ==========================================");
  }

  // EOF
}
