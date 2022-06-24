package com.zzzzzzzs.sqllineage.core;

import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.LinkedHashMultiset;

public class Table {
  // key: 列名, value: 列中的数据
  LinkedHashMultimap<String, LinkedHashMultiset<Object>> table; // = LinkedHashMultimap.create();

}
