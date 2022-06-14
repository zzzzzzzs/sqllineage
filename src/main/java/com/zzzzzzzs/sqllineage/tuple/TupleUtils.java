package com.zzzzzzzs.sqllineage.tuple;

import java.util.Arrays;

public class TupleUtils {
  public static String arrayAwareToString(Object o) {
    final String arrayString = Arrays.deepToString(new Object[] {o});
    return arrayString.substring(1, arrayString.length() - 1);
  }
}
