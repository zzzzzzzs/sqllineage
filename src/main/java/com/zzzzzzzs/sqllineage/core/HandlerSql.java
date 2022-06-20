package com.zzzzzzzs.sqllineage.core;

@FunctionalInterface
public interface HandlerSql<R, T1, T2> {
    R handler(T1 t1, T2 t2);
}
