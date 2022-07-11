package com.daasyyds.flink.sql.analyzer.common;

import java.util.Objects;

public class Tuple2<T, K> {
    public T f0;
    public K f1;

    public Tuple2() {
    }

    public Tuple2(T f0, K f1) {
        this.f0 = f0;
        this.f1 = f1;
    }

    public static <T, K> Tuple2<T, K> of(T f0, K f1) {
        return new Tuple2<>(f0, f1);
    }

    public T getF0() {
        return f0;
    }

    public void setF0(T f0) {
        this.f0 = f0;
    }

    public K getF1() {
        return f1;
    }

    public void setF1(K f1) {
        this.f1 = f1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Tuple2<?, ?> tuple2 = (Tuple2<?, ?>) o;
        return f0.equals(tuple2.f0) &&
                f1.equals(tuple2.f1);
    }

    @Override
    public int hashCode() {
        return Objects.hash(f0, f1);
    }
}
