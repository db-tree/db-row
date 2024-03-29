package me.vzhilin.dbrow.util;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;

public final class BiMap<A, B> {
    private final Map<A, B> forwardMap = new LinkedHashMap<>();
    private final Map<B, A> reverseMap = new LinkedHashMap<>();

    public void put(A k, B v) {
        forwardMap.put(k, v);
        reverseMap.put(v, k);
    }

    public B get(A k) {
        return forwardMap.get(k);
    }

    public A getReverse(B k) {
        return reverseMap.get(k);
    }

    public void forEach(BiConsumer<? super A, ? super B> action) {
        forwardMap.forEach(action);
    }

    public int size() {
        return forwardMap.size();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BiMap<?, ?> biMap = (BiMap<?, ?>) o;
        return forwardMap.equals(biMap.forwardMap) &&
                reverseMap.equals(biMap.reverseMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(forwardMap, reverseMap);
    }
}
