package me.vzhilin.dbrow.adapter;

class CommonValue implements RowValue {
    private final Object o;

    CommonValue(Object o) {
        this.o = o;
    }

    @Override
    public Object get() {
        return o;
    }

    @Override
    public String toString() {
        return String.valueOf(o);
    }
}
