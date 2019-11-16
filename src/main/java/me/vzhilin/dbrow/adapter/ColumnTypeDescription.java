package me.vzhilin.dbrow.adapter;

import me.vzhilin.dbrow.adapter.conv.NeverConverter;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

public final class ColumnTypeDescription {
    private final String name;
    private final ColumnType type;
    private String alias;

    private boolean hasLength;
    private boolean hasPrecision;
    private Set<String> attributes;
    private boolean mandatoryLength;
    private Converter conv = NeverConverter.INSTANCE;

    public ColumnTypeDescription(String name, ColumnType type) {
        this.name = name;
        this.type = type;
        this.alias = name;
    }

    public ColumnTypeDescription(String name, String alias, ColumnType type) {
        this(name, alias, type, false);
    }

    public ColumnTypeDescription(String name, String alias, ColumnType type, boolean hasLength) {
        this(name, alias, type, hasLength,  false);
    }

    public ColumnTypeDescription(String name, String alias, ColumnType type, boolean hasLength, boolean hasPrecision) {
        this(name, alias, type, hasLength, hasPrecision, Collections.emptySet());
    }

    public ColumnTypeDescription(String name, String alias, ColumnType type, boolean hasLength, boolean hasPrecision, Set<String> attributes) {
        this.name = name;
        this.alias = alias;
        this.type = type;
        this.hasLength = hasLength;
        this.hasPrecision = hasPrecision;
        this.attributes = attributes;
    }

    public String getName() {
        return name;
    }

    public String getAlias() {
        return alias;
    }

    public ColumnType getType() {
        return type;
    }

    public boolean hasLength() {
        return hasLength;
    }

    public boolean hasPrecision() {
        return hasPrecision;
    }

    public Set<String> getAttributes() {
        return attributes;
    }

    public ColumnTypeDescription setMandatoryLength() {
        this.hasLength = true;
        this.mandatoryLength = true;
        return this;
    }

    public boolean hasMandatoryLength() {
        return mandatoryLength;
    }

    public ColumnTypeDescription setAlias(String alias) {
        this.alias = alias;
        return this;
    }

    public boolean isHasLength() {
        return hasLength;
    }

    public ColumnTypeDescription setHasLength() {
        this.hasLength = true;
        return this;
    }

    public boolean isHasPrecision() {
        return hasPrecision;
    }

    public ColumnTypeDescription setHasPrecision() {
        this.hasPrecision = true;
        return this;
    }

    public void setAttributes(Set<String> attributes) {
        this.attributes = attributes;
    }

    public Converter getConv() {
        return conv;
    }

    public void setConv(Converter conv) {
        this.conv = conv;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ColumnTypeDescription that = (ColumnTypeDescription) o;
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
