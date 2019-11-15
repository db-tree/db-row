package me.vzhilin.dbrow.adapter;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

public final class ColumnTypeDescription {
    private final String name;
    private final String alias;

    private final ColumnType type;
    private final boolean hasLength;
    private final boolean hasPrecision;
    private final Set<String> attributes;
    private boolean mandatoryLength;

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

    public void setMandatoryLength(boolean mandatoryLength) {
        this.mandatoryLength = mandatoryLength;
    }

    public boolean isMandatoryLength() {
        return mandatoryLength;
    }
}
