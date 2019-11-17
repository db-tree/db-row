package me.vzhilin.dbrow.adapter;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

public final class ValueAccessorAdapter {
    private ValueAccessor accessor;

    public ValueAccessorAdapter(ValueAccessor accessor) {
        this.accessor = accessor;
    }

    public RowValue get(ResultSet rs, int index) throws SQLException {
        switch (rs.getMetaData().getColumnType(index)) {
            case Types.BIT: return accessor.getBit(rs, index);
            case Types.TINYINT: return accessor.getTinyint(rs, index);
            case Types.SMALLINT: return accessor.getSmallint(rs, index);
            case Types.INTEGER: return accessor.getInteger(rs, index);
            case Types.BIGINT: return accessor.getBigint(rs, index);
            case Types.FLOAT: return accessor.getFloat(rs, index);
            case Types.REAL: return accessor.getReal(rs, index);
            case Types.DOUBLE: return accessor.getDouble(rs, index);
            case Types.NUMERIC: return accessor.getNumeric(rs, index);
            case Types.DECIMAL: return accessor.getDecimal(rs, index);
            case Types.CHAR: return accessor.getChar(rs, index);
            case Types.VARCHAR: return accessor.getVarchar(rs, index);
            case Types.LONGVARCHAR: return accessor.getLongvarchar(rs, index);
            case Types.DATE: return accessor.getDate(rs, index);
            case Types.TIME: return accessor.getTime(rs, index);
            case Types.TIMESTAMP: return accessor.getTimestamp(rs, index);
            case Types.BINARY: return accessor.getBinary(rs, index);
            case Types.VARBINARY: return accessor.getVarbinary(rs, index);
            case Types.LONGVARBINARY: return accessor.getLongvarbinary(rs, index);
            case Types.NULL: return accessor.getNull(rs, index);
            case Types.OTHER: return accessor.getOther(rs, index);
            case Types.JAVA_OBJECT: return accessor.getJavaObject(rs, index);
            case Types.DISTINCT: return accessor.getDistinct(rs, index);
            case Types.STRUCT: return accessor.getStruct(rs, index);
            case Types.ARRAY: return accessor.getArray(rs, index);
            case Types.BLOB: return accessor.getBlob(rs, index);
            case Types.CLOB: return accessor.getClob(rs, index);
            case Types.REF: return accessor.getRef(rs, index);
            case Types.DATALINK: return accessor.getDatalink(rs, index);
            case Types.BOOLEAN: return accessor.getBoolean(rs, index);
            case Types.ROWID: return accessor.getRowid(rs, index);
            case Types.NCHAR: return accessor.getNchar(rs, index);
            case Types.NVARCHAR: return accessor.getNvarchar(rs, index);
            case Types.LONGNVARCHAR: return accessor.getLongnvarchar(rs, index);
            case Types.NCLOB: return accessor.getNclob(rs, index);
            case Types.SQLXML: return accessor.getSqlxml(rs, index);
            case Types.REF_CURSOR: return accessor.getRefCursor(rs, index);
            case Types.TIME_WITH_TIMEZONE: return accessor.getTimeWithTimezone(rs, index);
            case Types.TIMESTAMP_WITH_TIMEZONE: return accessor.getTimestampWithTimezone(rs, index);
            default: throw new RuntimeException("unknown type: " + rs.getType());
        }
    }
}
