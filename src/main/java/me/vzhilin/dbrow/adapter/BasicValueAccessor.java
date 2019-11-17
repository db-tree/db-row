package me.vzhilin.dbrow.adapter;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

public class BasicValueAccessor implements ValueAccessor {
    public RowValue get(ResultSet rs, int index) throws SQLException {
        switch (rs.getMetaData().getColumnType(index)) {
            case Types.BIT: return getBit(rs, index);
            case Types.TINYINT: return getTinyint(rs, index);
            case Types.SMALLINT: return getSmallint(rs, index);
            case Types.INTEGER: return getInteger(rs, index);
            case Types.BIGINT: return getBigint(rs, index);
            case Types.FLOAT: return getFloat(rs, index);
            case Types.REAL: return getReal(rs, index);
            case Types.DOUBLE: return getDouble(rs, index);
            case Types.NUMERIC: return getNumeric(rs, index);
            case Types.DECIMAL: return getDecimal(rs, index);
            case Types.CHAR: return getChar(rs, index);
            case Types.VARCHAR: return getVarchar(rs, index);
            case Types.LONGVARCHAR: return getLongvarchar(rs, index);
            case Types.DATE: return getDate(rs, index);
            case Types.TIME: return getTime(rs, index);
            case Types.TIMESTAMP: return getTimestamp(rs, index);
            case Types.BINARY: return getBinary(rs, index);
            case Types.VARBINARY: return getVarbinary(rs, index);
            case Types.LONGVARBINARY: return getLongvarbinary(rs, index);
            case Types.NULL: return getNull(rs, index);
            case Types.OTHER: return getOther(rs, index);
            case Types.JAVA_OBJECT: return getJavaObject(rs, index);
            case Types.DISTINCT: return getDistinct(rs, index);
            case Types.STRUCT: return getStruct(rs, index);
            case Types.ARRAY: return getArray(rs, index);
            case Types.BLOB: return getBlob(rs, index);
            case Types.CLOB: return getClob(rs, index);
            case Types.REF: return getRef(rs, index);
            case Types.DATALINK: return getDatalink(rs, index);
            case Types.BOOLEAN: return getBoolean(rs, index);
            case Types.ROWID: return getRowid(rs, index);
            case Types.NCHAR: return getNchar(rs, index);
            case Types.NVARCHAR: return getNvarchar(rs, index);
            case Types.LONGNVARCHAR: return getLongnvarchar(rs, index);
            case Types.NCLOB: return getNclob(rs, index);
            case Types.SQLXML: return getSqlxml(rs, index);
            case Types.REF_CURSOR: return getRefCursor(rs, index);
            case Types.TIME_WITH_TIMEZONE: return getTimeWithTimezone(rs, index);
            case Types.TIMESTAMP_WITH_TIMEZONE: return getTimestampWithTimezone(rs, index);
            default: return new NotSupported(rs.getMetaData().getColumnTypeName(index));
        }
    }

    @Override
    public RowValue getBit(ResultSet rs, int index) throws SQLException {
        return new CommonValue(rs.getObject(index));
    }

    @Override
    public RowValue getTinyint(ResultSet rs, int index) throws SQLException {
        return new CommonValue(rs.getInt(index));
    }

    @Override
    public RowValue getSmallint(ResultSet rs, int index) throws SQLException {
        return new CommonValue(rs.getInt(index));
    }

    @Override
    public RowValue getInteger(ResultSet rs, int index) throws SQLException {
        return new CommonValue(rs.getInt(index));
    }

    @Override
    public RowValue getBigint(ResultSet rs, int index) throws SQLException {
        return new CommonValue(rs.getLong(index));
    }

    @Override
    public RowValue getFloat(ResultSet rs, int index) throws SQLException {
        return new CommonValue(rs.getFloat(index));
    }

    @Override
    public RowValue getReal(ResultSet rs, int index) throws SQLException {
        return new CommonValue(rs.getDouble(index));
    }

    @Override
    public RowValue getDouble(ResultSet rs, int index) throws SQLException {
        return new CommonValue(rs.getDouble(index));
    }

    @Override
    public RowValue getNumeric(ResultSet rs, int index) throws SQLException {
        return new CommonValue(rs.getBigDecimal(index));
    }

    @Override
    public RowValue getDecimal(ResultSet rs, int index) throws SQLException {
        return new CommonValue(rs.getBigDecimal(index));
    }

    @Override
    public RowValue getChar(ResultSet rs, int index) throws SQLException {
        return new CommonValue(rs.getString(index)); // TODO: character stream
    }

    @Override
    public RowValue getVarchar(ResultSet rs, int index) throws SQLException {
        return new CommonValue(rs.getString(index));
    }

    @Override
    public RowValue getLongvarchar(ResultSet rs, int index) throws SQLException {
        return new CommonValue(rs.getString(index)); // TODO: character stream
    }

    @Override
    public RowValue getDate(ResultSet rs, int index) throws SQLException {
        return new CommonValue(rs.getDate(index)); // TODO java.sql.date
    }

    @Override
    public RowValue getTime(ResultSet rs, int index) throws SQLException {
        return new CommonValue(rs.getTime(index)); // TODO java.sql.time
    }

    @Override
    public RowValue getTimestamp(ResultSet rs, int index) throws SQLException {
        return new CommonValue(rs.getTimestamp(index)); // TODO java.sql.timestamp
    }

    @Override
    public RowValue getBinary(ResultSet rs, int index) {
        return new NotSupported("[BINARY]");
    }

    @Override
    public RowValue getVarbinary(ResultSet rs, int index) {
        return new NotSupported("[VARBINARY]");
    }

    @Override
    public RowValue getLongvarbinary(ResultSet rs, int index) {
        return new NotSupported("[LONGVARBINARY]");
    }

    @Override
    public RowValue getNull(ResultSet rs, int index) {
        return null;
    }

    @Override
    public RowValue getOther(ResultSet rs, int index) {
        return new NotSupported("[OTHER]");
    }

    @Override
    public RowValue getJavaObject(ResultSet rs, int index) {
        return new NotSupported("[JAVA_OBJECT]");
    }

    @Override
    public RowValue getDistinct(ResultSet rs, int index) {
        return new NotSupported("[DISTINCT]");
    }

    @Override
    public RowValue getStruct(ResultSet rs, int index) {
        return new NotSupported("[STRUCT]");
    }

    @Override
    public RowValue getArray(ResultSet rs, int index) {
        return new NotSupported("[ARRAY]");
    }

    @Override
    public RowValue getBlob(ResultSet rs, int index) {
        return new NotSupported("[BLOB]");
    }

    @Override
    public RowValue getClob(ResultSet rs, int index) {
        return new NotSupported("[CLOB]");
    }

    @Override
    public RowValue getRef(ResultSet rs, int index) {
        return new NotSupported("[REF]");
    }

    @Override
    public RowValue getDatalink(ResultSet rs, int index) {
        return new NotSupported("[DATALINK]");
    }

    @Override
    public RowValue getBoolean(ResultSet rs, int index) throws SQLException {
        return new CommonValue(rs.getBoolean(index));
    }

    @Override
    public RowValue getRowid(ResultSet rs, int index) {
        return new NotSupported("[ROWID]");
    }

    @Override
    public RowValue getNchar(ResultSet rs, int index) throws SQLException {
        return new CommonValue(rs.getString(index));
    }

    @Override
    public RowValue getNvarchar(ResultSet rs, int index) throws SQLException {
        return new CommonValue(rs.getString(index));
    }

    @Override
    public RowValue getLongnvarchar(ResultSet rs, int index) throws SQLException {
        return new CommonValue(rs.getString(index)); // FIXME length
    }

    @Override
    public RowValue getNclob(ResultSet rs, int index) {
        return new NotSupported("[NCLOB]");
    }

    @Override
    public RowValue getSqlxml(ResultSet rs, int index) {
        return new NotSupported("[SQLXML]");
    }

    @Override
    public RowValue getRefCursor(ResultSet rs, int index) {
        return new NotSupported("[REFCURSOR]");
    }

    @Override
    public RowValue getTimeWithTimezone(ResultSet rs, int index) throws SQLException {
        return new CommonValue(rs.getObject(index));
    }

    @Override
    public RowValue getTimestampWithTimezone(ResultSet rs, int index) throws SQLException {
        return new CommonValue(rs.getTimestamp(index));
    }

    protected RowValue getCommonValue(ResultSet rs, int index) throws SQLException {
        return new CommonValue(rs.getObject(index));
    }

    private static class NotSupported implements RowValue {
        private final String mesg;

        private NotSupported(String mesg) {
            this.mesg = mesg;
        }

        @Override
        public Object get() {
            return null;
        }

        @Override
        public String toString() {
            return mesg;
        }
    }

}
