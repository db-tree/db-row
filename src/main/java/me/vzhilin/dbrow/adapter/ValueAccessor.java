package me.vzhilin.dbrow.adapter;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface ValueAccessor {
    RowValue get(ResultSet rs, int index) throws SQLException;

    RowValue getBit(ResultSet rs, int index) throws SQLException;
    RowValue getTinyint(ResultSet rs, int index) throws SQLException;
    RowValue getSmallint(ResultSet rs, int index) throws SQLException;
    RowValue getInteger(ResultSet rs, int index) throws SQLException;
    RowValue getBigint(ResultSet rs, int index) throws SQLException;
    RowValue getFloat(ResultSet rs, int index) throws SQLException;
    RowValue getReal(ResultSet rs, int index) throws SQLException;
    RowValue getDouble(ResultSet rs, int index) throws SQLException;
    RowValue getNumeric(ResultSet rs, int index) throws SQLException;
    RowValue getDecimal(ResultSet rs, int index) throws SQLException;
    RowValue getChar(ResultSet rs, int index) throws SQLException;
    RowValue getVarchar(ResultSet rs, int index) throws SQLException;
    RowValue getLongvarchar(ResultSet rs, int index) throws SQLException;
    RowValue getDate(ResultSet rs, int index) throws SQLException;
    RowValue getTime(ResultSet rs, int index) throws SQLException;
    RowValue getTimestamp(ResultSet rs, int index) throws SQLException;
    RowValue getBinary(ResultSet rs, int index);
    RowValue getVarbinary(ResultSet rs, int index);
    RowValue getLongvarbinary(ResultSet rs, int index);
    RowValue getNull(ResultSet rs, int index);
    RowValue getOther(ResultSet rs, int index);
    RowValue getJavaObject(ResultSet rs, int index);
    RowValue getDistinct(ResultSet rs, int index);
    RowValue getStruct(ResultSet rs, int index);
    RowValue getArray(ResultSet rs, int index);
    RowValue getBlob(ResultSet rs, int index);
    RowValue getClob(ResultSet rs, int index);
    RowValue getRef(ResultSet rs, int index);
    RowValue getDatalink(ResultSet rs, int index);
    RowValue getBoolean(ResultSet rs, int index) throws SQLException;
    RowValue getRowid(ResultSet rs, int index);
    RowValue getNchar(ResultSet rs, int index) throws SQLException;
    RowValue getNvarchar(ResultSet rs, int index) throws SQLException;
    RowValue getLongnvarchar(ResultSet rs, int index) throws SQLException;
    RowValue getNclob(ResultSet rs, int index);
    RowValue getSqlxml(ResultSet rs, int index);
    RowValue getRefCursor(ResultSet rs, int index);
    RowValue getTimeWithTimezone(ResultSet rs, int index) throws SQLException;
    RowValue getTimestampWithTimezone(ResultSet rs, int index) throws SQLException;
}
