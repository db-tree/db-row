package me.vzhilin.dbrow.adapter.oracle;

import me.vzhilin.dbrow.adapter.BasicColumnTypeInfo;

public class OracleColumnTypeInfo extends BasicColumnTypeInfo {
    public OracleColumnTypeInfo() {
        addDecimal("NUMBER").setHasLength().setHasPrecision();

        addInteger("INTEGER").setAlias("NUMBER");
        addInteger("INT").setAlias("NUMBER");
        addInteger("SMALLINT").setAlias("NUMBER");

        addFloat("FLOAT").setHasLength();
        addFloat("REAL").setAlias("FLOAT");

        addFloat("DOUBLE PRECISION").setAlias("FLOAT");
        addFloat("BINARY_FLOAT");
        addFloat("BINARY_DOUBLE");

        addDate("DATE");
        addDate("TIMESTAMP").setHasLength();

        addString("VARCHAR2").setMandatoryLength();
        addString("NVARCHAR2").setMandatoryLength();
        addString("CHARACTER VARYING").setAlias("VARCHAR2").setMandatoryLength();
        addString("CHAR VARYING").setAlias("VARCHAR2").setMandatoryLength();
        addString("CHAR").setHasLength();
        addString("NCHAR").setHasLength();
        addString("CLOB");
        addString("NCLOB");

        addByteArray("BLOB");
        addByteArray("BFILE");
        addByteArray("RAW").setMandatoryLength();
        addByteArray("LONG RAW");
    }
}
