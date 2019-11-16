package me.vzhilin.dbrow.adapter.mariadb;

import me.vzhilin.dbrow.adapter.BasicColumnTypeInfo;
import me.vzhilin.dbrow.adapter.ColumnType;
import me.vzhilin.dbrow.adapter.ColumnTypeDescription;

import java.util.HashSet;
import java.util.Set;

public class MariadbColumnTypeInfo extends BasicColumnTypeInfo {
    public MariadbColumnTypeInfo() {
        Set<String> attr = new HashSet<>();
        attr.add("SIGNED");
        attr.add("UNSIGNED");
        attr.add("ZEROFILL");

        addColumnType(new ColumnTypeDescription("enum", "enum", ColumnType.ENUM));
        addInteger("boolean").setAlias("tinyint");
        addInteger("bit").setHasLength();
        addByteArray("binary").setHasLength();
        addByteArray("varbinary").setMandatoryLength();

        for (String t: new String[]{"smallint", "tinyint", "mediumint", "int", "bigint"}) {
            addInteger(t).setHasLength();
        }

        addInteger("integer").setAlias("int").setHasLength();

        for (String t: new String[]{"decimal", "dec", "numeric", "fixed"}) {
            addDecimal(t).setAlias("decimal").setHasLength().setHasPrecision();
        }

        for (String t: new String[]{"float", "double"}) {
            addFloat(t);
        }

        addFloat("double precision").setAlias("double");
        addString("char").setHasLength();
        addString("varchar").setMandatoryLength();
        addString("tinytext");
        addString("longtext");
        addString("json").setAlias("longtext");

        for (String t: new String[]{"date", "time", "datetime", "timestamp", "year"}) {
            addDate(t);
        }
    }

}
