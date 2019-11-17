package me.vzhilin.dbrow.adapter;

public enum SqlTypes {

    /**
     * <P>The constant in the Java programming language, sometimes referred
     * to as a type code, that identifies the generic SQL type
     * <code>BIT</code>.
     */
    BIT(java.sql.Types.BIT),

    /**
     * <P>The constant in the Java programming language, sometimes referred
     * to as a type code, that identifies the generic SQL type
     * <code>TINYINT</code>.
     */
    TINYINT(java.sql.Types.TINYINT),

    /**
     * <P>The constant in the Java programming language, sometimes referred
     * to as a type code, that identifies the generic SQL type
     * <code>SMALLINT</code>.
     */
    SMALLINT(java.sql.Types.SMALLINT),

    /**
     * <P>The constant in the Java programming language, sometimes referred
     * to as a type code, that identifies the generic SQL type
     * <code>INTEGER</code>.
     */
    INTEGER(java.sql.Types.INTEGER),

    /**
     * <P>The constant in the Java programming language, sometimes referred
     * to as a type code, that identifies the generic SQL type
     * <code>BIGINT</code>.
     */
    BIGINT(java.sql.Types.BIGINT),

    /**
     * <P>The constant in the Java programming language, sometimes referred
     * to as a type code, that identifies the generic SQL type
     * <code>FLOAT</code>.
     */
    FLOAT(java.sql.Types.FLOAT),

    /**
     * <P>The constant in the Java programming language, sometimes referred
     * to as a type code, that identifies the generic SQL type
     * <code>REAL</code>.
     */
    REAL(java.sql.Types.REAL),


    /**
     * <P>The constant in the Java programming language, sometimes referred
     * to as a type code, that identifies the generic SQL type
     * <code>DOUBLE</code>.
     */
    DOUBLE(java.sql.Types.DOUBLE),

    /**
     * <P>The constant in the Java programming language, sometimes referred
     * to as a type code, that identifies the generic SQL type
     * <code>NUMERIC</code>.
     */
    NUMERIC(java.sql.Types.NUMERIC),

    /**
     * <P>The constant in the Java programming language, sometimes referred
     * to as a type code, that identifies the generic SQL type
     * <code>DECIMAL</code>.
     */
    DECIMAL(java.sql.Types.DECIMAL),

    /**
     * <P>The constant in the Java programming language, sometimes referred
     * to as a type code, that identifies the generic SQL type
     * <code>CHAR</code>.
     */
    CHAR(java.sql.Types.CHAR),

    /**
     * <P>The constant in the Java programming language, sometimes referred
     * to as a type code, that identifies the generic SQL type
     * <code>VARCHAR</code>.
     */
    VARCHAR(java.sql.Types.VARCHAR),

    /**
     * <P>The constant in the Java programming language, sometimes referred
     * to as a type code, that identifies the generic SQL type
     * <code>LONGVARCHAR</code>.
     */
    LONGVARCHAR(java.sql.Types.LONGVARCHAR),


    /**
     * <P>The constant in the Java programming language, sometimes referred
     * to as a type code, that identifies the generic SQL type
     * <code>DATE</code>.
     */
    DATE(java.sql.Types.DATE),

    /**
     * <P>The constant in the Java programming language, sometimes referred
     * to as a type code, that identifies the generic SQL type
     * <code>TIME</code>.
     */
    TIME(java.sql.Types.TIME),

    /**
     * <P>The constant in the Java programming language, sometimes referred
     * to as a type code, that identifies the generic SQL type
     * <code>TIMESTAMP</code>.
     */
    TIMESTAMP(java.sql.Types.TIMESTAMP),


    /**
     * <P>The constant in the Java programming language, sometimes referred
     * to as a type code, that identifies the generic SQL type
     * <code>BINARY</code>.
     */
    BINARY(java.sql.Types.BINARY),

    /**
     * <P>The constant in the Java programming language, sometimes referred
     * to as a type code, that identifies the generic SQL type
     * <code>VARBINARY</code>.
     */
    VARBINARY(java.sql.Types.VARBINARY),

    /**
     * <P>The constant in the Java programming language, sometimes referred
     * to as a type code, that identifies the generic SQL type
     * <code>LONGVARBINARY</code>.
     */
    LONGVARBINARY(java.sql.Types.LONGVARBINARY),

    /**
     * <P>The constant in the Java programming language
     * that identifies the generic SQL value
     * <code>NULL</code>.
     */
    NULL(java.sql.Types.NULL),

    /**
     * The constant in the Java programming language that indicates
     * that the SQL type is database-specific and
     * gets mapped to a Java object that can be accessed via
     * the methods <code>getObject</code> and <code>setObject</code>.
     */
    OTHER(java.sql.Types.OTHER),



    /**
     * The constant in the Java programming language, sometimes referred to
     * as a type code, that identifies the generic SQL type
     * <code>JAVA_OBJECT</code>.
     * @since 1.2
     */
    JAVA_OBJECT(java.sql.Types.JAVA_OBJECT),

    /**
     * The constant in the Java programming language, sometimes referred to
     * as a type code, that identifies the generic SQL type
     * <code>DISTINCT</code>.
     * @since 1.2
     */
    DISTINCT(java.sql.Types.DISTINCT),

    /**
     * The constant in the Java programming language, sometimes referred to
     * as a type code, that identifies the generic SQL type
     * <code>STRUCT</code>.
     * @since 1.2
     */
    STRUCT(java.sql.Types.STRUCT),

    /**
     * The constant in the Java programming language, sometimes referred to
     * as a type code, that identifies the generic SQL type
     * <code>ARRAY</code>.
     * @since 1.2
     */
    ARRAY(java.sql.Types.ARRAY),

    /**
     * The constant in the Java programming language, sometimes referred to
     * as a type code, that identifies the generic SQL type
     * <code>BLOB</code>.
     * @since 1.2
     */
    BLOB(java.sql.Types.BLOB),

    /**
     * The constant in the Java programming language, sometimes referred to
     * as a type code, that identifies the generic SQL type
     * <code>CLOB</code>.
     * @since 1.2
     */
    CLOB(java.sql.Types.CLOB),

    /**
     * The constant in the Java programming language, sometimes referred to
     * as a type code, that identifies the generic SQL type
     * <code>REF</code>.
     * @since 1.2
     */
    REF(java.sql.Types.REF),

    /**
     * The constant in the Java programming language, sometimes referred to
     * as a type code, that identifies the generic SQL type <code>DATALINK</code>.
     *
     * @since 1.4
     */
    DATALINK(java.sql.Types.DATALINK),

    /**
     * The constant in the Java programming language, sometimes referred to
     * as a type code, that identifies the generic SQL type <code>BOOLEAN</code>.
     *
     * @since 1.4
     */
    BOOLEAN(java.sql.Types.BOOLEAN),

    //------------------------- JDBC 4.0 -----------------------------------

    /**
     * The constant in the Java programming language, sometimes referred to
     * as a type code, that identifies the generic SQL type <code>ROWID</code>
     *
     * @since 1.6
     *
     */
    ROWID(java.sql.Types.ROWID),

    /**
     * The constant in the Java programming language, sometimes referred to
     * as a type code, that identifies the generic SQL type <code>NCHAR</code>
     *
     * @since 1.6
     */
    NCHAR(java.sql.Types.NCHAR),

    /**
     * The constant in the Java programming language, sometimes referred to
     * as a type code, that identifies the generic SQL type <code>NVARCHAR</code>.
     *
     * @since 1.6
     */
    NVARCHAR(java.sql.Types.NVARCHAR),

    /**
     * The constant in the Java programming language, sometimes referred to
     * as a type code, that identifies the generic SQL type <code>LONGNVARCHAR</code>.
     *
     * @since 1.6
     */
    LONGNVARCHAR(java.sql.Types.LONGVARCHAR),

    /**
     * The constant in the Java programming language, sometimes referred to
     * as a type code, that identifies the generic SQL type <code>NCLOB</code>.
     *
     * @since 1.6
     */
    NCLOB(java.sql.Types.NCLOB),

    /**
     * The constant in the Java programming language, sometimes referred to
     * as a type code, that identifies the generic SQL type <code>XML</code>.
     *
     * @since 1.6
     */
    SQLXML(java.sql.Types.SQLXML),

    //--------------------------JDBC 4.2 -----------------------------

    /**
     * The constant in the Java programming language, sometimes referred to
     * as a type code, that identifies the generic SQL type {@code REF CURSOR}.
     *
     * @since 1.8
     */
    REF_CURSOR(java.sql.Types.REF_CURSOR),

    /**
     * The constant in the Java programming language, sometimes referred to
     * as a type code, that identifies the generic SQL type
     * {@code TIME WITH TIMEZONE}.
     *
     * @since 1.8
     */
    TIME_WITH_TIMEZONE(java.sql.Types.TIME_WITH_TIMEZONE),

    /**
     * The constant in the Java programming language, sometimes referred to
     * as a type code, that identifies the generic SQL type
     * {@code TIMESTAMP WITH TIMEZONE}.
     *
     * @since 1.8
     */
    TIMESTAMP_WITH_TIMEZONE(java.sql.Types.TIMESTAMP_WITH_TIMEZONE);

    public final int id;

    SqlTypes(int id) {
        this.id = id;
    }
}
