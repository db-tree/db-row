package me.vzhilin.dbrow.db;

import java.sql.SQLException;

public final class QueryException extends RuntimeException {
    private final String query;

    public QueryException(String message, String query, SQLException ex) {
        super(message, ex);
        this.query = query;
    }

    public QueryException(String message, SQLException e) {
        super(message, e);
        this.query = "";
    }
}
