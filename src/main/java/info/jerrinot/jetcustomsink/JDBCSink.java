package info.jerrinot.jetcustomsink;

import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sinks;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class JDBCSink {
    private static final String INSERT_QUERY = "insert into stock_updates (ts, price, symbol) values (?, ?, ?)";

    public static Sink<StockPriceUpdate> newSink(String connectionUrl) {
        return Sinks.<PreparedStatement, StockPriceUpdate>builder((unused) -> JDBCSink.createStatement(connectionUrl))
                .onReceiveFn(JDBCSink::insertUpdate)
                .destroyFn(JDBCSink::cleanup)
                .build();
    }

    private static PreparedStatement createStatement(String connectionUrl) {
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(connectionUrl);
            return connection.prepareStatement(INSERT_QUERY);
        } catch (SQLException e) {
            closeSilently(connection);
            throw new IllegalStateException("Cannot acquire a connection with URL '" + connectionUrl + "'", e);
        }
    }

    private static void closeSilently(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                //ignored
            }
        }
    }

    private static void cleanup(PreparedStatement c) {
        try {
            c.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void insertUpdate(PreparedStatement ps, StockPriceUpdate i) {
        try {
            ps.setLong(1, i.getTimestamp());
            ps.setLong(2, i.getPrice());
            ps.setString(3, i.getSymbol());

            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("Error while inserting " + i + " into database");
        }
    }
}
