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
        return Sinks.<Connection, StockPriceUpdate>builder((unused) -> JDBCSink.openConnection(connectionUrl))
                .onReceiveFn(JDBCSink::insertUpdate)
                .destroyFn(JDBCSink::closeConnection)
                .build();
    }

    private static Connection openConnection(String connectionUrl) {
        try {
            return DriverManager.getConnection(connectionUrl);
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot acquire a connection with URL '" + connectionUrl + "'", e);
        }
    }

    private static void closeConnection(Connection c) {
        try {
            c.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void insertUpdate(Connection c, StockPriceUpdate i) {
        try (PreparedStatement ps = c.prepareStatement(INSERT_QUERY)) {
            ps.setLong(1, i.getTimestamp());
            ps.setLong(2, i.getPrice());
            ps.setString(3, i.getSymbol());

            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("Error while inserting " + i + " into database");
        }
    }
}
