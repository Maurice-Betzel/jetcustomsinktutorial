package info.jerrinot.jetcustomsink;

import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.pipeline.JournalInitialPosition;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sources;
import org.junit.Rule;
import org.junit.Test;
import org.zapodot.junit.db.EmbeddedDatabaseRule;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.*;

public class JDBCSinkTest {
    private static String MAP_NAME = "MY_MAP";

    @Rule
    public final EmbeddedDatabaseRule dbRule = EmbeddedDatabaseRule
            .builder()
            .withInitialSql("CREATE TABLE stock_updates(ts long, price long, symbol VARCHAR(8)); ")
            .build();


    @Test
    public void testJDBCSink() throws SQLException, InterruptedException {
        // start new Jet Instance
        JetConfig jetConfig = new JetConfig();
        jetConfig.getHazelcastConfig().getMapEventJournalConfig(MAP_NAME).setEnabled(true);
        JetInstance jetInstance = Jet.newJetInstance(jetConfig);


        // create new pipeline
        String connectionJdbcUrl = dbRule.getConnectionJdbcUrl();
        Pipeline pipeline = Pipeline.create().drawFrom(Sources.<Integer, StockPriceUpdate>mapJournal(MAP_NAME, JournalInitialPosition.START_FROM_OLDEST))
                .map(Map.Entry::getValue)
                .drainTo(JDBCSink.newSink(connectionJdbcUrl))
                .getPipeline();

        // submit the pipeline as a job
        jetInstance.newJob(pipeline);

        // populate the source map
        int entryCount = 10_000;
        populateMap(jetInstance, entryCount);


        // all entries should eventually be visible the database
        assertTableSizeEqualsEventually(entryCount, dbRule.getConnection());
    }

    private int populateMap(JetInstance jetInstance, int entryCount) {
        IMapJet<Integer, StockPriceUpdate> map = jetInstance.getMap(MAP_NAME);
        for (int i = 0; i < entryCount; i++) {
            map.put(i, randomStockUpdate(i));
        }
        return entryCount;
    }

    private static StockPriceUpdate randomStockUpdate(int seq) {
        String symbol = "STCK" + (seq % 100);
        long price = ThreadLocalRandom.current().nextLong(1000);
        long timestamp = System.currentTimeMillis();
        return new StockPriceUpdate(symbol, price, timestamp);
    }

    private static void assertTableSizeEqualsEventually(int expectedSize, Connection connection) throws SQLException, InterruptedException {
        int maxAttemptCount = 100;
        int sleepingTimeMs = 100;

        for (int i = 0;;i++) {
            try {
                assertTableSizeEquals(expectedSize, connection);
                return;
            } catch (AssertionError ae) {
                //we reached maximum number of attempts. something is wrong, let's propagate the exception
                if (i == maxAttemptCount) {
                    throw ae;
                }
                // table does not have the expected size
                // let's try wait and try it again
                Thread.sleep(sleepingTimeMs);
            }
        }
    }

    private static void assertTableSizeEquals(int expectedSize, Connection connection) throws SQLException {
        PreparedStatement statement = connection.prepareStatement("select count(*) as size from stock_updates");

        ResultSet resultSet = statement.executeQuery();
        if (resultSet.next()) {
            int tableSize = resultSet.getInt("size");
            assertEquals(expectedSize, tableSize);
        } else {
            throw new AssertionError("Table is empty");
        }

    }


}