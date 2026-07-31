package dev.pgqueue;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class AntagonistTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgres:17.2-alpine");

    private static DataSource dataSource;

    @BeforeAll
    static void migrate() {
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(POSTGRES.getJdbcUrl());
        cfg.setUsername(POSTGRES.getUsername());
        cfg.setPassword(POSTGRES.getPassword());
        cfg.setMaximumPoolSize(4);
        dataSource = new HikariDataSource(cfg);
        Flyway.configure().dataSource(dataSource).load().migrate();
    }

    @BeforeEach
    void reset() throws Exception {
        try (Connection c = dataSource.getConnection();
             Statement st = c.createStatement()) {
            st.execute("TRUNCATE pgqueue.jobs RESTART IDENTITY");
        }
    }

    private Connection dedicatedConnection() throws Exception {
        return DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
    }

    @Test
    void antagonistPinsXminHorizonAndAgeGrowsWithWrites() throws Exception {
        try (Antagonist ant = new Antagonist(dedicatedConnection())) {
            ant.start();

            Antagonist.XminSnapshot first = ant.verify(dataSource);
            assertTrue(first.present(),
                    "antagonist backend_xmin should be visible in pg_stat_activity");

            // Bump the XID counter by producing dead tuples in another session.
            PgJobQueue queue = new PgJobQueue(dataSource);
            for (int i = 0; i < 50; i++) {
                long id = queue.enqueue(("j" + i).getBytes());
                queue.claim();
                queue.complete(id);
            }

            Antagonist.XminSnapshot second = ant.verify(dataSource);
            assertTrue(second.present(), "still holding after workload");
            assertTrue(second.xminAge() > first.xminAge(),
                    "xmin_age should grow after XID-advancing writes: was "
                            + first.xminAge() + ", now " + second.xminAge());
        }
    }

    @Test
    void closeReleasesTheSnapshotAndClosesTheBackend() throws Exception {
        Antagonist ant = new Antagonist(dedicatedConnection());
        ant.start();
        assertTrue(ant.verify(dataSource).present(), "should be holding after start");

        ant.close();
        assertFalse(ant.isHolding(), "holding flag should flip after close");
        assertFalse(ant.verify(dataSource).present(),
                "backend row should be gone from pg_stat_activity after close");
    }
}
