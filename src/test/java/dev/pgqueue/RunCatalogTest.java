package dev.pgqueue;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RunCatalogTest {

    @Test
    void c1IsSaturatedWithoutAntagonist() {
        RunPlan p = RunCatalog.plan("C1", Duration.ofMinutes(45));
        assertEquals("C1", p.runId());
        assertEquals("saturated", p.configName());
        assertNull(p.antagonistStart(), "C1 is the control — no antagonist");
        assertNull(p.mitigationName());
        assertInstanceOf(Mitigation.None.class, p.mitigation());
        Workload.Saturated s = assertInstanceOf(Workload.Saturated.class, p.workload());
        assertEquals(50_000, s.targetBacklog());
        assertEquals(20, p.workers());
    }

    @Test
    void m1AppliesAggressiveAutovacuumDdlWithAntagonist() {
        RunPlan p = RunCatalog.plan("M1", Duration.ofMinutes(45));
        assertEquals("M1", p.mitigationName());
        assertEquals(Duration.ofMinutes(5), p.antagonistStart());
        Mitigation.TableAlter m = assertInstanceOf(Mitigation.TableAlter.class, p.mitigation());
        assertNotNull(m.ddl());
        String ddl = String.join("\n", m.ddl());
        assertTrue(ddl.contains("autovacuum_vacuum_scale_factor"),
                "M1 DDL should set scale_factor: " + ddl);
        assertTrue(ddl.contains("autovacuum_vacuum_cost_delay"));
        assertTrue(ddl.contains("autovacuum_vacuum_cost_limit"));
    }

    @Test
    void m2AppliesFillfactorDdlWithAntagonist() {
        RunPlan p = RunCatalog.plan("M2", Duration.ofMinutes(45));
        assertEquals("M2", p.mitigationName());
        assertEquals(Duration.ofMinutes(5), p.antagonistStart());
        Mitigation.TableAlter m = assertInstanceOf(Mitigation.TableAlter.class, p.mitigation());
        String ddl = String.join("\n", m.ddl());
        assertTrue(ddl.contains("fillfactor = 70"), "M2 DDL should set fillfactor=70: " + ddl);
    }

    @Test
    void m3AppliesPartitionDropWithSweeperCadence() {
        RunPlan p = RunCatalog.plan("M3", Duration.ofMinutes(45));
        assertEquals("M3", p.mitigationName());
        assertEquals(Duration.ofMinutes(5), p.antagonistStart());
        Mitigation.PartitionDrop m =
                assertInstanceOf(Mitigation.PartitionDrop.class, p.mitigation());
        assertEquals(Duration.ofSeconds(60), m.partitionWidth());
        assertEquals(Duration.ofSeconds(60), m.sweepInterval());
        assertTrue(m.futureCount() >= 5,
                "need at least 5 future partitions of runway: " + m.futureCount());
    }

    @Test
    void m3bAppliesPartitionAttachWithSweeperCadence() {
        RunPlan p = RunCatalog.plan("M3b", Duration.ofMinutes(45));
        assertEquals("M3b", p.mitigationName());
        assertEquals(Duration.ofMinutes(5), p.antagonistStart());
        Mitigation.PartitionAttach m =
                assertInstanceOf(Mitigation.PartitionAttach.class, p.mitigation());
        assertEquals(Duration.ofSeconds(60), m.partitionWidth());
        assertEquals(Duration.ofSeconds(60), m.sweepInterval());
        assertTrue(m.futureCount() >= 5,
                "need at least 5 future partitions of runway: " + m.futureCount());
    }

    @Test
    void m3bNoDropAppliesPartitionAttachNoDropWithSweeperCadence() {
        RunPlan p = RunCatalog.plan("M3b-nodrop", Duration.ofMinutes(45));
        assertEquals("M3b-nodrop", p.mitigationName());
        assertEquals(Duration.ofMinutes(5), p.antagonistStart());
        Mitigation.PartitionAttachNoDrop m =
                assertInstanceOf(Mitigation.PartitionAttachNoDrop.class, p.mitigation());
        assertEquals(Duration.ofSeconds(60), m.partitionWidth());
        assertEquals(Duration.ofSeconds(60), m.sweepInterval());
        assertTrue(m.futureCount() >= 5,
                "need at least 5 future partitions of runway: " + m.futureCount());
    }

    @Test
    void m4UsesAppendOnlyLogAndSwapsTheJobQueueImpl() {
        RunPlan p = RunCatalog.plan("M4", Duration.ofMinutes(45));
        assertEquals("M4", p.mitigationName());
        assertEquals(Duration.ofMinutes(5), p.antagonistStart());
        assertInstanceOf(Mitigation.AppendOnlyLog.class, p.mitigation());
    }

    @Test
    void r1r2r3ShareParametersAndStartAntagonistAtFiveMinutes() {
        for (String id : new String[] {"R1", "R2", "R3"}) {
            RunPlan p = RunCatalog.plan(id, Duration.ofMinutes(45));
            assertEquals(id, p.runId());
            assertEquals(Duration.ofMinutes(5), p.antagonistStart(),
                    id + " should start antagonist at t=300s per spec");
            assertEquals("saturated", p.configName());
        }
    }

    @Test
    void unknownRunIdIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> RunCatalog.plan("NOPE", Duration.ofMinutes(1)));
    }
}
