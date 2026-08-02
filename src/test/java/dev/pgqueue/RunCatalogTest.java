package dev.pgqueue;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RunCatalogTest {

    @Test
    void c1IsSaturatedWithoutAntagonist() {
        RunPlan p = RunCatalog.plan("C1", Duration.ofMinutes(45));
        assertEquals("C1", p.runId());
        assertEquals("saturated", p.configName());
        assertNull(p.antagonistStart(), "C1 is the control — no antagonist");
        assertNull(p.mitigation());
        Workload.Saturated s = assertInstanceOf(Workload.Saturated.class, p.workload());
        assertEquals(50_000, s.targetBacklog());
        assertEquals(20, p.workers());
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
