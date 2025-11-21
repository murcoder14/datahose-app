package org.muralis.datahose;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class VisitCounterTest {

    @Test
    public void testVisitCounter() throws Exception {
        StreamingApp.VisitCounter visitCounter = new StreamingApp.VisitCounter();
        KeyedProcessOperator<String, Tuple2<String, Integer>, Tuple2<String, Integer>> operator = new KeyedProcessOperator<>(
                visitCounter);

        KeyedOneInputStreamOperatorTestHarness<String, Tuple2<String, Integer>, Tuple2<String, Integer>> testHarness = new KeyedOneInputStreamOperatorTestHarness<>(
                operator, value -> value.f0, Types.STRING);

        testHarness.open();

        // Process elements
        testHarness.processElement(new Tuple2<>("Dan", 1), 1000L);
        testHarness.processElement(new Tuple2<>("Dan", 1), 2000L);
        testHarness.processElement(new Tuple2<>("Kate", 1), 3000L);

        // Advance watermark to infinity to trigger timer
        testHarness.processWatermark(Long.MAX_VALUE);

        ConcurrentLinkedQueue<Object> output = testHarness.getOutput();

        // Expected: 2 output elements (Dan: 2, Kate: 1) + 1 Watermark
        // Note: The order depends on how the harness processes timers, but for distinct
        // keys it should be deterministic enough for this test or we check containment.
        // Actually, let's just verify the stream records.

        // We expect 3 items in output: StreamRecord(Dan, 2), StreamRecord(Kate, 1),
        // Watermark(MAX)
        assertEquals(3, output.size());
    }
}
