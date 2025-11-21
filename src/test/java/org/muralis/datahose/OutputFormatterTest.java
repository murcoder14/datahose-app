package org.muralis.datahose;

import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class OutputFormatterTest {

    @Test
    public void testOutputFormatter() {
        StreamingApp.OutputFormatter formatter = new StreamingApp.OutputFormatter();
        String result = formatter.map(new Tuple2<>("Dan", 7));
        assertEquals("Dan visited the gym 7 times", result);
    }
}
