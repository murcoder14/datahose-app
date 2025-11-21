package org.muralis.datahose;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

public class TokenizerTest {

    @Test
    public void testTokenizer() {
        StreamingApp.Tokenizer tokenizer = new StreamingApp.Tokenizer();
        Collector<Tuple2<String, Integer>> collector = Mockito.mock(Collector.class);

        tokenizer.flatMap("Dan,2024-10-21 10:15:00", collector);

        verify(collector, times(1)).collect(new Tuple2<>("Dan", 1));
    }

    @Test
    public void testTokenizerInvalidInput() {
        StreamingApp.Tokenizer tokenizer = new StreamingApp.Tokenizer();
        Collector<Tuple2<String, Integer>> collector = Mockito.mock(Collector.class);

        tokenizer.flatMap("InvalidRecord", collector);

        verify(collector, times(0)).collect(Mockito.any());
    }
}
