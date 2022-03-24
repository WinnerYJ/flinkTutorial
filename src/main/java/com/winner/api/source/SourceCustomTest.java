package com.winner.api.source;

import com.winner.event.ClickSource;
import com.winner.event.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SourceCustomTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);

        DataStreamSource<Event> eventDataStreamSource = executionEnvironment.addSource(new ClickSource());
        eventDataStreamSource.print();

        executionEnvironment.execute();
    }
}
