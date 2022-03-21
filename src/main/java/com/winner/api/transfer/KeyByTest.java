package com.winner.api.transfer;

import com.winner.event.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeyByTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);

        DataStreamSource<Event> dataStream = executionEnvironment.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Bob", "./prod?id=1", 3300L),
                new Event("Bob", "./prod?id=2", 3500L),
                new Event("Bob", ".home", 3800L),
                new Event("Bob", "./prod?id=4", 4200L),
                new Event("Alice", "./prod?id=2", 4300L));

        dataStream.keyBy(data -> data.user).max("timestamp").print("max:");

        dataStream.keyBy(data -> data.user).maxBy("timestamp").print("maxBy:");


        executionEnvironment.execute();
    }
}
