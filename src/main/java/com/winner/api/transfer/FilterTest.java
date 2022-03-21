package com.winner.api.transfer;

import com.winner.event.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FilterTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);

        DataStreamSource<Event> dataStream = executionEnvironment.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L));

        //进行转换计算，提取user字段


        dataStream.filter(data -> data.user.equals("Alice")).print("lambda:");


        executionEnvironment.execute();
    }
}
