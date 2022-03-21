package com.winner.api.transfer;

import com.winner.event.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformMapTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);

        DataStreamSource<Event> dataStream = executionEnvironment.fromElements(
                new Event("Mary", "./home", 1000L));

        //进行转换计算，提取user字段
        SingleOutputStreamOperator<String> result = dataStream.map(new MyMapper());
        SingleOutputStreamOperator<String> map = dataStream.map(data -> data.user);
        result.print();
        map.print();

        executionEnvironment.execute();
    }

    //自定义MapFunction
    public static class MyMapper implements MapFunction<Event, String>{

        @Override
        public String map(Event event) throws Exception {
            return event.getUser();
        }
    }
}
