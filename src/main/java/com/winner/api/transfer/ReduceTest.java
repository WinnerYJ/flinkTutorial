package com.winner.api.transfer;

import com.winner.event.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReduceTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);

        DataStreamSource<Event> dataStream = executionEnvironment.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Bob", "./prod?id=1", 3300L),
                new Event("Alice", "./prod?id=100", 3400L),
                new Event("Bob", "./prod?id=2", 3500L),
                new Event("Bob", ".home", 3800L),
                new Event("Bob", "./prod?id=4", 4200L),
                new Event("Alice", "./prod?id=2", 4300L));

        //1. 统计每个用户的访问频次
        SingleOutputStreamOperator<Tuple2<String, Long>> reduce = dataStream.map(new MapFunction<Event, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(Event event) throws Exception {
                return Tuple2.of(event.user, 1L);
            }
        }).keyBy(data -> data.f0).reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> t0, Tuple2<String, Long> t1) throws Exception {
                return Tuple2.of(t0.f0, t0.f1 + t1.f1);
            }
        });

        //2. 选取当前最活跃的用户
        SingleOutputStreamOperator<Tuple2<String, Long>> result = reduce.keyBy(data -> "key").reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> t0, Tuple2<String, Long> t1) throws Exception {
                return t0.f1 > t1.f1 ? t0 : t1;
            }
        });

        result.print();

        executionEnvironment.execute();
    }
}
