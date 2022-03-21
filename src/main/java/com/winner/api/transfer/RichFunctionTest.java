package com.winner.api.transfer;

import com.winner.event.Event;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RichFunctionTest {
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


        dataStream.map(new MyRichMapper()).print();

        executionEnvironment.execute();
    }

    public static class MyRichMapper extends RichMapFunction<Event, Integer>{
        private Integer i = 0;
        @Override
        public Integer map(Event event) throws Exception {
            i++;
            System.out.println("i:" + i);
            return event.url.length();
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            System.out.println("open生命周期被调用:" + getRuntimeContext().getIndexOfThisSubtask() + "号任务");
        }

        @Override
        public void close() throws Exception {
            super.close();
            System.out.println("close生命周期被调用:" + getRuntimeContext().getIndexOfThisSubtask() + "号任务");
        }
    }
}
