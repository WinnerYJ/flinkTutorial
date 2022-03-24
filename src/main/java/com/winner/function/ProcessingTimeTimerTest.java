package com.winner.function;

import com.winner.event.ClickSource;
import com.winner.event.Event;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class ProcessingTimeTimerTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        SingleOutputStreamOperator<Event> stream = executionEnvironment.addSource(new ClickSource());

        stream.keyBy(data -> data.user)
                .process(new KeyedProcessFunction<String, Event, String>() {
                    @Override
                    public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {
                        Long currentTimestamp = ctx.timerService().currentProcessingTime();
                        out.collect(ctx.getCurrentKey() + "的数据到达，到达时间：" + new Timestamp(currentTimestamp));
                        //注册一个10秒钟后的定时器
                        ctx.timerService().registerProcessingTimeTimer((currentTimestamp + 5*1000L));

                    }
                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect(ctx.getCurrentKey() + "的定时器触发，出发时间：" + new Timestamp(timestamp));
                    }
                }).print();

        executionEnvironment.execute();
    }
}
