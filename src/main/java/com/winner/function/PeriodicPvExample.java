package com.winner.function;

import com.winner.event.Event;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Calendar;

public class PeriodicPvExample {
    public static void main(String[] args)  throws Exception{
        long timeInMillis = Calendar.getInstance().getTimeInMillis();

        System.out.println( "" + new Timestamp(timeInMillis / 5 * 5));
//        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
//        executionEnvironment.setParallelism(1);
//
//        SingleOutputStreamOperator<Event> eventSingleOutputStreamOperator = executionEnvironment.addSource(new ClickSource())
//                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
//                        .withTimestampAssigner((new SerializableTimestampAssigner<Event>() {
//                            @Override
//                            public long extractTimestamp(Event event, long l) {
//                                return event.timestamp;
//                            }
//                        }))
//                );
//
//        System.out.println("input: " + eventSingleOutputStreamOperator);
//
//        //统计每个用户的pv
//        eventSingleOutputStreamOperator.keyBy(data -> data.user)
//                .process(new PeriodicPvResult())
//                .print();
//
//        executionEnvironment.execute();
    }

    private static class PeriodicPvResult extends KeyedProcessFunction<String, Event, String> {

        ValueState<Long> countState;
        ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count", Long.class));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-ts", Long.class));

        }

        @Override
        public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {
            //来一条数据，就更新对应的count
            Long count = countState.value();
            countState.update(count == null ? 1 : count + 1);

            //如果没有注册的话，注册定时器
            if(timerTsState.value() != null){
                ctx.timerService().registerEventTimeTimer(value.timestamp + 10 * 1000L);
                timerTsState.update(value.timestamp + 10 * 1000L);
            }
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect(ctx.getCurrentKey() + " pv: " + countState.value());

            timerTsState.clear();

            ctx.timerService().registerEventTimeTimer(timestamp + 10 * 1000L);
            timerTsState.update(timestamp + 10 * 1000L );
        }
    }
}
