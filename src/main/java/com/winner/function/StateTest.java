package com.winner.function;

import com.winner.event.ClickSource;
import com.winner.event.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class StateTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = executionEnvironment.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(((SerializableTimestampAssigner<Event>) (event, l) -> event.timestamp))
                );

        stream.keyBy(data -> data.user)
                .flatMap(new MyFlatMap())
                .print();

        executionEnvironment.execute();
    }

    static class MyFlatMap extends RichFlatMapFunction<Event, String>{
        ValueState<Event> myValueState;
        ListState<Event> myListState;
        MapState<String, Long> myMapState;
        ReducingState<Event> myReducingState;
        AggregatingState<Event, String> myAggregatingState;



        Long count = 0L;

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<Event> valueStateDescriptor = new ValueStateDescriptor<Event>("my-state", Event.class);

            myValueState = getRuntimeContext().getState(valueStateDescriptor);
            myListState = getRuntimeContext().getListState(new ListStateDescriptor<Event>("my-list-state", Event.class));
            myMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("my-map-state", String.class, Long.class));
            myReducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Event>("my-reducing-state", new ReduceFunction<Event>() {
                @Override
                public Event reduce(Event event, Event t1) throws Exception {
                    return new Event(event.user, event.url, t1.timestamp);
                }
            }, Event.class));

            myAggregatingState = getRuntimeContext().
                    getAggregatingState(new AggregatingStateDescriptor<Event, Long, String>(
                            "my-aggregate-state",
                            new AggregateFunction<Event, Long, String>() {
                @Override
                public Long createAccumulator() {
                    return 0L;
                }

                @Override
                public Long add(Event event, Long aLong) {
                    return aLong + 1;
                }

                @Override
                public String getResult(Long accumulator) {
                    return "count: " + accumulator;
                }

                @Override
                public Long merge(Long aLong, Long acc1) {
                    return aLong + acc1;
                }
            }, Long.class));

            StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(1))
                    .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                    .build();

            valueStateDescriptor.enableTimeToLive(ttlConfig);
        }

        @Override
        public void flatMap(Event event, Collector<String> collector) throws Exception {
        //?????????????????????
//            System.out.println(myValueState.value());
//            myValueState.update(event);
//            System.out.println("my value:" + myValueState.value());

            myListState.add(event);
            System.out.println("my list state: " + event);

            myMapState.put(event.user, myMapState.get(event.user) == null ? 1 : myMapState.get(event.user) + 1);
            System.out.println("my map state: " + event.user + " " + myMapState.get(event.user));

            myAggregatingState.add(event);
            System.out.println("my aggregating state: " + myAggregatingState.get());

            myReducingState.add(event);
            System.out.println("reducing state: " + myReducingState.get());

            System.out.println("count: " + ++count);
        }
    }
}