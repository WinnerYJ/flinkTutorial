package com.winner.function;

import com.winner.event.ClickSource;
import com.winner.event.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class AverageTimestampExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = executionEnvironment.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));

        stream.print("intput");

        stream.keyBy(data -> data.user)
                        .flatMap(new AvgTsResult(5L))
                                .print();

        executionEnvironment.execute();
    }

    private static class AvgTsResult extends RichFlatMapFunction<Event, String> {
        //?????????????????????????????????????????????
        AggregatingState<Event, Long> aggregatingState;
        private Long count;

        //??????????????????????????????????????????
        ValueState<Long> countState;

        public AvgTsResult(Long count) {
            this.count = count;
        }

        @Override
        public void flatMap(Event event, Collector<String> collector) throws Exception {
            //?????????????????????+1
            Long currentCount = countState.value();
            if(currentCount == null){
                currentCount = 1L;
            }else{
                currentCount++;
            }
            countState.update(currentCount);
            aggregatingState.add(event);

            //????????????count????????????????????????
            if(currentCount.equals(count)){
                collector.collect(event.user + "?????? " + count + "??????????????????????????????" + aggregatingState.get());
                //????????????
                countState.clear();
            //    aggregatingState.clear();
            }
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            aggregatingState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Event, Tuple2<Long, Long>, Long>(
                    "avg-ts"
                    , new AggregateFunction<Event, Tuple2<Long, Long>, Long>() {
                @Override
                public Tuple2<Long, Long> createAccumulator() {
                    return Tuple2.of(0L, 0L);
                }

                @Override
                public Tuple2<Long, Long> add(Event event, Tuple2<Long, Long> accumulator) {
                    return Tuple2.of(accumulator.f0 + event.timestamp, accumulator.f1 + 1);
                }

                @Override
                public Long getResult(Tuple2<Long, Long> accumulator) {
                    return accumulator.f0 / accumulator.f1;
                }

                @Override
                public Tuple2<Long, Long> merge(Tuple2<Long, Long> longLongTuple2, Tuple2<Long, Long> acc1) {
                    return null;
                }
            },
                    Types.TUPLE(Types.LONG, Types.LONG)
            ));

            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count", Long.class));

        }
    }
}
