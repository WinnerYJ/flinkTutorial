package com.winner.api.windows;

import com.winner.event.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class LateDataTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        executionEnvironment.getConfig().setAutoWatermarkInterval(100);

        SingleOutputStreamOperator<Event> stream = executionEnvironment.socketTextStream("flink1", 7777).map(new MapFunction<String, Event>() {
            @Override
            public Event map(String s) throws Exception {
                String [] fields = s.split(",");

                return new Event(fields[0].trim(), fields[1].trim(), Long.valueOf(fields[2].trim()));
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event event, long l) {
                        return event.timestamp;
                    }
                }));

        stream.print("input");

        //统计每个url的访问量


        executionEnvironment.execute();
    }
}
