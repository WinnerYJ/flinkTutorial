package com.winner.function;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class BroadcastBehaviorTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        executionEnvironment.enableCheckpointing(10000L);
        executionEnvironment.setStateBackend(new EmbeddedRocksDBStateBackend());
        //1定义用户的行为数据流
        DataStreamSource<Action> actionDataStreamSource = executionEnvironment.fromElements(
                new Action("Alice", "login"),
                new Action("Alice", "pay"),
                new Action("Bob", "login"),
                new Action("Bob", "order")
        );

        //2行为模式流，通过广播流构建
        DataStreamSource<Pattern> patternDataStreamSource = executionEnvironment.fromElements(
                new Pattern("login", "pay"),
                new Pattern("login", "order")
        );

        //构建广播流，通过广播状态描述器
        MapStateDescriptor<Void, Pattern> patternMapStateDescriptor = new MapStateDescriptor<>("pattern", Types.VOID, Types.POJO(Pattern.class));
        BroadcastStream<Pattern> broadcastStream = patternDataStreamSource.broadcast(patternMapStateDescriptor);
        //连接两条流
        SingleOutputStreamOperator<Tuple2<String, Pattern>> matches = actionDataStreamSource.keyBy(data -> data.userId)
                .connect(broadcastStream)
                .process(new PatternDetector());

        matches.print();

        executionEnvironment.execute();

    }

    public static class Action{
        public String userId;
        public String action;

        public Action() {
        }

        public Action(String userId, String action) {
            this.userId = userId;
            this.action = action;
        }

        @Override
        public String toString() {
            return "Action{" +
                    "userId='" + userId + '\'' +
                    ", action='" + action + '\'' +
                    '}';
        }
    }

    public static class Pattern{
        public String action1;
        public String action2;

        public Pattern(){

        }

        public Pattern(String action1, String action2) {
            this.action1 = action1;
            this.action2 = action2;
        }

        @Override
        public String toString() {
            return "Pattern{" +
                    "action1='" + action1 + '\'' +
                    ", action2='" + action2 + '\'' +
                    '}';
        }
    }

    public static class PatternDetector extends KeyedBroadcastProcessFunction<String, Action, Pattern, Tuple2<String, Pattern>> {
        //定义一个keyedState保存当前用户的上一个行为
        ValueState<String> preActionState;

        @Override
        public void open(Configuration parameters) throws Exception {
            preActionState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last-action", String.class));
        }

        @Override
        public void processElement(Action value, KeyedBroadcastProcessFunction<String, Action, Pattern, Tuple2<String, Pattern>>.ReadOnlyContext ctx, Collector<Tuple2<String, Pattern>> out) throws Exception {
            //从广播状态中获取匹配模式
            ReadOnlyBroadcastState<Void, Pattern> patternState = ctx.getBroadcastState(new MapStateDescriptor<>("pattern", Types.VOID, Types.POJO(Pattern.class)));
            Pattern pattern = patternState.get(null);

            //拿到上一次的行为，来判断是否可以匹配到模式
            String prevAction = preActionState.value();

            //判断是否匹配
            if(pattern != null && prevAction != null){
                if(pattern.action1.equals(prevAction) && pattern.action2.equals(value.action)){
                    out.collect(new Tuple2<>(ctx.getCurrentKey(), pattern));
                }
            }
            //当没有匹配成功的
            preActionState.update(value.action);
        }

        @Override
        public void processBroadcastElement(Pattern value, KeyedBroadcastProcessFunction<String, Action, Pattern, Tuple2<String, Pattern>>.Context ctx, Collector<Tuple2<String, Pattern>> out) throws Exception {
        //从上下文获取广播状态，并用当前数据更新状态
            BroadcastState<Void, Pattern> patternState = ctx.getBroadcastState(new MapStateDescriptor<>("pattern", Types.VOID, Types.POJO(Pattern.class)));
            patternState.put(null, value);
        }
    }
}
