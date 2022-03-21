package com.winner.api.transfer;

import com.winner.event.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class PartitionTest {
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

      //  dataStream.shuffle().print().setParallelism(4);
        
       // dataStream.rebalance().print().setParallelism(4);

        executionEnvironment.addSource(new RichParallelSourceFunction<Integer>() {
            @Override
            public void run(SourceContext<Integer> ctx) throws Exception {
                for(int i = 1; i <= 8; i++){
                    System.out.println("i:" + String.valueOf(i) + ", index:" + getRuntimeContext().getIndexOfThisSubtask());
                    if(i % 2 == getRuntimeContext().getIndexOfThisSubtask()){
                        ctx.collect(i);
                    }
                }
            }

            @Override
            public void cancel() {

            }
        }).setParallelism(2).rescale().print().setParallelism(4);

        executionEnvironment.execute();
    }

}
