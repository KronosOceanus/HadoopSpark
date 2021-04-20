package flink_api;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 使用 DataSet 实现
 */
public class WordCount {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> lineDS = env.fromElements("hadoop version spark",
                "spark hadoop version", "kafka spark hadoop", "kafka");

        //必须使用匿名内部类，不能使用 lambda，会丢失方法的类型信息
        DataSet<String> words = lineDS.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] arr = s.split(" ");
                for (String ss : arr){
                    collector.collect(ss);
                }
            }
        });
        DataSet<Tuple2<String, Integer>> wordAndOne = words.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return Tuple2.of(s, 1);
            }
        });
        UnsortedGrouping<Tuple2<String, Integer>> grouped = wordAndOne.groupBy(0);
        AggregateOperator<Tuple2<String, Integer>> result = grouped.sum(1);

        result.print();
    }
}
