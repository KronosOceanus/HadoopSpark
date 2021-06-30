package flink_api;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * DataStream
 * Lambda
 */
public class WordCount3_Yarn {

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String output = "";
        if (parameterTool.has("output")){
            output = parameterTool.get("output");
        }else {
            System.out.println("-----------use default output------------");
            output = "hdfs://node1:8020/output/flink_wordcount_output";
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setRuntimeMode(RuntimeExecutionMode.BATCH); //批处理
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING); //流处理
        DataStream<String> lines = env.fromElements("hadoop version spark",
                "spark hadoop version", "kafka spark hadoop", "kafka");

        DataStream<String> words = lines.flatMap((String s, Collector<String> collector) -> {
            Arrays.stream(s.split(" ")).forEach(collector::collect);
        })
                .returns(Types.STRING);

        DataStream<Tuple2<String, Integer>> wordAndOne = words.map(s -> Tuple2.of(s, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT));

        KeyedStream<Tuple2<String, Integer>, String> grouped = wordAndOne.keyBy(
                stringIntegerTuple2 -> stringIntegerTuple2.f0);
        DataStream<Tuple2<String, Integer>> result = grouped.sum(1);

        // yarn 集群运行权限问题
        System.setProperty("HADOOP_USER_NAME", "root");
        result.writeAsText(output + System.currentTimeMillis()).setParallelism(1);  //设置并行度

        result.print();

        env.execute("jobName");
    }
}
