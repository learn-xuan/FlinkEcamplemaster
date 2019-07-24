package BatchAPI;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;

public class BatchWorldCountJava {

    public static void main(String[] args) throws Exception {

        // 获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 读取文件
        DataSource<String> textFile = env.readTextFile("/Users/wangxuan/Documents/workDir/FlinkEcamplemaster/src/main/resources/world.txt");

        AggregateOperator<Tuple2<String, Integer>> sum = textFile.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {

                String[] split = value.toLowerCase().split(",");
                for (String s : split) {
                    if (s.length() > 0) {

                        collector.collect(new Tuple2<String, Integer>(s, 1));

                    }
                }

            }
        }).groupBy(0).sum(1);

        sum.writeAsText("/Users/wangxuan/Documents/workDir/FlinkEcamplemaster/src/main/resources/result.txt").setParallelism(1);

        env.execute("Batch WorldCount for Java");

    }

}
