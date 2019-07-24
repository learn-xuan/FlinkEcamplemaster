package BatchAPI;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

public class BatchDemoDistinct {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ArrayList<String> list = new ArrayList<String>();
        list.add("hello man");
        list.add("hello boy");

        DataSource<String> source = env.fromCollection(list);

        FlatMapOperator<String, String> flatMap = source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> collector) throws Exception {
                String[] split = value.split(" ");

                for (String s : split) {
                    collector.collect(s);
                }
            }
        });

        flatMap.distinct().print();  // 对数据去重


    }

}
