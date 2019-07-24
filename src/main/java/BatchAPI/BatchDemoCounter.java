package BatchAPI;


import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.configuration.Configuration;

/**
 * 全局累加器
 *
 * counter 计数器
 *
 * 需求：
 * 计算map函数中处理了多少数据
 *
 *
 * 注意：只有在任务执行结束后，才能获取到累加器的值
 *
 *
 *
 * Created by xuwei.tech on 2018/10/8.
 */
public class BatchDemoCounter {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> data = env.fromElements("a", "b", "c", "d");

        DataSet<String> result = data.map(new RichMapFunction<String, String>() {

            // 创建累加器
            private IntCounter numList = new IntCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                // 注册累加器
                getRuntimeContext().addAccumulator("num-lines", numList);
            }

            @Override
            public String map(String s) throws Exception {

                this.numList.add(1);

                return s;
            }
        });

        result.writeAsText("/Users/wangxuan/Documents/workDir/FlinkEcamplemaster/src/main/resources/counter.txt");

        JobExecutionResult counter = env.execute("counter");

        Object num = counter.getAccumulatorResult("num-lines");

        System.out.println("++++++++++++:" + num);

    }

}
