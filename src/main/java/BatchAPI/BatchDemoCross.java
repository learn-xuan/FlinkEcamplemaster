package BatchAPI;


import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.CrossOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.table.expressions.In;

import java.util.ArrayList;

/**
 * 获取笛卡尔积
 */
public class BatchDemoCross {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ArrayList<String> data1 = new ArrayList<String>();
        ArrayList<Integer> data2 = new ArrayList<Integer>();

        data1.add("zs");
        data1.add("li");

        data2.add(1);
        data2.add(2);

        DataSource<String> source1 = env.fromCollection(data1);
        DataSource<Integer> source2 = env.fromCollection(data2);

        CrossOperator.DefaultCross<String, Integer> cross = source1.cross(source2);

        cross.print();


    }

}
