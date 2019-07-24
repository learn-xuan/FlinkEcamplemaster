package BatchAPI;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;

/**
 * 获取集合的前N项
 */
public class BatchDemoFirstN {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ArrayList<Tuple2<Integer, String>> data = new ArrayList<Tuple2<Integer, String>>();

        data.add(new Tuple2<Integer, String>(2, "zs"));
        data.add(new Tuple2<Integer, String>(4, "ls"));
        data.add(new Tuple2<Integer, String>(3, "ww"));
        data.add(new Tuple2<Integer, String>(1, "xw"));
        data.add(new Tuple2<Integer, String>(1, "aw"));
        data.add(new Tuple2<Integer, String>(1, "nw"));

        DataSource<Tuple2<Integer, String>> text = env.fromCollection(data);

        // 获取前三条数据。按照数据插入的顺序
        text.first(3).print();
        System.out.println("==============================");

        // 根据数据中的第一列进行分组，在对第二列进行排序，获取每组的前两个元素
        text.groupBy(0).sortGroup(1,Order.ASCENDING).first(2).print();
        System.out.println("==============================");


        // 不分组，全局排序获取集合中的前三个元素，第一列生序，第二列降序
        text.sortPartition(0, Order.ASCENDING).sortPartition(1, Order.DESCENDING).first(3).print();


    }

}
