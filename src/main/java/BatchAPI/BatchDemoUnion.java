package BatchAPI;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;

public class BatchDemoUnion {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ArrayList<Tuple2<Integer, String>> data1 = new ArrayList<Tuple2<Integer, String>>();
        ArrayList<Tuple2<Integer, String>> data2 = new ArrayList<Tuple2<Integer, String>>();

        data1.add(new Tuple2<Integer, String>(1, "zs"));
        data1.add(new Tuple2<Integer, String>(2, "ls"));
        data1.add(new Tuple2<Integer, String>(3, "ww"));

        data2.add(new Tuple2<Integer, String>(1, "lili"));
        data2.add(new Tuple2<Integer, String>(2, "jack"));
        data2.add(new Tuple2<Integer, String>(3, "jessic"));

        DataSource<Tuple2<Integer, String>> source1 = env.fromCollection(data1);
        DataSource<Tuple2<Integer, String>> source2 = env.fromCollection(data2);

        source1.union(source2).print();


    }

}
