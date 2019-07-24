package BatchAPI;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;

public class BatchDemoJoin {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ArrayList<Tuple2<Integer, String>> data1 = new ArrayList<Tuple2<Integer, String>>();
        ArrayList<Tuple2<Integer, String>> data2 = new ArrayList<Tuple2<Integer, String>>();

        data1.add(new Tuple2<Integer, String>(1,"zs"));
        data1.add(new Tuple2<Integer, String>(2,"ls"));
        data1.add(new Tuple2<Integer, String>(3,"ww"));

        data2.add(new Tuple2<Integer, String>(1,"beijing"));
        data2.add(new Tuple2<Integer, String>(2,"shanghai"));
        data2.add(new Tuple2<Integer, String>(3,"guangzhou"));

        DataSource<Tuple2<Integer, String>> source1 = env.fromCollection(data1);
        DataSource<Tuple2<Integer, String>> source2 = env.fromCollection(data2);

        source1.join(source2).where(0) // 指定第一个数据集中需要进行比较的元素的下标
                .equalTo(0)  // 指定第二个数据集中需要进行比较的元素下标
                .with(new JoinFunction<Tuple2<Integer,String>, Tuple2<Integer,String>, Tuple3<Integer,String,String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        return new Tuple3<Integer, String, String>(first.f0,first.f1,second.f1);
                    }
                }).print();

        System.out.println("=====================");

        // 这里用map和上面使用with效果是一样的
        source1.join(source2).where(0)
                .equalTo(0)
                .map(new MapFunction<Tuple2<Tuple2<Integer,String>,Tuple2<Integer,String>>, Tuple3<Integer,String,String>>() {
                    @Override
                    public Tuple3<Integer, String, String> map(Tuple2<Tuple2<Integer, String>, Tuple2<Integer, String>> value) throws Exception {
                        return new Tuple3<Integer, String, String>(value.f0.f0,value.f0.f1,value.f1.f1);
                    }
                }).print();


    }

}
