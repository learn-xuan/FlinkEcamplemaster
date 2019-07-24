package BatchAPI;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;

/**
 * 外连接
 *
 * 左外连接
 * 右外连接
 * 全外连接
 *
 */
public class BatchDemoOuter {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ArrayList<Tuple2<Integer, String>> data1 = new ArrayList<Tuple2<Integer, String>>();
        ArrayList<Tuple2<Integer, String>> data2 = new ArrayList<Tuple2<Integer, String>>();

        // 用户id，姓名
        data1.add(new Tuple2<Integer, String>(1, "zs"));
        data1.add(new Tuple2<Integer, String>(2, "ls"));
        data1.add(new Tuple2<Integer, String>(3, "ww"));

        // 用户id，所在城市
        data2.add(new Tuple2<Integer, String>(1, "beijing"));
        data2.add(new Tuple2<Integer, String>(2, "shanghai"));
        data2.add(new Tuple2<Integer, String>(3, "guangzhou"));

        DataSource<Tuple2<Integer, String>> source1 = env.fromCollection(data1);
        final DataSource<Tuple2<Integer, String>> source2 = env.fromCollection(data2);

        /**
         * 左外连接
         */
        source1.leftOuterJoin(source2)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer,String>, Tuple2<Integer,String>, Tuple3<Integer, String,String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        if (second == null){
                            return new Tuple3<Integer, String, String>(first.f0, first.f1,"null");
                        }else {
                            return new Tuple3<Integer, String, String>(first.f0, first.f1, second.f1);
                        }
                    }
                }).print();

        System.out.println("=================================================");

        /**
         * 右外连接
         */
        source1.rightOuterJoin(source2)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer,String>, Tuple2<Integer,String>, Tuple3<Integer, String,String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {

                        if(first == null){
                            return new Tuple3<Integer, String, String>(second.f0,"null", second.f1);
                        }else {
                            return new Tuple3<Integer, String, String>(first.f0,first.f1,second.f1);
                        }
                    }
                }).print();

        System.out.println("=================================================");

        /**
         * 全外连接
         */
        source1.fullOuterJoin(source2)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer,String>, Tuple2<Integer,String>, Tuple3<Integer, String,String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        if(first == null){
                            return new Tuple3<Integer, String, String>(second.f0,"null", second.f1);
                        }else if (second == null){
                            return new Tuple3<Integer, String, String>(first.f0, first.f1,"null");
                        }else {
                            return new Tuple3<Integer, String, String>(first.f0,first.f1,second.f1);
                        }

                    }
                }).print();

    }

}
