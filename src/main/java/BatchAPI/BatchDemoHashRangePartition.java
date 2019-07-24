package BatchAPI;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Hash-Partition
 *
 * Range-Partition
 *
 */
public class BatchDemoHashRangePartition {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ArrayList<Tuple2<Integer, String>> data = new ArrayList<Tuple2<Integer, String>>();

        data.add(new Tuple2<Integer, String>(1, "hello1"));
        data.add(new Tuple2<Integer, String>(2, "hello2"));
        data.add(new Tuple2<Integer, String>(2, "hello3"));
        data.add(new Tuple2<Integer, String>(3, "hello4"));
        data.add(new Tuple2<Integer, String>(3, "hello5"));
        data.add(new Tuple2<Integer, String>(3, "hello6"));
        data.add(new Tuple2<Integer, String>(4, "hello7"));
        data.add(new Tuple2<Integer, String>(4, "hello8"));
        data.add(new Tuple2<Integer, String>(4, "hello9"));
        data.add(new Tuple2<Integer, String>(4, "hello10"));
        data.add(new Tuple2<Integer, String>(5, "hello11"));
        data.add(new Tuple2<Integer, String>(5, "hello12"));
        data.add(new Tuple2<Integer, String>(5, "hello13"));
        data.add(new Tuple2<Integer, String>(5, "hello14"));
        data.add(new Tuple2<Integer, String>(5, "hello15"));
        data.add(new Tuple2<Integer, String>(6, "hello16"));
        data.add(new Tuple2<Integer, String>(6, "hello17"));
        data.add(new Tuple2<Integer, String>(6, "hello18"));
        data.add(new Tuple2<Integer, String>(6, "hello19"));
        data.add(new Tuple2<Integer, String>(6, "hello20"));
        data.add(new Tuple2<Integer, String>(6, "hello21"));

        DataSource<Tuple2<Integer, String>> source = env.fromCollection(data);

        /*source.partitionByHash(0).mapPartition(new MapPartitionFunction<Tuple2<Integer,String>, Tuple2<Integer,String>>() {
            @Override
            public void mapPartition(Iterable<Tuple2<Integer, String>> iterable, Collector<Tuple2<Integer, String>> collector) throws Exception {
                Iterator<Tuple2<Integer, String>> iterator = iterable.iterator();
                while (iterator.hasNext()){
                    Tuple2<Integer, String> next = iterator.next();
                    System.out.println("-----------当前线程id："+Thread.currentThread().getId()+","+next);
                }
            }
        }).print();*/

        source.partitionByRange(0).mapPartition(new MapPartitionFunction<Tuple2<Integer,String>, Tuple2<Integer,String>>() {
            @Override
            public void mapPartition(Iterable<Tuple2<Integer, String>> iterable, Collector<Tuple2<Integer, String>> collector) throws Exception {

                Iterator<Tuple2<Integer, String>> iterator = iterable.iterator();
                while (iterator.hasNext()){
                    Tuple2<Integer, String> next = iterator.next();
                    System.out.println("-----------当前线程id："+Thread.currentThread().getId()+","+next);
                }

            }
        }).print();

    }

}
