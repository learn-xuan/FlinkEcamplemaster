package BatchAPI;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * broadcast广播变量
 *
 *
 *
 * 需求：
 * flink会从数据源中获取到用户的姓名
 *
 * 最终需要把用户的姓名和年龄信息打印出来
 *
 * 分析：
 * 所以就需要在中间的map处理的时候获取用户的年龄信息
 *
 * 建议吧用户的关系数据集使用广播变量进行处理
 *
 *
 *
 *
 * 注意：如果多个算子需要使用同一份数据集，那么需要在对应的多个算子后面分别注册广播变量
 *
 *
 *
 */
public class BatchDemoBroadCast {

    public static void main(String[] args) throws Exception {

        // 获得运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 1.准备需要广播的数据
        ArrayList<Tuple2<String, Integer>> broadCast = new ArrayList<Tuple2<String, Integer>>();
        broadCast.add(new Tuple2<String, Integer>("zs",18));
        broadCast.add(new Tuple2<String, Integer>("ls",28));
        broadCast.add(new Tuple2<String, Integer>("ww",38));
        broadCast.add(new Tuple2<String, Integer>("zl",48));

        DataSource<Tuple2<String, Integer>> source = env.fromCollection(broadCast);

        // 1.1处理需要广播的数据，把数据集转换成map类型，map中的key为用户姓名，value为用户年龄
        DataSet<HashMap<String, Integer>> broadCastmap = source.map(new MapFunction<Tuple2<String, Integer>, HashMap<String, Integer>>() {
            @Override
            public HashMap<String, Integer> map(Tuple2<String, Integer> data) throws Exception {

                HashMap<String, Integer> map = new HashMap<String, Integer>();

                String name = data.f0;
                Integer age = data.f1;
                map.put(name, age);
                return map;
            }
        });

        // 原数据
        DataSource<String> data = env.fromElements("zs", "ls", "ww");

        DataSet<String> result = data.map(new RichMapFunction<String, String>() {

            List<HashMap<String, Integer>> broadCastMap = new ArrayList<HashMap<String, Integer>>();
            HashMap<String, Integer> allMap = new HashMap<String, Integer>();

            /**
             * 这个方法只会执行一次
             * 可以在这里实现一些初始化的功能
             *
             * 所以，就可以在open方法中获取广播变量数据
             *
             */
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                // 3.获取广播变量
                this.broadCastMap = getRuntimeContext().getBroadcastVariable("broadCastMapName");
                for (HashMap map : broadCastMap) {
                    allMap.putAll(map);
                }

            }

            @Override
            public String map(String s) throws Exception {

                Integer age = allMap.get(s);

                return s + "," + age;
            }
        }).withBroadcastSet(broadCastmap, "broadCastMapName");  // 2.执行广播数据的操作

        result.print();

    }

}
