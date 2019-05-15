package flinkTable;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.Arrays;

public class StreamSqlExample2 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = TableEnvironment.getTableEnvironment(env);

        DataStreamSource<StreamSQLExample.Order> orderA = env.fromCollection(Arrays.asList(
                new StreamSQLExample.Order(1L, "beer", 3),
                new StreamSQLExample.Order(1L, "diaper", 4),
                new StreamSQLExample.Order(3L, "rubber", 2)
        ));
        DataStreamSource<StreamSQLExample.Order> orderB = env.fromCollection(Arrays.asList(
                new StreamSQLExample.Order(2L, "pen", 3),
                new StreamSQLExample.Order(2L, "rubber", 3),
                new StreamSQLExample.Order(3L, "beer", 1)
        ));

        // 注册orderA表
        tableEnvironment.registerDataStream("orderA", orderA, "user, product, amount");
        tableEnvironment.registerDataStream("orderB", orderB, "user, product, amount");

        Table result = tableEnvironment.sqlQuery("select * from orderA where amount > 2 union all" +
                " select * from orderB where amount > 2");


        tableEnvironment.toAppendStream(result, StreamSQLExample.Order.class).print();

        env.execute();


    }
}

