package flinkTable;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.Arrays;

public class StreamSQLExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.getTableEnvironment(env);

        StreamTableEnvironment tableEnvironment = TableEnvironment.getTableEnvironment(env);

        DataStreamSource<Order> orderA = env.fromCollection(Arrays.asList(
                new Order(1L, "beer", 3),
                new Order(1L, "diaper", 4),
                new Order(3L, "rubber", 2)
        ));
        DataStreamSource<Order> orderB = env.fromCollection(Arrays.asList(
                new Order(2L, "pen", 3),
                new Order(2L, "rubber", 3),
                new Order(3L, "beer", 1)
        ));

        // 将DataStream转换成Table有两种方式，一种是使用tableEnvironment.fromDataStream之间转换，一种是使用registerDataStream注册表
        Table tableA = tableEnvironment.fromDataStream(orderA ,"user, product, amount");

        Table tableB = tableEnvironment.fromDataStream(orderB, "user, product, amount");

//        Table result = tableEnv.sqlQuery("SELECT * FROM " + orderA + " WHERE amount > 2 UNION ALL"); //+ "SELECT * FROM " + orderB + " WHERE amount < 2");
        Table result = tableEnvironment.sqlQuery("select * from "+tableA); //+ "SELECT * FROM " + orderB + " WHERE amount < 2");

        tableEnvironment.toAppendStream(result, Order.class).print();

        env.execute("StreamSQLExample");


    }

    public static class Order{

        public Long user;
        public String product;
        public int amount;

        public Order(){}

        public Order(Long user, String product, int amount) {
            this.user = user;
            this.product = product;
            this.amount = amount;
        }

        @Override
        public String toString() {
            return "Order{" +
                    "user=" + user +
                    ", product='" + product + '\'' +
                    ", amount=" + amount +
                    '}';
        }
    }

}
