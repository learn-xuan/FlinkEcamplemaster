package datahub;

import com.aliyun.datahub.DatahubClient;
import com.aliyun.datahub.DatahubConfiguration;
import com.aliyun.datahub.auth.AliyunAccount;
import com.aliyun.datahub.common.data.Field;
import com.aliyun.datahub.common.data.FieldType;
import com.aliyun.datahub.common.data.RecordSchema;
import com.aliyun.datahub.common.data.RecordType;
import com.aliyun.datahub.model.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class dataHub_Demo {

    public static void main(String[] args) {

        // 初始化阿里云Account与DataHubConfiguration
        String accessId = "";
        String accessKey = "";
        String endpoint = "http://";   // 云服务器地址

        AliyunAccount account = new AliyunAccount(accessId, accessKey);
        DatahubConfiguration conf = new DatahubConfiguration(account, endpoint);

        // 初始化client, client是线程安全的
        DatahubClient client = new DatahubClient(conf);

        String topicName = "topic";
        String topicDesc = "topic_desc";  //topic的描述信息
        String project = "project_name";  // 项目名称

        // 创建topic
        createTopic(topicName, topicDesc, project, client);

        // 写数据
        writeData(topicName,topicDesc,project,client);

        // 读取数据
        readData(topicName, topicDesc, project, client);


    }

    /**
     * 创建topic
     * @param topicName
     * @param topicDesc
     * @param project
     * @param client
     */
    public static void createTopic(String topicName,String topicDesc,String project,DatahubClient client){

        // 创建结构化topic
        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("Id", FieldType.STRING));
        schema.addField(new Field("Name", FieldType.STRING));
        int shardCount = 5;  // Topic数据传输的并发通道数
        int lifeCycle = 3;  // 存活时间，单位/天
        client.createTopic(project,topicName,shardCount,lifeCycle,RecordType.TUPLE,schema,topicDesc);

        client.waitForShardReady(project, topicName);   // 等待服务端通道打开
    }

    /**
     * 写入数据
     * @param topicName
     * @param topicDesc
     * @param project
     * @param client
     */
    public static void writeData(String topicName,String topicDesc,String project,DatahubClient client){

        ListShardResult listShardResult = client.listShard(project, topicName);
        ArrayList<RecordEntry> recordEntries = new ArrayList<RecordEntry>();

        //获取可用的shared
        String shardId = listShardResult.getShards().get(0).getShardId();

        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("Id", FieldType.STRING));
        schema.addField(new Field("Name", FieldType.STRING));

        RecordEntry entry = new RecordEntry(schema);
        entry.setString(0,"1");
        entry.setString(1,"ruyi");
        entry.setShardId(shardId);

        recordEntries.add(entry);

        PutRecordsResult result = client.putRecords(project, topicName, recordEntries);

//        if(result.getFailedRecordCount() != 0){
//            List<ErrorEntry> errors = result.getFailedRecordError();
//        }


    }

    /**
     * 读取数据
     * @param topicName
     * @param topicDesc
     * @param project
     * @param client
     */
    public static void readData(String topicName,String topicDesc,String project,DatahubClient client){

        ListShardResult listShardResult = client.listShard(project, topicName);
        String shardId = listShardResult.getShards().get(0).getShardId();

        GetCursorResult cursors = client.getCursor(project, topicName, shardId, GetCursorRequest.CursorType.OLDEST);
//        GetCursorResult cursors = client.getCursor(project, topicName, shardId, System.currentTimeMillis() - 24*3600*1000);

        int limit = 100;
        String cursor = cursors.getCursor();  // 获取指针


        while (true){
            GetBlobRecordsResult records = client.getBlobRecords(project, topicName, shardId, cursor, limit);

            List<BlobRecordEntry> recordEntries = records.getRecords();

            if(recordEntries.size() == 0){
                //没有消息
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else {
                Iterator<BlobRecordEntry> iterator = recordEntries.iterator();
                while (iterator.hasNext()){
                    BlobRecordEntry next = iterator.next();
                    byte[] data = next.getData();
                    System.out.println(data.toString());
                }
            }

            cursor = records.getNextCursor(); //获取下一个

        }


    }

}
