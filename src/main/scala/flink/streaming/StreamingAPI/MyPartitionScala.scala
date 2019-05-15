package flink.streaming.StreamingAPI

import org.apache.flink.api.common.functions.Partitioner

/**
  * 自定义分区规则
  */
class MyPartitionScala extends Partitioner[Long]{

  override def partition(k: Long, i: Int): Int = {
    println("分区总数："+i)
    if(k%2 == 0){
      0
    }else{
      1
    }
  }

}
