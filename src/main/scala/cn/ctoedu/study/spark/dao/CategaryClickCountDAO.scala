package cn.ctoedu.spark.dao

import cn.ctoedu.spark.domain.CategaryClickCount
import cn.ctoedu.spark.utils.HBaseUtils
import cn.ctoedu.spark.utils.HBaseUtil
import cn.ctoedu.spark.utils.HBaseConn
import org.apache.hadoop.hbase.client.{Get, HTable}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/**
  *
  */
object CategaryClickCountDAO {
    //val tableName = "test_list"
    val tableName = "categary_clickcount"
    val cf = "info"
    //val cf = "fileInfo"
    val qualifer = "click_count"

    /**
      * 保存数据
      * @param list
      */
    def save(list:ListBuffer[CategaryClickCount]): Unit ={
      val table = HBaseConn.getTable(tableName)
        for(els <- list){
            table.incrementColumnValue(Bytes.toBytes(els.categaryID),Bytes.toBytes(cf),Bytes.toBytes(qualifer),els.clickCout);
        }

    }

    def count(day_categary:String) : Long={
        val table  = HBaseConn.getTable(tableName)
        val get = new Get(Bytes.toBytes(day_categary))
        val  value =  table.get(get).getValue(Bytes.toBytes(cf), Bytes.toBytes(qualifer))
         if(value == null){
           0L
         }else{
             Bytes.toLong(value)
         }
    }

    def main(args: Array[String]): Unit = {
       val list = new ListBuffer[CategaryClickCount]
        list.append(CategaryClickCount("20171122_22",300))
        list.append(CategaryClickCount("20171122_42", 60))
        list.append(CategaryClickCount("20171122_52", 160))
        save(list)

        print(count("20171122_10")+"---")
    }

}
