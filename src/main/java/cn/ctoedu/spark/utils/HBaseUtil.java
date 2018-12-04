package cn.ctoedu.spark.utils;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class HBaseUtil {
    /**
     * 创建HBase表
     *
     * @param tableName 表名
     * @param cfs       列族的数组
     * @return 是否创建成功
     */
    public static boolean createTable(String tableName, String[] cfs) {
        try {
            HBaseAdmin admin = (HBaseAdmin) HBaseConn.getHBaseConn().getAdmin();
            if (admin.tableExists(tableName)) {
                return false;
            }
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            Arrays.stream(cfs).forEach(cf -> {
                HColumnDescriptor columnDescriptor = new HColumnDescriptor(cf);
                columnDescriptor.setMaxVersions(1);
                tableDescriptor.addFamily(columnDescriptor);
            });
            admin.createTable(tableDescriptor);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return true;
    }

    /**
     * 删除表
     *
     * @param tableName
     * @return
     */
    public static boolean deleteTable(String tableName) {
        try {
            HBaseAdmin admin = (HBaseAdmin) HBaseConn.getHBaseConn().getAdmin();
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }

    /**
     * 插入一条数据
     *
     * @param tableName 表名
     * @param rowKey    唯一标识
     * @param cfName    列族名
     * @param qualifier 列标识
     * @param data      数据
     * @return 是否插入成功
     */
    public static boolean putRow(String tableName, String rowKey, String cfName, String qualifier, String data) {
        try {
            Table table = HBaseConn.getTable(tableName);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(cfName), Bytes.toBytes(qualifier), Bytes.toBytes(data));
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }

    /**
     * 批量插入数据
     *
     * @param tableName
     * @param puts
     * @return
     */
    public static boolean putRows(String tableName, List<Put> puts) {
        try {
            Table table = HBaseConn.getTable(tableName);
            table.put(puts);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }

    /**
     * 获取单条数据
     *
     * @param tableName 表名
     * @param rowKey    唯一标识
     * @return 查询结果
     */
    public static Result getRow(String tableName, String rowKey) {
        try {
            Table table = HBaseConn.getTable(tableName);
            Get get = new Get(Bytes.toBytes(rowKey));
            return table.get(get);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 利用过滤器获取单条数据
     *
     * @param tableName
     * @param rowKey
     * @param filterList
     * @return
     */
    public static Result getRow(String tableName, String rowKey, FilterList filterList) {
        try {
            Table table = HBaseConn.getTable(tableName);
            Get get = new Get(Bytes.toBytes(rowKey));
            get.setFilter(filterList);
            return table.get(get);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * scan扫描表操作
     *
     * @param tableName
     * @return
     */
    public static ResultScanner getScanner(String tableName) {
        try {
            Table table = HBaseConn.getTable(tableName);
            Scan scan = new Scan();
            scan.setCaching(1000);
            return table.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 批量检索数据
     *
     * @param tableName   表名
     * @param startRowKey 起始RowKey
     * @param endRowKey   终止RowKey
     * @return resultScanner
     */
    public static ResultScanner getScanner(String tableName, String startRowKey, String endRowKey) {
        try {
            Table table = HBaseConn.getTable(tableName);
            Scan scan = new Scan();
            scan.setStartRow(Bytes.toBytes(startRowKey));
            scan.setStopRow(Bytes.toBytes(endRowKey));
            scan.setCaching(1000);
            return table.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 过滤扫描
     *
     * @param tableName
     * @param startRowKey
     * @param endRowKey
     * @param filterList
     * @return
     */
    public static ResultScanner getScanner(String tableName, String startRowKey, String endRowKey, FilterList filterList) {
        try {
            Table table = HBaseConn.getTable(tableName);
            Scan scan = new Scan();
            scan.setStartRow(Bytes.toBytes(startRowKey));
            scan.setStopRow(Bytes.toBytes(endRowKey));
            scan.setFilter(filterList);
            scan.setCaching(1000);
            return table.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 删除一条记录
     *
     * @param tableName 表名
     * @param rowKey    唯一标识行
     * @return 是否删除成功
     */
    public static boolean deleteRow(String tableName, String rowKey) {
        try {
            Table table = HBaseConn.getTable(tableName);
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }

    /**
     * 删除列族
     *
     * @param tableName
     * @param cfName
     * @return
     */
    public static boolean deleteColumnFamily(String tableName, String cfName) {
        try {
            HBaseAdmin admin = (HBaseAdmin) HBaseConn.getHBaseConn().getAdmin();
            admin.deleteColumn(tableName, cfName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }

    public static boolean deleteQualifier(String tableName, String rowKey, String cfName, String qualifier) {
        try {
            Table table = HBaseConn.getTable(tableName);
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            delete.addColumn(Bytes.toBytes(cfName), Bytes.toBytes(qualifier));
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;

    }

    public static void main(String[] args){
       // HBaseUtil.createTable("test_list",new String[]{"fileInfo"});
        //HBaseUtil.putRow("test_list", "rowkey21", "fileInfo", "click_count", "103027");
    }
}
