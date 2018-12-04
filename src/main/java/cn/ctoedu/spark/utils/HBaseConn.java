package cn.ctoedu.spark.utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

public class HBaseConn {
    private static final HBaseConn INSTANCE = new HBaseConn();
    private static Configuration configuration;
    private static Connection connection;
    private static final String HADOOP_USER_NAME = "hbase.hadoop.user";
    //hbase服务器上可登录连接hbase的用户名
    private static String oozieUser = "root";

    private HBaseConn() {
        //设置当前window/linux下用户为HBase可访问用户，这里为hbase服务器上的root用户，否则会出现访问权限问题
        System.setProperty("HADOOP_USER_NAME", "root");
        if (configuration == null) {
            configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum", "node1.hde.h3c.com,node2.hde.h3c.com,node3.hde.h3c.com");
            configuration.set("hbase.zookeeper.property.clientPort", "2181");
            //需设置zookeeper.znode.parent，不然会包空指针错误
            configuration.set("zookeeper.znode.parent", "/hbase-unsecure");
            //hbase服务器上可登录连接hbase的用户名
            configuration.set("hadoop.user.name", "root");
        }
    }

    /**
     * 创建数据库连接
     *
     * @return
     */
    public Connection getConnection() {
        if (connection == null || connection.isClosed()) {
            try {
                connection = ConnectionFactory.createConnection(configuration);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return connection;
    }

    /**
     * 获取数据库连接
     *
     * @return
     */
    public static Connection getHBaseConn() {

        return INSTANCE.getConnection();
    }

    /**
     * 获取表实例
     *
     * @param tableName
     * @return
     * @throws IOException
     */
    public static Table getTable(String tableName) throws IOException {
        return INSTANCE.getConnection().getTable(TableName.valueOf(tableName));
    }

    /**
     * 关闭连接
     */
    public static void closeConn() {
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    public static void main(String[] args) {
        //测试连接
        Connection conn = HBaseConn.getHBaseConn();
        System.out.println(conn.isClosed());
        HBaseConn.closeConn();
        System.out.println(conn.isClosed());
        //测试获取表
        try {
            Table table = HBaseConn.getTable("category_clickcount");
            System.out.println(table.getName().getNameAsString());
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
