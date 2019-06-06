import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by JackManWu on 2018/3/2.
 */
public class HbaseMain {
    private static final Logger LOGGER = LoggerFactory.getLogger(HbaseMain.class);

    private static Configuration configuration = HBaseConfiguration.create();

    static {
        //Hbase连接配置
        configuration.set("hbase.rootdir", "hdfs://master:9000/hbase");
        configuration.set("hbase.zookeeper.quorum", "master,slave1,slave2");
    }

    /**
     * 创建表
     *
     * @param tableName
     * @param columnFamily
     */
    public static void createTable(String tableName, String columnFamily) {
        Connection connection = null;
        Admin admin = null;
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
            TableName table = TableName.valueOf(tableName);
            if (admin.tableExists(table)) {
                LOGGER.warn("{}表已经存在", tableName);
                return;
            }
            HTableDescriptor descriptor = new HTableDescriptor(table);
            descriptor.addFamily(new HColumnDescriptor(columnFamily));
            admin.createTable(descriptor);
            LOGGER.info("创建表{}成功", tableName);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (admin != null) {
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 数据
     *
     * @param tableName
     * @param rowKey
     * @param family
     * @param qualifier
     * @param value
     */
    public static void insert(String tableName, String rowKey, String family, String qualifier, String value) {
        Connection connection = null;
        Table table = null;
        try {
            connection = ConnectionFactory.createConnection(configuration);
            table = connection.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void delete(String tableName, String rowKey, String family, String qualifier) {
        Connection connection = null;
        Table table = null;
        try {
            connection = ConnectionFactory.createConnection(configuration);
            table = connection.getTable(TableName.valueOf(tableName));
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            delete.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) {
//        createTable("tb_shop", "shop_name");
//        insert("tb_shop", "ts_10000", "shop_name", "name", "苹果特卖店");
        delete("tb_shop", "ts_10000", "shop_name","name");
    }
}
