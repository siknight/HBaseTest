package first;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ThreadPoolExecutor;

public class hbaseTest {

    private static Configuration configuration = null;

    private static Connection connection = null;

    private static Admin admin = null;

    /**
     * 基本配置
     * 创建连接
     */
    static {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "hadoop102");
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
        //获取admin
        try {
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void close() {
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if (admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 测试hbase中的表是否存在
     *
     * @throws IOException
     */
    @Test
    public void test01() throws IOException {
        //配置
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "hadoop102");
        //下面这个端口号可以不写，默认是2181
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        //连接客户端
        HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);
        boolean b = hBaseAdmin.tableExists("student");
        System.out.println(b);
    }

    /**
     * 判断是否存在该表
     *
     * @throws IOException
     */
    @Test
    public void test02() throws IOException {
        System.out.println(admin.tableExists(TableName.valueOf("student")));
        System.out.println(admin.tableExists(TableName.valueOf("student02")));
    }

    /**
     * 创建表
     */

    @Test
    public void test03() throws IOException {
        //创建表描述器,表名需要转字节
        HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf("student02"));
        //添加列族   new HColumnDescriptor("info")创建列族描述器
        descriptor.addFamily(new HColumnDescriptor("info"));
        //创建表
        admin.createTable(descriptor);

    }

    /**
     * 删除表
     */
    @Test
    public void test04() throws IOException {
        admin.disableTable(TableName.valueOf("student02"));
        admin.deleteTable(TableName.valueOf("student02"));

    }


    /**
     * 向表中插入数据
     *
     * @throws IOException
     */
    @Test
    public void test05() throws IOException {
        Table table = connection.getTable(TableName.valueOf("student01"));
        //设置rowkey值
        Put put = new Put(Bytes.toBytes("1001"));
        //添加列族，列名，值
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("lisi"));
        table.put(put);
    }

    /**
     * 删除一条数据
     *
     * @throws IOException
     */
    @Test
    public void test06() throws IOException {
        Table table = connection.getTable(TableName.valueOf("student01"));
        Delete delete = new Delete(Bytes.toBytes("1001"));
        delete.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));
        table.delete(delete);
        table.close();
    }


    /**
     * 删除多条数据
     *
     * @throws IOException
     */
    @Test
    public void test06_2() throws IOException {
        Table table = connection.getTable(TableName.valueOf("student01"));
        ArrayList<Delete> deletes = new ArrayList<Delete>();
        String [] rowkeys=new String[]{"1001","1002"};
        for (String rowkey:rowkeys){
            Delete delete = new Delete(Bytes.toBytes(rowkey));
            deletes.add(delete);
        }
        table.delete(deletes);
    }




    /**
     * scan 全表扫描
     *
     * @throws IOException
     */
    @Test
    public void test07() throws IOException {
        Table table = connection.getTable(TableName.valueOf("student"));
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        for (Result result: scanner){
            Cell[] cells = result.rawCells();
            for (Cell cell : cells){
                System.out.println("roByteswkey:"+Bytes.toString(CellUtil.cloneRow(cell))
                +",cf:"+Bytes.toString(CellUtil.cloneFamily(cell))
                +",cn:"+Bytes.toString(CellUtil.cloneQualifier(cell))
                +",value:"+Bytes.toString(CellUtil.cloneValue(cell))
                );
            }
        }
        table.close();
    }


    /**
     * 得到一条数据
     * @throws IOException
     */
    @Test
    public void test08() throws IOException {
        Table table = connection.getTable(TableName.valueOf("student"));
        Result result = table.get(new Get(Bytes.toBytes("1001")));
        Cell[] cells = result.rawCells();

        for (Cell cell : cells){
            System.out.println("roByteswkey:"+Bytes.toString(CellUtil.cloneRow(cell))
                    +",cf:"+Bytes.toString(CellUtil.cloneFamily(cell))
                    +",cn:"+Bytes.toString(CellUtil.cloneQualifier(cell))
                    +",value:"+Bytes.toString(CellUtil.cloneValue(cell))
            );
        }

        table.close();

    }


    /**
     * 得到一条数据  指定列族
     * @throws IOException
     */
    @Test
    public void test09() throws IOException {
        Table table = connection.getTable(TableName.valueOf("student"));
        Get get = new Get(Bytes.toBytes("1001"));
        get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));
        Result result = table.get(get);
        Cell[] cells = result.rawCells();

        for (Cell cell : cells){
            System.out.println("roByteswkey:"+Bytes.toString(CellUtil.cloneRow(cell))
                    +",cf:"+Bytes.toString(CellUtil.cloneFamily(cell))
                    +",cn:"+Bytes.toString(CellUtil.cloneQualifier(cell))
                    +",value:"+Bytes.toString(CellUtil.cloneValue(cell))
            );
        }

        table.close();
    }


}