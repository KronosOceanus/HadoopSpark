package hbase_api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class TestAPI {

    private static Connection connection = null;
    private static Admin admin = null;

    static {
        try {
            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum", "node1,node2,node3");

            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void close() throws IOException {
        if (admin != null){
            admin.close();
        }
        if (connection != null){
            connection.close();
        }
    }


    private static boolean isTableExist(String tableName) throws IOException {
        return admin.tableExists(TableName.valueOf(tableName));
//        admin.close();
    }

    //列族
    private static void createTable(String tableName, String... cfs) throws IOException {
        if (cfs.length <= 0){
            System.out.println("请设置列族");
            return;
        }
        if (isTableExist(tableName)){
            System.out.println(tableName + "表已存在");
        }

        List<ColumnFamilyDescriptor> cfList = new LinkedList<>();
        for (String cf : cfs){
            cfList.add(ColumnFamilyDescriptorBuilder.newBuilder(cf.getBytes()).build());
        }

        TableDescriptor tableDescriptor = TableDescriptorBuilder.
                newBuilder(TableName.valueOf(tableName)).
                setColumnFamilies(cfList).build();

        admin.createTable(tableDescriptor);
    }

    private static void deleteTable(String tableName) throws IOException {
        if (! isTableExist(tableName)){
            System.out.println(tableName + "表不存在");
            return;
        }
        admin.disableTable(TableName.valueOf(tableName));
        admin.deleteTable(TableName.valueOf(tableName));
    }

    private static void createNameSpace(String nameSpace){
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();
        try {
            admin.createNamespace(namespaceDescriptor);
        }catch (NamespaceExistException e){
            System.out.println(nameSpace + "命名空间已存在");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void putData(String tableName, String rowKey, String cf, String cn,
                                String value) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));   // tableName 之后的参数，每个 put 对应一个 rowKey
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn), Bytes.toBytes(value));
        table.put(put);
        table.close();
    }

    private static void getData(String tableName, String rowKey, String cf, String cn) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));    //获取特定列
        get.setMaxVersions();   //获取所有版本
        Result result = table.get(get);

        for (Cell cell : result.rawCells()){
            System.out.println("CF:" + Bytes.toString(CellUtil.cloneFamily(cell)) +
                    "----CN:" + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                    "----value:" + Bytes.toString(CellUtil.cloneValue(cell)));
        }

        table.close();
    }

    private static void scanData(String tableName) throws IOException{
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan(Bytes.toBytes("1001"), Bytes.toBytes("1001"));     //扫描范围
        ResultScanner rs = table.getScanner(scan);

        for (Result result : rs){
            for (Cell cell : result.rawCells()){
                System.out.println("CF:" + Bytes.toString(CellUtil.cloneFamily(cell)) +
                        "----CN:" + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                        "----value:" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }

        table.close();
    }

    private static void deleteData(String tableName, String rowKey, String cf, String cn) throws IOException{
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
//        delete.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));     //删除单个版本
//        delete.addColumns(Bytes.toBytes(cf), Bytes.toBytes(cn));     //删除所有版本
        delete.addFamily(Bytes.toBytes(cf));    //删除列族
        table.delete(delete);
        table.close();
    }
    
    
    public static void main(String[] args) throws Exception {
//        System.out.println(isTableExist("student"));
//        createTable("teacher", "info1", "info2");
//        deleteTable("teacher");
//        createNameSpace("namespace1");
//        putData("student", "1001", "info", "name", "apitest");
//        getData("student", "1001", "info", "name");
//        scanData("student");
//        deleteData("student", "1001", "info", "name");
    }
}
