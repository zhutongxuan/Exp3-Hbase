import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class studentInfoDB {
    public static Configuration configuration; //HBase 配置信息
    public static Connection connection; //HBase 连接
    public static Admin admin;

    //建立连接
    public void init () {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","localhost");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        try{
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        }catch(IOException e){
            e.printStackTrace();
        }
    }

    //关闭连接
    public void close() {
        try{
            if(admin != null) {
                admin.close();
            }
            if (null != connection) {
                connection.close();
            }
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    //检查现在所有的表
    public void listTables() throws IOException {
        //Getting all the list of tables using HBaseAdmin object
        HTableDescriptor[] tableDescriptor =admin.listTables();
        // printing all the table names.
        if(tableDescriptor.length == 0){
            System.out.println("no table.");
        }
        else{
            System.out.println("Tables:");
            for (int i=0; i<tableDescriptor.length; i++ ){
                System.out.println(tableDescriptor[i].getNameAsString());
            }
        }

    }

    /**
     * 创建 HBase 数据库表的时候，首先需要定义表的模型，包括表的名称、行键和列族的名称。
     * @param myTableName 表名
     * @param colFamily 列族
     * @throws IOException
     */
    public void createTable(String myTableName, String[] colFamily) throws IOException {
        TableName tableName = TableName.valueOf(myTableName);
        if(admin.tableExists(tableName)){
            System.out.println("table "+ myTableName +" exists!");
        } else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            for(String str:colFamily){
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(str);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            admin.createTable(hTableDescriptor);
            System.out.println("Successfully create table: "+ myTableName +"!");
        }
    }

    /**
     * 扫描全表
     * @param myTableName
     * @throws IOException
     */
    public void scanTable(String myTableName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(myTableName)); //获取table
        Scan scan = new Scan();
        ResultScanner scanResult = table.getScanner(scan);
        System.out.println("ROW\tCOLUMN+CELL"); //模仿shell表头

        //遍历每一行
        for (Result result : scanResult) {
            String row = new String(result.getRow()); //获取row key
            List<Cell> cells = result.listCells(); //将cell的内容放到list中
            //行键 column=列族名:列族限定, value=xxx
            for (Cell c:cells) {
                System.out.println(row+"\t"+new String(CellUtil.cloneFamily(c))+":"+
                        new String(CellUtil.cloneQualifier(c))+", value="+new String(CellUtil.cloneValue(c)));
            }
        }
    }

    //按照列来扫描
    public void scanTableByColumn(String myTableName, String colFamily, String col) throws IOException {
        Table table=connection.getTable(TableName.valueOf(myTableName));

        ResultScanner scanResult = table.getScanner(colFamily.getBytes(), col.getBytes()); //getScanner方法中设置列族和列名即可获得某列的scanner
        System.out.println("ROW\tCOLUMN+CELL");
        for (Result result : scanResult) {
            String row = new String(result.getRow());
            List<Cell> cells = result.listCells();
            for (Cell c:cells) {
                System.out.println(row+"\t"+new String(CellUtil.cloneFamily(c))+":"+
                        new String(CellUtil.cloneQualifier(c))+", value="+new String(CellUtil.cloneValue(c)));
            }
        }
    }

    //添加新的列族
    public void addFamily(String myTableName, String colFamily) throws IOException {
        HTableDescriptor tableDescriptor =  admin.getTableDescriptor(TableName.valueOf(myTableName)); //获得原来表的定义信息
        HColumnDescriptor nColumnDescriptor = new HColumnDescriptor(colFamily); //define a column family
        tableDescriptor.addFamily(nColumnDescriptor); //add new column family into table
        admin.modifyTable(TableName.valueOf(myTableName), tableDescriptor); //commit it to admin
        System.out.println("Add column family: "+colFamily+" successfully!");
    }

    /**
     * 插入一行数据
     * @param myTableName
     * @param rowKey
     * @param colFamily
     * @param col
     * @param val
     * @throws IOException
     */
    public void insertData(String myTableName, String rowKey, String colFamily, String col, String val) throws IOException {
        Table table = connection.getTable(TableName.valueOf(myTableName));
        Put put = new Put(rowKey.getBytes());
        put.addColumn(colFamily.getBytes(),col.getBytes(),val.getBytes());
        table.put(put);
        table.close();
    }

    public void deleteByCell(String myTableName, String rowKey, String colFamily, String col) throws IOException {
        Table table = connection.getTable(TableName.valueOf(myTableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        //删除指定列
        delete.addColumns(Bytes.toBytes(colFamily), Bytes.toBytes(col));
        table.delete(delete);
    }
    //删除表
    public void dropTable(String myTableName) throws IOException {
        if (admin.tableExists(TableName.valueOf(myTableName))) {
            //如果表存在，则先disable表，然后才能删除表
            admin.disableTable(TableName.valueOf(myTableName));
            admin.deleteTable(TableName.valueOf(myTableName));
            System.out.println("Drop table "+myTableName+" successfully!");
        } else {
            //如果表不存在，输出提示
            System.out.println("There is no table "+myTableName);
        }
    }

    public static void main (String [] args) throws IOException{
        //设置数据库所在的主机名称
        String masterName = "node3";
        String myTableName = "studentInfoDB";
        String[] families= new String[] {"S_info", "C_1", "C_2", "C_3"};  //设置表的列族

        studentInfoDB operation=new studentInfoDB(); //创建operation对象
        operation.init(); //建立连接

        System.out.println("\n-------[1] Create table: studentInfoDB -------");
        operation.createTable(myTableName, families);//建表

        //添加学生信息
        operation.insertData(myTableName,"2015001","S_info","S_Name", "Li Lei");
        operation.insertData(myTableName,"2015001","S_info","S_Sex", "male");
        operation.insertData(myTableName,"2015001","S_info","S_Age", "23");
        operation.insertData(myTableName,"2015002","S_info","S_Name", "Han Meimei");
        operation.insertData(myTableName,"2015002","S_info","S_Sex", "female");
        operation.insertData(myTableName,"2015002","S_info","S_Age", "22");
        operation.insertData(myTableName,"2015003","S_info","S_Name", "Li Lei");
        operation.insertData(myTableName,"2015003","S_info","S_Sex", "male");
        operation.insertData(myTableName,"2015003","S_info","S_Age", "24");
        //添加选课信息
        operation.insertData(myTableName,"2015001","C_1","SC_Cno", "123001");
        operation.insertData(myTableName,"2015001","C_1","C_Name", "Math");
        operation.insertData(myTableName,"2015001","C_1","C_Credit", "2.0");
        operation.insertData(myTableName,"2015001","C_1","SC_Score", "86");
        operation.insertData(myTableName,"2015001","C_3","SC_Cno", "123003");
        operation.insertData(myTableName,"2015001","C_3","C_Name", "English");
        operation.insertData(myTableName,"2015001","C_3","C_Credit", "3.0");
        operation.insertData(myTableName,"2015001","C_3","SC_Score", "69");
        operation.insertData(myTableName,"2015002","C_2","SC_Cno", "123002");
        operation.insertData(myTableName,"2015002","C_2","C_Name", "Computer Science");
        operation.insertData(myTableName,"2015002","C_2","C_Credit", "5.0");
        operation.insertData(myTableName,"2015002","C_2","SC_Score", "77");
        operation.insertData(myTableName,"2015002","C_3","SC_Cno", "123003");
        operation.insertData(myTableName,"2015002","C_3","C_Name", "English");
        operation.insertData(myTableName,"2015002","C_3","C_Credit", "3.0");
        operation.insertData(myTableName,"2015002","C_3","SC_Score", "99");
        operation.insertData(myTableName,"2015003","C_1","SC_Cno", "123001");
        operation.insertData(myTableName,"2015003","C_1","C_Name", "Math");
        operation.insertData(myTableName,"2015003","C_1","C_Credit", "2.0");
        operation.insertData(myTableName,"2015003","C_1","SC_Score", "98");
        operation.insertData(myTableName,"2015003","C_2","SC_Cno", "123002");
        operation.insertData(myTableName,"2015003","C_2","C_Name", "Computer Science");
        operation.insertData(myTableName,"2015003","C_2","C_Credit", "5.0");
        operation.insertData(myTableName,"2015003","C_2","SC_Score", "95");

        System.out.println("\n-------[1result] Scan Table Just Created -------");
        operation.scanTable(myTableName);//扫描全表，以检查建表结果

        System.out.println("\n-------[2] Query the score of students select course Computer Science -------");
        operation.scanTableByColumn(myTableName, "C_2", "SC_Score"); //scan C_2:SC_Score

        //添加Contact:Email列的信息
        System.out.println("\n-------[3] Add Contact:Email -------");
        operation.addFamily(myTableName, "Contact");
        operation.insertData(myTableName, "2015001", "Contact", "Email", "lilei@qq.com");
        operation.insertData(myTableName, "2015002", "Contact", "Email", "hmm@qq.com");
        operation.insertData(myTableName, "2015003", "Contact", "Email", "zs@qq.com");

        System.out.println("\n-------[3result] Scan Contact:Email Just Modified -------");
        operation.scanTableByColumn(myTableName, "Contact", "Email");//扫描Contact:Email列

        System.out.println("\n-------[4] Delete Student 2015003 Course Select Information -------");
        operation.deleteByCell(myTableName, "2015003","C_1", "SC_Cno");
        operation.deleteByCell(myTableName, "2015003","C_1", "C_Name");
        operation.deleteByCell(myTableName, "2015003","C_1", "C_Credit");
        operation.deleteByCell(myTableName, "2015003","C_1", "SC_Score");
        operation.deleteByCell(myTableName, "2015003","C_2", "SC_Cno");
        operation.deleteByCell(myTableName, "2015003","C_2", "C_Name");
        operation.deleteByCell(myTableName, "2015003","C_2", "C_Credit");
        operation.deleteByCell(myTableName, "2015003","C_2", "SC_Score");
        operation.deleteByCell(myTableName, "2015003","C_3", "SC_Cno");
        operation.deleteByCell(myTableName, "2015003","C_3", "C_Name");
        operation.deleteByCell(myTableName, "2015003","C_3", "C_Credit");
        operation.deleteByCell(myTableName, "2015003","C_3", "SC_Score");

        System.out.println("\n-------[4result] Scan Table Just Modified -------");
        operation.scanTable(myTableName);

        System.out.println("\n-------[5] Drop table studentInfoDB -------");
        operation.listTables(); //查询现在所有的表
        operation.dropTable(myTableName); //删除表
        operation.listTables(); //查询所有表，验证删除是否成功

        //关闭连接
        operation.close();
    }
}
