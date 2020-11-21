package com.hbase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.*;

import java.util.List;
import java.util.ArrayList;

import java.io.IOException;

public class HbOperation {
    Admin admin;
    Connection conn;

    //连接数据库
    public void connect(String mastername) throws IOException {
        //创建一个configuration
        Configuration conf = HBaseConfiguration.create();
        //目标主机名设为mastername
        conf.set("hbase.zookeeper.quorum", mastername);
        //连接数据库
        conn = ConnectionFactory.createConnection(conf);
        //获得admin
        admin=conn.getAdmin();
        System.out.println("Connect successfully!");
    }

    //关闭连接
    public void disconnect() throws IOException {
        admin.close();
        conn.close();
        System.out.println("Connection closed.");
    }

    //扫描表
    public void scanTable(String tablename) throws IOException {
        //获取table
        Table table=conn.getTable(TableName.valueOf(tablename));
        Scan scan=new Scan();
        ResultScanner resscan=table.getScanner(scan);
        System.out.println("ROW\tCOLUMN+CELL");
        //遍历每一行
        for (Result result:resscan) {
            //获取row key
            String row = new String(result.getRow());
            //将cell的内容放到list中
            List<Cell> cells = result.listCells();
            //打印每个列族列属性和对应value
            for (Cell c:cells) {
                System.out.println(row+"\t"+new String(CellUtil.cloneFamily(c))+":"+
                new String(CellUtil.cloneQualifier(c))+", value="+new String(CellUtil.cloneValue(c)));
            }
        }
    }

    //查询所有的表
    public void listTables() throws IOException {
        //从admin获取tablelist，然后遍历打印tablename
        HTableDescriptor[] tableDescriptor =admin.listTables();
        System.out.println("All tables:");
        for (int i=0; i<tableDescriptor.length;i++ ){ 
            System.out.println(tableDescriptor[i].getNameAsString()); 
        }
    }

    //列出所有列族
    public void listCF(String tablename) throws IOException {
        //先获取table
        Table table=conn.getTable(TableName.valueOf(tablename));
        List<String> cflist=new ArrayList<>();
        //然后获取table的信息
        HTableDescriptor hTableDescriptor=table.getTableDescriptor();
        //将列族名加入到list中
        for(HColumnDescriptor fdescriptor : hTableDescriptor.getColumnFamilies()){
            cflist.add(fdescriptor.getNameAsString());
        }
        //打印所有列族名
        System.out.println("Column Families of "+tablename+": "+String.join(", ", cflist));
        /*for(int i=0;i<list.size();i++) {
            System.out.print(list.get(i)+" ");
        }
        System.out.println();*/
    }

    //创建表
    public void createTable(String tablename, String[] families) throws IOException {
        //如果表已经存在则无法创建
        if (admin.tableExists(TableName.valueOf(tablename))) {
            System.out.println("Table "+tablename+" already exists!");
        } else {
            //先创建descriptor
            HTableDescriptor newtable = new HTableDescriptor(TableName.valueOf(tablename));
            //添加列族名
            for (String family:families) {
                newtable.addFamily(new HColumnDescriptor(family));
            }
            //提交创建
            admin.createTable(newtable);
            System.out.println("Create table "+tablename+" successfully!");
        }
    }

    //添加一项数据
    public void putData(String tablename, String rowKey, String family, String qualifier, String value)
    throws IOException {
        //获取table
        Table table=conn.getTable(TableName.valueOf(tablename));
        //设置row key
        Put put = new Put(Bytes.toBytes(rowKey));
        //将value插入到family:qualifier中
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
        table.put(put);
        System.out.println("One record inserted.");
    }

    //写入基本表数据
    public void putAlldata(String tablename) throws IOException { 
        putData(tablename, "001", "Description", "Name", "Li Lei");
        putData(tablename, "001", "Description", "Height", "176");
        putData(tablename, "001", "Courses", "Chinese", "80");
        putData(tablename, "001", "Courses", "Math", "90");
        putData(tablename, "001", "Courses", "Physics", "95");
        putData(tablename, "001", "Home", "Province", "Zhejiang");

        putData(tablename, "002", "Description", "Name", "Han Meimei");
        putData(tablename, "002", "Description", "Height", "183");
        putData(tablename, "002", "Courses", "Chinese", "88");
        putData(tablename, "002", "Courses", "Math", "77");
        putData(tablename, "002", "Courses", "Physics", "66");
        putData(tablename, "002", "Home", "Province", "Beijing");

        putData(tablename, "003", "Description", "Name", "Li Lei");
        putData(tablename, "003", "Description", "Height", "162");
        putData(tablename, "003", "Courses", "Chinese", "90");
        putData(tablename, "003", "Courses", "Math", "90");
        putData(tablename, "003", "Courses", "Physics", "95");
        putData(tablename, "003", "Home", "Province", "Shanghai");
        
    }

    //按照某列查询
    public void scanByColumn(String tablename, String family, String qualifier) throws IOException {
        Table table=conn.getTable(TableName.valueOf(tablename));
        //Scan scan=new Scan();
        //getScanner方法中设置列族和列名即可获得某列的scanner，接下来的操作和scan全表相同
        ResultScanner resscan=table.getScanner(family.getBytes(), qualifier.getBytes());
        System.out.println("ROW\tCOLUMN+CELL");
        for (Result result:resscan) {
            String row = new String(result.getRow());
            List<Cell> cells = result.listCells();
            for (Cell c:cells) {
                System.out.println(row+"\t"+new String(CellUtil.cloneFamily(c))+":"+
                new String(CellUtil.cloneQualifier(c))+", value="+new String(CellUtil.cloneValue(c)));
            }
        }
    }

    //添加Courses:English列的信息
    public void putEnglish(String tablename) throws IOException { 
        putData(tablename, "001", "Courses", "English", "95");
        putData(tablename, "002", "Courses", "English", "95");
        putData(tablename, "003", "Courses", "English", "95");
    }

    //添加一个新的列族
    public void addFamily(String tablename, String family) throws IOException {
        //获得原来表的定义信息
        HTableDescriptor tableDescriptor =  admin.getTableDescriptor(TableName.valueOf(tablename));
        //构造新的列族定义
        HColumnDescriptor nColumnDescriptor = new HColumnDescriptor(family);
        //将列族添加到表的定义中
        tableDescriptor.addFamily(nColumnDescriptor);
        //将修改后的表的定义提交到admin
        admin.modifyTable(TableName.valueOf(tablename), tableDescriptor);
        System.out.println("Add family "+family+" successfully!");
    }

    //添加Contact:Email列的信息
    public void putEmail(String tablename) throws IOException { 
        putData(tablename, "001", "Contact", "Email", "lilei@qq.com");
        putData(tablename, "002", "Contact", "Email", "hanmeimei@qq.com");
        putData(tablename, "003", "Contact", "Email", "xiaoming@qq.com");
    }

    //删除表
    public void dropTable(String tablename) throws IOException {
        if (admin.tableExists(TableName.valueOf(tablename))) {
            //如果表存在，则先disable表，然后才能删除表
            admin.disableTable(TableName.valueOf(tablename));
            admin.deleteTable(TableName.valueOf(tablename));
            System.out.println("Drop table "+tablename+" successfully!");
        } else {
            System.out.println("No such table: "+tablename);
        }
    }

    public static void main(String[] args) {
        //设置数据库所在的主机名称
        String mastername="node3";
        //设置需要操作的表名称
        String tablename="students";
        //设置表的列族
        String[] families= new String[] {"ID","Description","Courses","Home"};
        //创建operation对象
        HbOperation operation=new HbOperation();

        try{
            //连接数据库
            operation.connect(mastername);

            //展示所有的表
            operation.listTables();

            //（1）创建讲义中的students表；
            System.out.println("\n-------(1) Create table: students-------");
            //创建表
            operation.createTable(tablename,families);
            //查询表的所有列族
            operation.listCF(tablename);
            //添加数据
            operation.putAlldata(tablename); 

            //（2）扫描创建后的students表；
            System.out.println("\n-------(2) Scan table: students-------");
            operation.scanTable(tablename); 

            //（3）查询学生来自的省；
            System.out.println("\n-------(3) Query Home:Province-------");
            //查询Home:Province列
            operation.scanByColumn(tablename, "Home", "Province");

             //（4）增加新的列Courses:English，并添加数据；
            System.out.println("\n-------(4) Add new column, Courses:English-------");
            //添加数据
            operation.putEnglish(tablename);
            System.out.println("Data added:");
            //查询Courses:English列，验证插入是否成功
            operation.scanByColumn(tablename, "Courses", "English");

            //（5）增加新的列族Contact和新列Contact:Email，并添加数据；
            System.out.println("\n-------(5) Add new column family: Contact,new column Contact:Email-------");
            //添加新的列族
            operation.addFamily(tablename, "Contact");
            //查询所有列族，验证是否添加成功
            operation.listCF(tablename);
            //添加数据
            operation.putEmail(tablename);
            System.out.println("Data added:");
            //查询Contact:Email列，验证插入是否成功
            operation.scanByColumn(tablename, "Contact", "Email");

            //（6）删除students表。
            System.out.println("\n-------(6) Drop table: students-------");
            //删除前查询所有表
            operation.listTables();
            //删除表
            operation.dropTable(tablename);
            //删除后查询所有表，验证删除是否成功
            operation.listTables(); 

            //关闭连接
            operation.disconnect(); 

        }  catch (IOException e) {
            e.printStackTrace();
        }

    }
}