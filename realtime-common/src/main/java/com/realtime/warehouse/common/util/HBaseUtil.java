package com.realtime.warehouse.common.util;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

import static com.realtime.warehouse.common.constant.Constant.HBASE_ZOOKEEPER_QUORUM;

public class HBaseUtil {
    /**
     * 获取HBase连接
     * @return
     */
    public static Connection getConnection() {
        // 使用Configuration对象获取连接
//        Configuration conf = new Configuration();
//        conf.set("hbase.zookeeper.quorum",HBASE_ZOOKEEPER_QUORUM);
        Connection connection = null;
        try{
            connection = ConnectionFactory.createConnection();
        }catch (Exception e){
            e.printStackTrace();
        }

        return connection;
    }

    /**
     * 关闭连接
     * @param connection 一个HBase的同步连接
     */
    public static void close(Connection connection) {
        if(connection != null && !connection.isClosed()){
            try {
                connection.close();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    /**
     * 创建表
     * @param connection
     * @param namespace
     * @param tableName
     * @param columnFamily
     * @throws IOException
     */
    public static void createTable(Connection connection,String namespace,String tableName,String... columnFamily) throws IOException {

        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(namespace, tableName));

        for (String family : columnFamily) {
            ColumnFamilyDescriptor familyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family))
                    .build();
            tableDescriptorBuilder.setColumnFamily(familyDescriptor);
        }

        try{
            admin.createTable(tableDescriptorBuilder.build());
        }catch (Exception e){
            System.out.println("当前表格已存在，不需要重复创建："+namespace+"."+tableName);
        }

        admin.close();

    }

    /**
     * 删除表
     * @param connection
     * @param tableName
     * @throws IOException
     */
    public static void dropTable(Connection connection,String namespace,String tableName) throws IOException{
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();

        try {
            admin.disableTable(TableName.valueOf(namespace,tableName));
            admin.deleteTable(TableName.valueOf(namespace,tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }

        admin.close();
    }

    /**
     * 插入数据
     * @param connection
     * @param namespace
     * @param tableName
     * @param rowKey
     * @param family
     * @param data
     * @throws IOException
     */
    public static void putCells(Connection connection, String namespace, String tableName, String rowKey, String family, JSONObject data) throws IOException {
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));
        Put put = new Put(Bytes.toBytes(rowKey));

        for (String key : data.keySet()) {
            String columnValue = data.getString(key);
            if(columnValue!=null){
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(key), Bytes.toBytes(columnValue));
            }
        }

        try {
            table.put(put);
        }catch (IOException e) {
            e.printStackTrace();
        }
        table.close();
    }

    /**
     * 删除一整行数据
     * @param connection
     * @param namespace
     * @param tableName
     * @param rowKey
     * @throws IOException
     */
   public static void deleteCells(Connection connection,String namespace, String tableName, String rowKey) throws IOException {
       Table table = connection.getTable(TableName.valueOf(namespace, tableName));

       Delete delete = new Delete(Bytes.toBytes(rowKey));

       try {
           table.delete(delete);
       } catch (IOException e) {
           e.printStackTrace();
       }
       table.close();
   }
}
