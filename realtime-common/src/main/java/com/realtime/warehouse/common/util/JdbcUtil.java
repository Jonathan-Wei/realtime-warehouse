package com.realtime.warehouse.common.util;

import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import static com.realtime.warehouse.common.constant.Constant.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class JdbcUtil {
    /**
     * 获取Mysql连接
     * @return
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    public static Connection getMysqlConnection() throws ClassNotFoundException, SQLException {
        Class.forName(MYSQL_DRIVER);
        return DriverManager.getConnection(MYSQL_URL,MYSQL_USERNAME,MYSQL_PASSWORD)
    }

    /**
     * 查询数据
     * @param connection
     * @param querySql
     * @param clazz
     * @param isUnderScoreToCamel
     * @return
     * @param <T>
     * @throws Exception
     */
    public static <T> List<T> queryList(Connection connection,String querySql,Class<T> clazz,boolean... isUnderScoreToCamel) throws Exception {
        boolean defaultTsUToC = false; // 默认不执行下划线转驼峰

        if(isUnderScoreToCamel.length >0){
            defaultTsUToC = isUnderScoreToCamel[0];
        }

        List<T> result = new ArrayList<>();
        // 预编译
        PreparedStatement preparedStatement = connection.prepareStatement(querySql);
        // 执行查询，获取结果集
        ResultSet resultSet = preparedStatement.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();
        // 解析结果集，把数据封装到一个List集合中
        while (resultSet.next()){
            // 通过反射创建一个对象
            T t  = clazz.newInstance();
            // 遍历每一列，给对象的属性赋值
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                // 获取列名
                String columnName = metaData.getColumnName(i);
                // 获取列的值
                Object columnValue = resultSet.getObject(columnName);
                // 如果开启了下划线转驼峰
                if(defaultTsUToC){
                    // 下划线转驼峰
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,columnName);
                }
                // t.name=value
                BeanUtils.setProperty(t,columnName,columnValue);
            }
            result.add(t);
        }
        return result;
    }

    /**
     * 关闭连接
     * @param connection
     * @throws SQLException
     */
    public static void closeConnection(Connection connection) throws SQLException{
        if(connection != null && !connection.isClosed()){
            connection.close();
        }
    }
}
