package com.realtime.warehouse.dim.app;

import com.alibaba.fastjson.JSONObject;
import com.realtime.warehouse.common.base.BaseApp;
import com.realtime.warehouse.common.bean.TableProcessDim;
import com.realtime.warehouse.common.constant.Constant;
import com.realtime.warehouse.common.util.FlinkSourceUtil;
import com.realtime.warehouse.common.util.HBaseUtil;
import com.realtime.warehouse.common.util.JdbcUtil;
import com.realtime.warehouse.dim.app.function.DimBroadcaseFuntion;
import com.realtime.warehouse.dim.app.function.DimHBaseSinkFunction;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.sql.DriverManager;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class DimApp extends BaseApp {
    public static void main(String[] args) {
        new DimApp().start(10001,4,"dim_app", Constant.TOPIC_DB);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 核心业务逻辑 实现dim层纬度表的同步任务
        //1. 对ods读取对原始数据进行数据清晰
        /*stream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                boolean flat  = false;
                try{
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    String type = jsonObject.getString("type");
                    String database = jsonObject.getString("database");
                    JSONObject data = jsonObject.getJSONObject("data");

                    if("gmall".equals(database)
                            && "bootstrap-start".equals(type)
                            && "bootstrap-complete".equals(type)
                            && data != null
                            && data.size() != 0){
                        flat = true;
                    }

                    flat = true;
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return flat;
            }
        }).map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                return JSONObject.parseObject(value);
            }
        });*/

        SingleOutputStreamOperator<JSONObject> jsonObjectStream = etl(stream);

        //2. 使用flinkCDC读取监控配置表数据
        MySqlSource<String> mySQLSource = FlinkSourceUtil.getMySQLSource(Constant.PROCESS_DATABASE, Constant.PROCESS_DIM_TABLE_NAME);
        DataStreamSource<String> mysqlStreamSource = env.fromSource(mySQLSource, WatermarkStrategy.noWatermarks(), "mysql_source" ).setParallelism(1);

//        mysqlStreamSource.print();

        //3. 在HBase中创建纬度表
        SingleOutputStreamOperator<TableProcessDim> createTableStream = createHBaseTable(mysqlStreamSource).setParallelism(1);

//        createTableStream.print();

        //4. 做成广播流
        // 广播状态的key用来判断是否是纬度表，value用来补充信息写出到HBase
        MapStateDescriptor<String, TableProcessDim> broadcastState = new MapStateDescriptor<>("broadcast_state", String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastStream = createTableStream.broadcast(broadcastState);

        //5. 连接主流和广播流
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimStream = connectStream(jsonObjectStream, broadcastStream, broadcastState);
//        dimStream.print();
        //6. 筛选出需要写出对字段
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> filterColumnStream =filterColumnStream(dimStream);

//        filterColumnStream.print();

        //7. 写出到HBase
        filterColumnStream.addSink(new DimHBaseSinkFunction());
    }

    private static SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>>  filterColumnStream(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimStream) {
        return dimStream.map(new MapFunction<Tuple2<JSONObject, TableProcessDim>, Tuple2<JSONObject, TableProcessDim>>() {

            @Override
            public Tuple2<JSONObject, TableProcessDim> map(Tuple2<JSONObject, TableProcessDim> value) throws Exception {
                JSONObject jsonObject = value.f0;
                TableProcessDim dim = value.f1;

                String sinkColumns = dim.getSinkColumns();
                List<String> columns = Arrays.asList(sinkColumns.split("," ));
                JSONObject data = jsonObject.getJSONObject("data" );
                data.keySet().removeIf(key -> !columns.contains(key));
                return value;
            }
        });
    }

    private static SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> connectStream(SingleOutputStreamOperator<JSONObject> jsonObjectStream, BroadcastStream<TableProcessDim> broadcastStream, MapStateDescriptor<String, TableProcessDim> broadcastState) {
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectStream = jsonObjectStream.connect(broadcastStream);
        return  connectStream.process(new DimBroadcaseFuntion(broadcastState)).setParallelism(1);
    }

    private static SingleOutputStreamOperator<TableProcessDim> createHBaseTable(DataStreamSource<String> mysqlStreamSource) {
        SingleOutputStreamOperator<TableProcessDim> createTableStream = mysqlStreamSource.flatMap(new RichFlatMapFunction<String, TableProcessDim>() {
            Connection connection;

            @Override
            public void open(Configuration parameters) throws Exception {
                // 获取HBase连接
                connection = HBaseUtil.getConnection();
            }

            @Override
            public void close() throws Exception {
                // 关闭HBase连接
                connection.close();
            }

            @Override
            public void flatMap(String value, Collector<TableProcessDim> out) throws Exception {
                //使用读取的配置表数据，到HBase中创建与之对应的表哥
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    String op = jsonObject.getString("op" );
                    TableProcessDim dim;
                    if ("d".equals(op)) {
                        dim = jsonObject.getObject("before", TableProcessDim.class);
                        // 当配置表发送一个D类型的数据，对应HBase要删除一张纬度表
                        deleteTable(dim);
                    } else if ("c".equals(op) || "r".equals(op)) {
                        dim = jsonObject.getObject("after", TableProcessDim.class);
                        // 当配置表发送一个C或者R类型的数据，对应HBase要创建一张纬度表
                        createTable(dim);

                    } else {
                        dim = jsonObject.getObject("after", TableProcessDim.class);
                        deleteTable(dim);
                        createTable(dim);
                    }
                    dim.setOp(op);
                    out.collect(dim);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            private void createTable(TableProcessDim dim) {
                String sinkFamily = dim.getSinkFamily();
                String[] split = sinkFamily.split("," );
                try {
                    HBaseUtil.createTable(connection, Constant.HBASE_NAMESPACE, dim.getSinkTable(), split);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            private void deleteTable(TableProcessDim dim) {
                try {
                    HBaseUtil.dropTable(connection, Constant.HBASE_NAMESPACE, dim.getSinkTable());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        return createTableStream;
    }

    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    String type = jsonObject.getString("type" );
                    String database = jsonObject.getString("database" );
                    JSONObject data = jsonObject.getJSONObject("data" );

                    if ("gmall".equals(database)
                            && "bootstrap-start".equals(type)
                            && "bootstrap-complete".equals(type)
                            && data != null
                            && data.size() != 0) {
                        out.collect(jsonObject);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }
}
