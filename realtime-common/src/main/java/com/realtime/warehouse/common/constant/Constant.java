package com.realtime.warehouse.common.constant;

public class Constant {
    public static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
    public static final String TOPIC_DB = "topic_db";
    public static final String TOPIC_LOG = "topic_log";
    public static final String MYSQL_HOST="hadoop102";
    public static final int MYSQL_PORT=3306;
    public static final String MYSQL_USERNAME="root";
    public static final String MYSQL_PASSWORD="123456";
    public static final String PROCESS_DATABASE="gmall2023";
    public static final String PROCESS_DIM_TABLE_NAME="table_process_dim";
    public static final String MYSQL_DRIVER="com.mysql.jdbc.Driver";
    public static final String MYSQL_URL="jdbc:mysql://"+MYSQL_HOST+":"+MYSQL_PORT+"/realtime_warehouse?useSSL=false&characterEncoding=utf8";
    public static final String HBASE_NAMESPACE="realtime_warehouse";

    public static final String HBASE_ZOOKEEPER_QUORUM="";

    public static final String TOPIC_DWD_TRAFFIC_START= "dwd_traffic_start";
    public static final String TOPIC_DWD_TRAFFIC_ERR = "dwd_traffic_err";

    public static final String TOPIC_DWD_TRAFFIC_PAGE = "dwd_traffic_page";
    public static final String TOPIC_DWD_TRAFFIC_ACTION = "dwd_traffic_action";
    public static final String TOPIC_DWD_TRAFFIC_DISPLAY = "dwd_traffic_display";

    public static final String TOPIC_DWD_INTERACTION_COMMENT_INFO= "dwd_interaction_comment_info";
    public static final String TOPIC_DWD_TRACE_CART_ADD = "dwd_trace_cart_add";
    public static final String TOPIC_DWD_TRACE_ORDER_DETAIL = "dwd_trace_order_detail";
    public static final String TOPIC_DWD_TRACE_ORDER_CANCEL= "dwd_trace_order_cancel";
    public static final String TOPIC_DWD_TRACE_ORDER_PAYMENT_SUCCESS= "dwd_trace_order_payment_success";
    public static final String TOPIC_DWD_TRACE_ORDER_REFUND= "dwd_trace_order_refund";
    public static final String TOPIC_DWD_TRACE_REFUND_PAYMENT_SUCCESS= "dwd_trace_refund_payment_success";
    public static final String TOPIC_DWD_USER_REGISTER = "dwd_user_register";




}
