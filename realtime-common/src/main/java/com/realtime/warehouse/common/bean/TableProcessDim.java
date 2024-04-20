package com.realtime.warehouse.common.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableProcessDim {
    //来源表名
    String sourceTable;
    // 目标表名
    String sinkTable;
    // 数据到HBase的列簇
    String sinkFamily;
    // sink 到 HBse 的时候的主键名
    String sinkRowKey;
    // sink 到 HBase 的时候的列名
    String sinkColumns;
    //配置表操作类型
    String op;
}
