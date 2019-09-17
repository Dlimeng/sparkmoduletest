package com.lm.beam.sql;

import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;



/**
 *
 * Caused by: java.lang.ClassCastException: Cannot cast org.apache.beam.sdk.extensions.sql.impl.planner.BeamRelDataTypeSystem to org.apache.calcite.rel.type.RelDataTypeSystem
 * 	at java.lang.Class.cast(Class.java:3369)
 * 	at org.apache.calcite.avatica.ConnectionConfigImpl$4.apply(ConnectionConfigImpl.java:229)
 * 	... 24 more
 *
 * @Author: limeng
 * @Date: 2019/7/9 16:45
 */
public class BeamSqlExample {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        // 显式指定PipelineRunner：DirectRunner（Local模式）
        options.setRunner(DirectRunner.class);

        Pipeline pipeline = Pipeline.create(options);

       // DriverManager.getConnection(JdbcDriver.CONNECT_STRING_PREFIX)

//        pipeline.run().waitUntilFinish();
    }
}
