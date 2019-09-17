package com.lm.beam.sql.extensions;




import org.apache.beam.sdk.transforms.*;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;


/**
 * @Author: limeng
 * @Date: 2019/7/29 18:42
 */
public class BeamSqlDslUdfUdafTest{
    public static void main(String[] args) {
//        PipelineOptions options = PipelineOptionsFactory.create();
//        // 显式指定PipelineRunner：DirectRunner（Local模式）
//        options.setRunner(DirectRunner.class);
//
//        Pipeline pipeline = Pipeline.create(options);
//
//
//        String sql2="select cubic2(name)  from PCOLLECTION";
//
//
//        Schema beamSchema = Schema.builder().addStringField("name").build();
//
//        String name="lisi";
//
//        Row row = Row.withSchema(beamSchema).addValue(name).build();
//
//        PCollection<Row> p1 = pipeline.apply(Create.of(row).withRowSchema(beamSchema));
//
//
//        PCollection<Row> p2 =PCollectionTuple.of(new TupleTag<Row>("PCOLLECTION"), p1)
//                .apply(
//                        "testUdf2", SqlTransform.query(sql2).registerUdf("cubic2", new CubicIntegerFn()));
//        pipeline.run().waitUntilFinish();
    }



    public static class CubicIntegerFn implements SerializableFunction<String, String> {
        /**
         *
         */
        private static final long serialVersionUID = 1L;

        @Override
        public String apply(String input) {
            System.out.print("输出："+input);
            return input +"ccccc" ;
        }
    }


}
