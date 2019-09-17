package com.lm.beam.file;

import com.lm.beam.model.IndexerPipelineOptions;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import java.util.ArrayList;
import java.util.List;

/**
 * 保存
 * @Author: limeng
 * @Date: 2019/8/27 15:16
 */
public class FileWriteTest {
    public static void main(String[] args) {
        IndexerPipelineOptions indexerPipelineOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(IndexerPipelineOptions.class);
        indexerPipelineOptions.setRunner(DirectRunner.class);
        Pipeline pipeline = Pipeline.create(indexerPipelineOptions);

        List<Row> rows=new ArrayList<>();

        Schema type = Schema.builder().addStringField("id").addStringField("name").build();
        for (int i = 0; i < 10; i++) {
            Row build = Row.withSchema(type).addValue("id" + i).addValue("name" + i).build();
            rows.add(build);
        }
        PCollection<Row> rowPCollection = pipeline.apply(Create.of(rows)).setCoder(SchemaCoder.of(type));
        rowPCollection.apply(ParDo.of(new DoFn<Row, String>() {
            private static final long serialVersionUID =1L;
            @ProcessElement
            public void processElement(ProcessContext c){
                Row element = c.element();
                String result = element.getString(0)+","+element.getString(1);
                c.output(result);
            }
        })).apply(TextIO.write().to("D:\\log\\cc.cvs"));

        pipeline.run().waitUntilFinish();
    }



}
