package com.lm.beam.transform;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;

import java.beans.PropertyVetoException;
import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: limeng
 * @Date: 2019/8/28 18:50
 */
public class HIveTransFormTest implements Serializable {
    public static void main(String[] args) throws PropertyVetoException {
        PipelineOptions options = PipelineOptionsFactory.create();
        options.setRunner(DirectRunner.class);
        Pipeline pipeline = Pipeline.create(options);

        PCollection<Row> t1 = select(pipeline, "select * from t1");
        PCollection<Row> t2 = select(pipeline, "select * from t2");
        PCollection<Double> entid = t2.apply(ParDo.of(new DoFn<Row, Double>() {
            private static final long serialVersionUID = 1L;
            @ProcessElement
            public void processElement(ProcessContext ctx) throws Exception {
                Row element = ctx.element();
                ctx.output(element.getDouble("entid"));
            }
        }));



        PCollection<Row> m1 = PCollectionList.of(t1).and(t2).apply(Flatten.<Row>pCollections());
       // String cName="entid";
        PCollection<Row> d1 = m1.apply(Distinct.withRepresentativeValueFn(new SerializableFunction<Row, String>() {

            @Override
            public String apply(Row input) {
                return input.getString("");
            }
        }).withRepresentativeType(TypeDescriptor.of(String.class))
        );

//        Filter.compose(new SerializableFunction<PInput, POutput>() {
//            @Override
//            public POutput apply(PInput input) {
//                return null;
//            }
//        })

//        d1.apply(ParDo.of(new DoFn<Row, Void>() {
//            private static final long serialVersionUID =1L;
//            @ProcessElement
//            public void processElement(ProcessContext ctx) throws Exception {
//                Row element = ctx.element();
//                List<Object> values = element.getValues();
//                String out="";
//                for(Object o:values){
//                    out +=o.toString()+" ";
//                }
//                System.out.println(out);
//            }
//
//        }));
        pipeline.run().waitUntilFinish();
    }


    public static JdbcIO.Read<Row> select(String sql) throws PropertyVetoException {
        String driverClass="com.mysql.jdbc.Driver";
        ComboPooledDataSource cpds = new ComboPooledDataSource();
        cpds.setDriverClass(driverClass);
        cpds.setJdbcUrl("jdbc:mysql://192.168.20.115:3306/test?useSSL=false");
        cpds.setUser("root");
        cpds.setPassword("root");
        Schema build = Schema.builder().addStringField("id").addStringField("name").addStringField("entid").build();
        return JdbcIO.<Row>read().
                withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
                        .create(cpds)

                ).withCoder(SchemaCoder.of(build))
                .withQuery(sql)
                .withRowMapper(new JdbcIO.RowMapper<Row>() {
                    @Override
                    public Row mapRow(ResultSet resultSet) throws SQLException {
                        //映射
                        Row resultSetRow = null;
                        Schema build = Schema.builder().addStringField("id").addStringField("name").addStringField("entid").build();
                        resultSetRow = Row.withSchema(build).addValue(resultSet.getString(1)).addValue(resultSet.getString(2)).addValue(resultSet.getString(3)).build();
                        return resultSetRow;
                    }
                });
    }

    public  static  PCollection<Row>  select(Pipeline pipeline,String sql) throws PropertyVetoException {
        String driverClass="com.mysql.jdbc.Driver";


        ComboPooledDataSource cpds = new ComboPooledDataSource();
        cpds.setDriverClass(driverClass);
        cpds.setJdbcUrl("jdbc:mysql://192.168.20.115:3306/test?useSSL=false");
        cpds.setUser("root");
        cpds.setPassword("root");
        Schema build = Schema.builder().addStringField("id").addStringField("name").addStringField("entid").build();
        JdbcIO.Read<Row> rowRead = JdbcIO.<Row>read().
                withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
                        .create(cpds)

                ).withCoder(SchemaCoder.of(build))
                .withQuery(sql)
                .withRowMapper(new JdbcIO.RowMapper<Row>() {
                    @Override
                    public Row mapRow(ResultSet resultSet) throws SQLException {
                        //映射
                        Row resultSetRow = null;
                        Schema build = Schema.builder().addStringField("id").addStringField("name").addStringField("entid").build();
                        resultSetRow = Row.withSchema(build).addValue(resultSet.getString(1)).addValue(resultSet.getString(2)).addValue(resultSet.getString(3)).build();
                        return resultSetRow;
                    }
                });
        PCollection<Row> apply = pipeline.apply(rowRead);

        return apply;
    }

    /**
     * row格式转换HCatSchema
     * @return
     */
    public static HCatSchema getRowAndHCatSchema(Schema schema) throws HCatException {
        if(schema != null){
            List<HCatFieldSchema> columns = new ArrayList<>();
            List<Schema.Field> fields = schema.getFields();
            if(fields != null && fields.size()>0){
                Schema.TypeName typeName = null;
                for(Schema.Field field:fields){
                    typeName = field.getType().getTypeName();
                    if(typeName == Schema.TypeName.STRING){
                        columns.add(new HCatFieldSchema(field.getName(), TypeInfoFactory.stringTypeInfo, ""));
                    }else if(typeName == Schema.TypeName.INT16){
                        columns.add(new HCatFieldSchema(field.getName(), TypeInfoFactory.shortTypeInfo, ""));
                    }else if(typeName == Schema.TypeName.INT32){
                        columns.add(new HCatFieldSchema(field.getName(), TypeInfoFactory.intTypeInfo, ""));
                    }else if(typeName == Schema.TypeName.INT64){
                        columns.add(new HCatFieldSchema(field.getName(), TypeInfoFactory.longTypeInfo, ""));
                    }else if(typeName == Schema.TypeName.FLOAT){
                        columns.add(new HCatFieldSchema(field.getName(), TypeInfoFactory.floatTypeInfo, ""));
                    }else if(typeName == Schema.TypeName.DOUBLE){
                        columns.add(new HCatFieldSchema(field.getName(), TypeInfoFactory.doubleTypeInfo, ""));
                    }else if(typeName == Schema.TypeName.BOOLEAN){
                        columns.add(new HCatFieldSchema(field.getName(), TypeInfoFactory.booleanTypeInfo, ""));
                    }else if(typeName == Schema.TypeName.DATETIME){
                        columns.add(new HCatFieldSchema(field.getName(), TypeInfoFactory.timestampTypeInfo, ""));
                    }else if(typeName == Schema.TypeName.BYTES){
                        columns.add(new HCatFieldSchema(field.getName(), TypeInfoFactory.byteTypeInfo, ""));
                    }else if(typeName == Schema.TypeName.DECIMAL){
                        columns.add(new HCatFieldSchema(field.getName(), TypeInfoFactory.decimalTypeInfo, ""));
                    }
                }

                if(columns != null && columns.size()>0){
                    return new HCatSchema(columns);
                }
            }
        }
        return null;
    }


    static class RowAndHCatRecord extends DoFn<Row, HCatRecord> {
        private static final long serialVersionUID = 3987880242259032890L;
        private final Schema type;
        private HCatSchema hCatSchema;
        private List<String> fieldNames;
        private HCatRecord record;
        public RowAndHCatRecord(Schema type) {
            this.type = type;
        }

        @Setup
        public void setUp() throws HCatException {
            hCatSchema = getRowAndHCatSchema(type);
            fieldNames = type.getFieldNames();
        }

        @ProcessElement
        public void processElement(ProcessContext ctx) throws Exception {
            Row element = ctx.element();
            if(fieldNames != null && fieldNames.size()>0){
                record = new DefaultHCatRecord();
                for(String fieldName:fieldNames){
                    record.set(fieldName,hCatSchema,element.getValue(fieldName));
                }
                ctx.output(record);
            }
        }
    }
}
