package com.lm.beam.neo4j;

import org.apache.beam.repackaged.beam_sdks_java_core.org.apache.commons.lang3.builder.ToStringExclude;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Assert;
import org.junit.Test;
import org.neo4j.driver.v1.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.neo4j.driver.v1.Values.parameters;

/**
 * @Author: limeng
 * @Date: 2019/9/19 15:09
 */
public class Neo4jTest implements Serializable {
    private Driver createDrive(){

        return GraphDatabase.driver( "bolt://localhost:7687", AuthTokens.basic( "neo4j", "limeng" ));
    }

    @Test
    public void testWrite(){
        try{
            Driver driver = createDrive();
            Session session = driver.session();
            Transaction transaction = session.beginTransaction();
            CompletionStage<Transaction> transactionCompletionStage = session.beginTransactionAsync();

            session.beginTransactionAsync().thenCompose(tx ->
                tx.runAsync("CREATE (a:Person {name: {x}})", parameters("x", "Alice"))
                .exceptionally(e -> {
                e.printStackTrace();
            return null;
            })
              .thenApply(ignore -> tx)
             ).thenCompose(Transaction::commitAsync);

            session.run( "CREATE (a:Person {name: {name}, title: {title}})",
                    parameters( "name", "Arthur001", "title", "King001" ) );

            Value parameters = parameters("name", "Arthur001");

            StatementResult result = session.run( "MATCH (a:Person) WHERE a.name = {name} " +
                            "RETURN a.name AS name, a.title AS title",
                    parameters( "name", "Arthur001" ) );

            while ( result.hasNext() )
            {
                Record record = result.next();
                System.out.println( record.get( "title" ).asString() + " " + record.get( "name" ).asString() + " " + record.get( "id" ).asString() );
            }

            session.close();
            driver.close();

        }catch(Exception e){
           e.printStackTrace();
        }
    }

    @Test
    public void testWrite3(){
        Driver driver = createDrive();
        Session session = driver.session();
        //Transaction transaction = session.beginTransaction();



        try (Transaction tx = session.beginTransaction()){
            tx.run( "CREATE (a:Person {name: {name}, title: {title}})",
                    parameters( "name", "Arthur001", "title", "King001" ) );
            tx.success();
        }


        session.close();
        driver.close();
    }

    @Test
    public void testSave(){
        try {
            Driver drive = createDrive();
            Session session = drive.session();
            Code code = new Code();
            code.setLabel("l1");
            code.setProperty("name:'n1',type:'t2'");
            //String sql="CREATE (a:\" + code.getLabel() + \" {\" + code.getProperty() + \"}) return a";
            StatementResult result  = session.run("CREATE (a:" + code.getLabel() + " {" + code.getProperty() + "}) return a");
            while (result.hasNext()){
                Record record = result.next();

                String replace = record.fields().get(0).value().toString().replace("node<", "").replace(">", "");
                System.out.println(replace);
            }

            session.close();
            drive.close();

        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Test
    public void testRelate(){
        try {
            Driver drive = createDrive();
            Session session = drive.session();
            Code code = new Code();

//            session.run("MATCH (a:" + code.getNodeFromLabel() + "), (b:" + code.getNodeToLabel() + ") " +
//                    "WHERE ID(a) = " + code.getNodeFromId() + " AND ID(b) = " + code.getNodeToId()
//                    + " CREATE (a)-[:" + code.getRelation() + "]->(b)");
            //String dsl="MATCH (a:Node),(b:Node) WHERE a.id={aid} AND b.id={bid} CREATE (a)-[r:Investment {weight:{weight},type:{type}}]->(b)";

            String dsl="MATCH (a:Node),(b:Node) WHERE a.id={startid} AND b.id={endid}  CREATE (a)-[r={label} {weight:{weight},isPerson:{isPerson},createDate:{createDate},updateDate:{updateDate},type:{type},title:{title}}] ->(b)";
            Value parameters = parameters("startid", "6F268F84D96FC7E3E0539601A8C062C45", "endid","6F268F84D9E9C7E3E0539601A8C062C45",
                    "label","header", "weight","0","isPerson","P","createDate","2019-05-15","updateDate","9999-12-31","type","Header","title","监事");

            session.run(dsl,parameters);
            session.close();
            drive.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Test
    public void testUpdate(){
        try {
            Driver drive = createDrive();
            Session session = drive.session();
            Code code = new Code();

            StatementResult result = session.run("MATCH (a:" + code.getLabel() + ") WHERE a." + code.getWhere() + " SET a." + code.getUpdate() + " return COUNT(a)");

            session.close();
            drive.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Test
    public void testWrite2(){
        SparkPipelineOptions options = PipelineOptionsFactory.fromArgs("").withValidation().as(SparkPipelineOptions.class);
        options.setRunner(DirectRunner.class);
        Pipeline pipeline = Pipeline.create(options);
        //CREATE (A:Node {name:"limeng2",iscp:"C2",id:"13",invGrtTyp:"None2",regCap:"152" })
        //MATCH (a:Node),(b:Node) WHERE a.id = "12" AND b.id = "13" CREATE (a)-[r:Header {weight:"0",type:"Header",title:"t1",createDate:"2019-01-31"}]->(b)
        String sql="CREATE (a:Person {name: {name}, title: {title}})";
        Neo4jObject neo4jObject1 = new Neo4jObject();
        Map<String,Object> map=new HashMap<>();
        map.put("name","limeng3");
        map.put("title","limeng3");
        neo4jObject1.setParMap(map);

        Neo4jObject neo4jObject2 = new Neo4jObject();
        map=new HashMap<>();
        map.put("name","limeng4");
        map.put("title","limeng4");
        neo4jObject2.setParMap(map);

        List<Neo4jObject> object=new ArrayList<>();
        object.add(neo4jObject1);
        object.add(neo4jObject2);
        Driver drive = createDrive();

      //  Driver driver = GraphDatabaseTest.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "limeng"));

        PCollection<Neo4jObject> np = pipeline.apply(Create.of(object)).setCoder(SerializableCoder.of(Neo4jObject.class));
        String url="bolt://localhost:7687";
        String username="neo4j";
        String password="limeng";

        Neo4jIO.<Neo4jObject>write().withDriverConfiguration(Neo4jIO.DriverConfiguration.create(url,username,password)).withStatement(sql);
        Neo4jIO.<Neo4jObject>write().withDriverConfiguration(Neo4jIO.DriverConfiguration.create(url,username,password)).withStatement(sql);

       // np.apply(Neo4jIO.<Neo4jObject>write().withDriverConfiguration(Neo4jIO.DriverConfiguration.create(url,username,password)).withStatement(sql));

        pipeline.run().waitUntilFinish();
    }

    @Test
    public void testRelate2(){
//        String fromat=":START_ID(Node)\t:END_ID(Node)\tweight\tisPerson\tcreateDate\tupdateDate\ttype\ttitle\n";
//        String rex="\\s|\t|\r|\n";
//        String[] split = fromat.split(rex);
//        Assert.assertNotNull(split);

        String str ="中华人民共和国，简称(中国)。";
        Matcher mat = Pattern.compile("(?<=\\()(\\S+)(?=\\))").matcher(str);//此处是中文输入的（）
        while(mat.find()){
                 System.out.println(mat.group());
        }
    }



}
