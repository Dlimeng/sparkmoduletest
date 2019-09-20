package com.lm.beam.neo4j;

import org.apache.beam.repackaged.beam_sdks_java_core.org.apache.commons.lang3.builder.ToStringExclude;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;
import org.neo4j.driver.v1.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

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

            session.run("MATCH (a:" + code.getNodeFromLabel() + "), (b:" + code.getNodeToLabel() + ") " +
                    "WHERE ID(a) = " + code.getNodeFromId() + " AND ID(b) = " + code.getNodeToId()
                    + " CREATE (a)-[:" + code.getRelation() + "]->(b)");

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

        String sql="CREATE (a:Person {name: {name}, title: {title}})";
        Neo4jObject neo4jObject = new Neo4jObject();
        Object[] o=new Object[]{"name", "Arthur001"};
        neo4jObject.setObjects(o);
        List<Neo4jObject> object=new ArrayList<>();
        object.add(neo4jObject);
        Driver drive = createDrive();

      //  Driver driver = GraphDatabaseTest.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "limeng"));

        PCollection<Neo4jObject> np = pipeline.apply(Create.of(object)).setCoder(SerializableCoder.of(Neo4jObject.class));

        np.apply(Neo4jIO.<Neo4jObject>write().withDriver(drive).withStatement(sql));

        pipeline.run().waitUntilFinish();
    }




}
