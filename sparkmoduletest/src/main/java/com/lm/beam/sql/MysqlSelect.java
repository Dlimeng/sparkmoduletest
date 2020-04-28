package com.lm.beam.sql;

import com.lm.beam.model.ce;
import com.lm.util.BaseUtil;
import com.lm.util.Pinyin;
import com.lm.util.UUIDFactoryUtil;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * @Author: limeng
 * @Date: 2019/8/26 18:41
 */
public class MysqlSelect {



    public static void main(String[] args) throws PropertyVetoException {
        String driverClass="com.mysql.jdbc.Driver";
        PipelineOptions options = PipelineOptionsFactory.create();

        options.setRunner(DirectRunner.class);

        Pipeline pipeline = Pipeline.create(options);
        ComboPooledDataSource cpds = new ComboPooledDataSource();
        cpds.setDriverClass(driverClass);
        cpds.setJdbcUrl("jdbc:mysql://192.168.20.115:3306/test?useSSL=false");
        cpds.setUser("root");
        cpds.setPassword("root");
        Schema type = Schema.builder().addStringField("name").addInt32Field("id").build();


        JdbcIO.Read<Row> rowRead = JdbcIO.<Row>read().
                withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
                        .create(cpds)

                ).withCoder(SchemaCoder.of(type))
                .withQuery("select * from t")
                .withRowMapper(new JdbcIO.RowMapper<Row>() {
                    @Override
                    public Row mapRow(ResultSet resultSet) throws SQLException {

                        //映射
                        Row resultSetRow = null;
                        Schema build = Schema.builder().addStringField("name").addInt32Field("id").build();
                        resultSetRow = Row.withSchema(build).addValue(resultSet.getString(1)).addValue(resultSet.getInt(2)).build();
                        return resultSetRow;
                    }
                });



        pipeline.apply(rowRead);


        pipeline.run().waitUntilFinish();
    }

    @Test
    public void testRead(){

    }
    @Test
    public void testSave2() throws Exception{
        ComboPooledDataSource cpds = new ComboPooledDataSource();
        cpds.setDriverClass("com.mysql.jdbc.Driver");
        cpds.setJdbcUrl("jdbc:mysql://192.168.200.21:3306/dt_chain?useSSL=false");
        cpds.setUser("root");
        cpds.setPassword("root");

        String sql ="select * from dictionary where `type` is not null";
        Connection connection = cpds.getConnection();
        PreparedStatement ps = connection.prepareStatement(sql);
        ResultSet resultSet = ps.executeQuery();

        Map<String,String> map=new HashMap<>();
        while (resultSet.next()){
            String type = resultSet.getString("type");
            String value = resultSet.getString("value");
            String id = resultSet.getString("id");

            map.put(id,type+"_"+value);
        }

        Connection connection2 = cpds.getConnection();
        map.forEach((k,v)->{
            String sql2="update dictionary set `value`= '"+v+"' where id='"+k+"'";
            try {
                PreparedStatement  ps2 = connection2.prepareStatement(sql2);
                ps2.executeUpdate();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });


    }

    @Test
    public void testSave5() throws Exception{
        ComboPooledDataSource cpds = new ComboPooledDataSource();
        cpds.setDriverClass("com.mysql.jdbc.Driver");
        cpds.setJdbcUrl("jdbc:mysql://192.168.200.115:3306/dt_chain?useSSL=false");
        cpds.setUser("root");
        cpds.setPassword("root");

        List<String> list = BaseUtil.readFileContent("D:\\工具\\workspace\\sparkmoduletest\\sparkmoduletest\\src\\main\\resources\\files\\words1");

        String province="";
        String city="";

        Connection connection = cpds.getConnection();
        connection.setAutoCommit(false);

        Date date = new Date();
        String sql="insert into dictionary(id,label,`value`,`type`,description,createDate,sort,parentId) values(?,?,?,?,?,?,?,?)";
        PreparedStatement ps = connection.prepareStatement(sql);
        List<ce> ceList = new ArrayList<>();
        int index1 = 1;
        int index2 = 1;

        String id1 = null;
        String id2 = null;

        Map<String,String> map=new HashMap<>();

        for(String s:list) {
            String[] split = s.split("\t");
            province = split[0].trim();
            if(split.length >1){
                city = split[1];
            }


                id1 = map.get("province"+province);
                if(StringUtils.isBlank(id1)){
                    id1 = UUIDFactoryUtil.getUUIDStr();
                    map.put("province"+province,id1);

                    ceList.add(new ce(id1,province,BaseUtil.stringToMD5("province"+province),"province",province,index1,null));
                    index1++;
                }

                if(StringUtils.isNotBlank(city)){
                    city = city.trim();
                    id2 = map.get("city"+city);
                    if(StringUtils.isBlank(id2)){
                        id2 = UUIDFactoryUtil.getUUIDStr();
                        map.put("city"+city,id2);

                        ceList.add(new ce(id2,city,BaseUtil.stringToMD5("city"+city),"city",city,index2,id1));
                        index2++;
                    }

                }




        }


        for (ce c:ceList){

            ps.setString(1,c.getId());
            ps.setString(2,c.getLabel());
            ps.setString(3,c.getValue());
            ps.setString(4,c.getType());
            ps.setString(5,c.getDescription());
            ps.setDate(6,new java.sql.Date(date.getTime()));
            ps.setInt(7,c.getSort());
            ps.setString(8,c.getParentId());
            ps.addBatch();
        }
        ps.executeBatch();
        connection.commit();

    }

    @Test
    public void testSave3() throws Exception{
        ComboPooledDataSource cpds = new ComboPooledDataSource();
        cpds.setDriverClass("com.mysql.jdbc.Driver");
        cpds.setJdbcUrl("jdbc:mysql://192.168.200.115:3306/dt_chain?useSSL=false");
        cpds.setUser("root");
        cpds.setPassword("root");

        List<String> list = BaseUtil.readFileContent("D:\\工具\\workspace\\sparkmoduletest\\sparkmoduletest\\src\\main\\resources\\files\\words2");

        String business1="";
        String business2="";
        String business3="";
        String business4="";

        Connection connection = cpds.getConnection();
        connection.setAutoCommit(false);

        Date date = new Date();
        String sql="insert into dictionary(id,label,`value`,`type`,description,createDate,sort,parentId) values(?,?,?,?,?,?,?,?)";
        PreparedStatement ps = connection.prepareStatement(sql);
        List<ce> ceList = new ArrayList<>();
        int index1 = 1;
        int index2 = 1;
        int index3 = 1;
        int index4 = 1;
        String tmpbusiness1=null;
        String tmpbusiness2=null;
        String tmpbusiness3=null;

        String id1 = null;
        String id2 = null;
        String id3 = null;
        String id4 = null;
        Map<String,String> map=new HashMap<>();
        for(String s:list){
            String[] split = s.split("\t");
            if(split.length >=4){

                business1 = split[0].trim();
                business2 = split[1].trim();
                business3 = split[2].trim();
                business4 = split[3].trim();


                 id1 = map.get("business1"+business1);
                 if(StringUtils.isBlank(id1)){
                     id1 = UUIDFactoryUtil.getUUIDStr();
                     map.put("business1"+business1,id1);

                     ceList.add(new ce(id1,business1,BaseUtil.stringToMD5("business1"+business1),"business1",business1,index1,null));
                     index1++;
                 }

                 id2 = map.get("business2"+business2);
                if(StringUtils.isBlank(id2)){
                    id2 = UUIDFactoryUtil.getUUIDStr();
                    map.put("business2"+business2,id2);

                    ceList.add(new ce(id2,business2,BaseUtil.stringToMD5("business2"+business2),"business2",business2,index2,id1));
                    index2++;
                }

                id3 = map.get("business3"+business3);
                if(StringUtils.isBlank(id3)){
                    id3 = UUIDFactoryUtil.getUUIDStr();
                    map.put("business3"+business3,id3);

                    ceList.add(new ce(id3,business3,BaseUtil.stringToMD5("business3"+business3),"business3",business3,index3,id2));
                    index3++;
                }


                id4 = map.get("business4"+business4);
                if(StringUtils.isBlank(id4)){
                    id4 = UUIDFactoryUtil.getUUIDStr();
                    map.put("business4"+business4,id4);

                    ceList.add(new ce(id4,business4,BaseUtil.stringToMD5("business4"+business4),"business4",business4,index4,id3));

                    index4++;
                }



            }
        }

        for (ce c:ceList){

            ps.setString(1,c.getId());
            ps.setString(2,c.getLabel());
            ps.setString(3,c.getValue());
            ps.setString(4,c.getType());
            ps.setString(5,c.getDescription());
            ps.setDate(6,new java.sql.Date(date.getTime()));
            ps.setInt(7,c.getSort());
            ps.setString(8,c.getParentId());
            ps.addBatch();
        }
        ps.executeBatch();
        connection.commit();


    }

    /**
     * 集成电路行业
     * 第一级
     * @throws PropertyVetoException
     */
    @Test
    public void testSave6() throws PropertyVetoException, SQLException {
        ComboPooledDataSource cpds = new ComboPooledDataSource();
        cpds.setDriverClass("com.mysql.jdbc.Driver");
        cpds.setJdbcUrl("jdbc:mysql://192.168.200.115:3306/dt_chain?useSSL=false");
        cpds.setUser("root");
        cpds.setPassword("root");

        List<String> list = BaseUtil.readFileContent("D:\\工具\\workspace\\sparkmoduletest\\sparkmoduletest\\src\\main\\resources\\files\\words4");

        String sql="insert into electric_tree(id,label,description,createDate,sort,parentId) values(?,?,?,?,?,?)";

        Connection connection = cpds.getConnection();
        connection.setAutoCommit(false);
        PreparedStatement ps = connection.prepareStatement(sql);

        List<ce> ceList = new ArrayList<>();
        Date date = new Date();

        ce tmp =null;
        int index=1;
        for(String s:list){
            String[] split = s.split("\t");
            String label = split[0];
            String id = split[1];


            tmp = new ce();
            tmp.setId(id);
            tmp.setLabel(label);
            tmp.setSort(index);
            ceList.add(tmp);
            index++;
        }

        for (ce c:ceList){

            ps.setString(1,c.getId());
            ps.setString(2,c.getLabel());
            ps.setString(3,c.getDescription());
            ps.setDate(4,new java.sql.Date(date.getTime()));
            ps.setInt(5,c.getSort());
            ps.setString(6,c.getParentId());
            ps.addBatch();

        }
        ps.executeBatch();
        connection.commit();
    }

    /**
     * 集成电路行业多级
     */
    @Test
    public void testSave7() throws PropertyVetoException, SQLException {
        ComboPooledDataSource cpds = new ComboPooledDataSource();
        cpds.setDriverClass("com.mysql.jdbc.Driver");
        cpds.setJdbcUrl("jdbc:mysql://192.168.200.115:3306/dt_chain?useSSL=false");
        cpds.setUser("root");
        cpds.setPassword("root");

        List<String> list = BaseUtil.readFileContent("D:\\工具\\workspace\\sparkmoduletest\\sparkmoduletest\\src\\main\\resources\\files\\words5");

        String sql="insert into electric_tree(id,label,description,createDate,sort,parentId) values(?,?,?,?,?,?)";

        Connection connection = cpds.getConnection();
        connection.setAutoCommit(false);
        PreparedStatement ps = connection.prepareStatement(sql);

        List<ce> ceList = new ArrayList<>();
        Date date = new Date();

        ce tmp =null;
        int index=1;
        for(String s:list){
            String[] split = s.split("\t");

            String parentId = split[1];
            String label = split[2];
            String id = split[3];

            tmp = new ce();
            tmp.setId(id);
            tmp.setLabel(label);
            tmp.setSort(index);
            tmp.setParentId(parentId);
            ceList.add(tmp);
            index++;
        }

        for (ce c:ceList){

            ps.setString(1,c.getId());
            ps.setString(2,c.getLabel());
            ps.setString(3,c.getDescription());
            ps.setDate(4,new java.sql.Date(date.getTime()));
            ps.setInt(5,c.getSort());
            ps.setString(6,c.getParentId());
            ps.addBatch();

        }
        ps.executeBatch();
        connection.commit();
    }

    @Test
    public void testQuery() throws Exception{
        ComboPooledDataSource cpds = new ComboPooledDataSource();
        cpds.setDriverClass("com.mysql.jdbc.Driver");
        cpds.setJdbcUrl("jdbc:mysql://192.168.200.116:3306/linkis?useSSL=false");
        cpds.setUser("root");
        cpds.setPassword("know@321");

        Connection connection = cpds.getConnection();
        String sql="select id from cron_job ";
        PreparedStatement statement = connection.prepareStatement(sql);

        ResultSet set = statement.executeQuery();
        String id="";
        while(set.next()){
            id = set.getString("id");
        }


        set.close();
        statement.close();
        connection.close();

    }

    //修改语句
    public static void update(String id,String parentId) throws Exception {
        ComboPooledDataSource cpds = new ComboPooledDataSource();
        cpds.setDriverClass("com.mysql.jdbc.Driver");
        cpds.setJdbcUrl("jdbc:mysql://192.168.200.115:3306/dt_chain?useSSL=false");
        cpds.setUser("root");
        cpds.setPassword("root");
        Connection connection =null;
        PreparedStatement statement =null;
        try {

            connection = cpds.getConnection();
            String sql = "update dictionary set parentId = ? where id = ?";
            statement = connection.prepareStatement(sql);
            statement.setString(1, parentId);
            statement.setString(2, id);
            int result =statement.executeUpdate();// 返回值代表收到影响的行数
            if(result>0) {
                System.out.println("修改成功");
            }else {
                System.out.println("修改失败");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {

            statement.close();
            connection.close();
        }
    }


    public void testSave4() throws Exception{
        ComboPooledDataSource cpds = new ComboPooledDataSource();
        cpds.setDriverClass("com.mysql.jdbc.Driver");
        cpds.setJdbcUrl("jdbc:mysql://192.168.200.115:3306/dt_chain?useSSL=false");
        cpds.setUser("root");
        cpds.setPassword("root");



    }

    @Test
    public void testSave() throws Exception{
        ComboPooledDataSource cpds = new ComboPooledDataSource();
        cpds.setDriverClass("com.mysql.jdbc.Driver");
        cpds.setJdbcUrl("jdbc:mysql://192.168.200.115:3306/dt_chain?useSSL=false");
        cpds.setUser("root");
        cpds.setPassword("root");

        String sql="insert into dictionary(id,label,`value`,`type`,description,createDate,sort) values(?,?,?,?,?,?,?)";
        Connection connection = cpds.getConnection();
        connection.setAutoCommit(false);
        PreparedStatement ps = connection.prepareStatement(sql);
        Date date = new Date();

        List<ce> ceList = new ArrayList<>();

        List<String> list = BaseUtil.readFileContent("D:\\工具\\workspace\\sparkmoduletest\\sparkmoduletest\\src\\main\\resources\\files\\words3");
        int index = 1;
        for(String s:list){
            ceList.add(new ce(s,BaseUtil.stringToMD5(s),"reg_capital_distributed",s,index));
            index ++;
        }
        //        ceList.add(new ce("100万以下",UUIDFactoryUtil.getUUIDStr(),"investment","100万以下"));
//        ceList.add(new ce("100万到1000万",UUIDFactoryUtil.getUUIDStr(),"investment","100万到1000万"));
//        ceList.add(new ce("1000万到3000万",UUIDFactoryUtil.getUUIDStr(),"investment","1000万到3000万"));
//        ceList.add(new ce("3000万到1亿",UUIDFactoryUtil.getUUIDStr(),"investment","3000万到1亿"));
//        ceList.add(new ce("1亿到5亿",UUIDFactoryUtil.getUUIDStr(),"investment","1亿到5亿"));
//        ceList.add(new ce("5亿以上",UUIDFactoryUtil.getUUIDStr(),"investment","5亿以上"));

      //  ceList.add(new ce("石油天然气勘探与生产",UUIDFactoryUtil.getUUIDStr(),"business4","石油天然气勘探与生产"));

//        ceList.add(new ce("中基协备案","1","record","中基协备案"));
//        ceList.add(new ce("非中基协备案","-1","record","非中基协备案"));
//        ceList.add(new ce("人民币","rmb","currency","人民币"));
//
//        ceList.add(new ce("1亿以上","1over","investment","1亿以上"));
//        ceList.add(new ce("1000万到1亿","m_hm","investment","1000万到1亿"));
//        ceList.add(new ce("1000万以下","mbelow","investment","1000万以下"));
//        ceList.add(new ce("其他","other","investment","其他"));
//
//
//
//        ceList.add(new ce("股权投资基金","equityinvestmen","anature","股权投资基金"));
//        ceList.add(new ce("中央国有企业","centralenterprise","anature","中央国有企业"));
//        ceList.add(new ce("地方国有企业","localenterprise","anature","地方国有企业"));
//        ceList.add(new ce("民营企业","privateenterprise","anature","民营企业"));
//        ceList.add(new ce("对冲基金","hedgefund","anature","对冲基金"));
//        ceList.add(new ce("券商资产管理","brokerageassets","anature","券商资产管理"));
//        ceList.add(new ce("担保公司","guaranteecompany","anature","担保公司"));
//        ceList.add(new ce("中外合资企业","jointventures","anature","中外合资企业"));
//        ceList.add(new ce("外商独资企业","whollyforeignowned","anature","外商独资企业"));
//        ceList.add(new ce("外资银行","foreignbank","anature","外资银行"));
//        ceList.add(new ce("集体企业","collectiveenterprises","anature","集体企业"));
//        ceList.add(new ce("基金待定资产管理公司","fundpendingassets","anature","基金待定资产管理公司"));
//        ceList.add(new ce("公众企业","publicenterprise","anature","公众企业"));
//        ceList.add(new ce("其他企业","othercompanies","anature","其他企业"));
//
//
//        String[] ss=new String[]{"北京","天津","上海","重庆","河北","山西","辽宁","吉林","黑龙江","江苏","浙江","安徽"
//        ,"福建","江西","山东","河南","湖北","湖南","广东","海南","四川","贵州","云南","陕西","甘肃","青海","台湾","内蒙古","广西","西藏","宁夏","新疆","香港","澳门"};
//        for (String s:ss) {
//            String pinYin = Pinyin.getCnASCII(s);
//            ceList.add(new ce(s,pinYin,"province",s));
//        }
//
//
//        String[] s2=new String[]{"Pre-Angel","Angel","Pre-A","A","A+","Pre-B","B","B+","C","D","E","F","G","H","PIPE","Strategy","A++","B++","C++","D+","E+"};
//        for (String s:s2) {
//            ceList.add(new ce(s,s,"financing_round",s));
//        }
//
//        ceList.add(new ce("100万以下","below_million","exit_amount","100万以下"));
//        ceList.add(new ce("100万到1000万","m_10million","exit_amount","100万到1000万"));
//        ceList.add(new ce("1000万到3000万","m_30million","exit_amount","1000万到3000万"));
//        ceList.add(new ce("3000万到1亿","30m_hm","exit_amount","3000万到1亿"));
//        ceList.add(new ce("1亿到5亿","hm_5","exit_amount","1亿到5亿"));
//        ceList.add(new ce("5亿以上","5hmover","exit_amount","5亿以上"));
//
//        ceList.add(new ce("IPO","ipo","exit_way","IPO"));
//        ceList.add(new ce("M&A","ma","exit_way","M&A"));
//        ceList.add(new ce("股权转让","equitytransfer","exit_way","股权转让"));
//        ceList.add(new ce("清算","liquidation","exit_way","清算"));
//        ceList.add(new ce("MBO","mbo","exit_way","MBO"));




        for (ce c:ceList){

            ps.setString(1,UUIDFactoryUtil.getUUIDStr());
            ps.setString(2,c.getLabel());
            ps.setString(3,c.getValue());
            ps.setString(4,c.getType());
            ps.setString(5,c.getDescription());
            ps.setDate(6,new java.sql.Date(date.getTime()));
            ps.setInt(7,c.getSort());
            ps.addBatch();
        }
        ps.executeBatch();
        connection.commit();

    }

    @Test
    public void save(){
        PipelineOptions options = PipelineOptionsFactory.create();

        options.setRunner(DirectRunner.class);

        Pipeline pipeline = Pipeline.create(options);

        List<KV<String, String>> kvs = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            String id="id:"+i;
            String name="name:"+i;
            kvs.add(KV.of(id,name));
        }

//        pipeline.apply(Create.of(kvs)).setCoder(KvCoder.of(StringUtf8Coder.of(),StringUtf8Coder.of()))
//                .apply(Filter.by( (KV<String, String> kv) ->  kv.getKey() == "id:1"))
//                .apply(JdbcIO.<KV<String, String>>write().withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create("数据连接池")).withStatement("sql")
//                        .withPreparedStatementSetter((element, statement) -> {
//                            statement.setString(1,element.getKey());
//                            statement.setString(2,element.getValue());
//                        }));

        pipeline.run().waitUntilFinish();

    }


    @Test
    public void testReadByDb() throws Exception {
        ComboPooledDataSource cpds = new ComboPooledDataSource();
        cpds.setDriverClass("com.mysql.jdbc.Driver");
        cpds.setJdbcUrl("jdbc:mysql://192.168.20.115:3306/kd_swap?useSSL=false");
        cpds.setUser("root");
        cpds.setPassword("root");


        Connection connection = cpds.getConnection();
        PreparedStatement ps = connection.prepareStatement("show databases");
        ResultSet resultSet = ps.executeQuery();

        while (resultSet.next()){
            String database = resultSet.getString("Database");
            System.out.println("database:"+database);
        }
        resultSet.close();
        ps.close();
        connection.close();

    }


    @Test
    public void testsaveByDb() throws Exception {
        ComboPooledDataSource cpds = new ComboPooledDataSource();
        cpds.setDriverClass("com.mysql.jdbc.Driver");
        cpds.setJdbcUrl("jdbc:mysql://192.168.200.115:3306/kd_test?useSSL=false");
        cpds.setUser("root");
        cpds.setPassword("root");

        String sql="insert into test2(id,`name`,version) values(?,?,?)";

        Connection connection = cpds.getConnection();
        connection.setAutoCommit(false);
        PreparedStatement ps = connection.prepareStatement(sql);
        for (int i = 0; i < 10000; i++) {

            ps.setString(1,"id"+i);
            ps.setString(2,"name"+i);
            ps.setString(3,"version"+i);
            ps.addBatch();
            if(i!=0 && i % 1000 == 0){
                ps.executeBatch();
                connection.commit();
                ps.clearBatch();
            }
        }
        ps.executeBatch();
        connection.commit();
        ps.close();
        connection.close();

    }

    @Test
    public void test1(){
        int a=2000;
        System.out.println(a&(1000-1));
        System.out.println(3000&(1000-1));
        System.out.println(4000&(1000-1));
        System.out.println(100&(1000-1));
        System.out.println(99&(1000-1));

    }


}
