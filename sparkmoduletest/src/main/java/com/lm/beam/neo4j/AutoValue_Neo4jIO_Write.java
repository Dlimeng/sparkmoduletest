package com.lm.beam.neo4j;

import org.neo4j.driver.v1.Driver;

import javax.annotation.Nullable;

/**
 * @Author: limeng
 * @Date: 2019/9/20 17:21
 */
final class AutoValue_Neo4jIO_Write<T> extends Neo4jIO.Write<T> {
    private final  Driver driver;
    private final  long batchSize;
    private final  String statement;

    private AutoValue_Neo4jIO_Write(@Nullable Driver driver,long batchSize, @Nullable String statement) {
        this.driver = driver;
        this.batchSize = batchSize;
        this.statement = statement;
    }


    @Nullable
    @Override
    Driver getDriver() {
        return this.driver;
    }

    @Nullable
    @Override
    String getStatement() {
        return this.statement;
    }

    @Override
    long getBatchSize() {
        return this.batchSize;
    }

    @Override
    Builder toBuilder() {
        return new AutoValue_Neo4jIO_Write.Builder(this);
    }



    @Override
    public boolean equals(Object obj) {
        if(obj == this){
            return true;
        }
        if((obj instanceof Neo4jIO.Write)){
            Neo4jIO.Write<?> that = (Neo4jIO.Write) obj;
            return (this.getDriver() == null ?  that.getDriver() == null : this.getDriver().equals(that.getDriver())) &&
                    (this.getStatement() == null ?  that.getStatement() == null : this.getStatement().equals(that.getStatement())) &&
                        (this.getBatchSize() == that.getBatchSize());

        }
        return false;
    }

    /**
     * 2^6+3 是个素数，发生hash碰撞可能低于偶数
     * @return
     */
    @Override
    public int hashCode() {
        int h = 1;
        h *= 1000003;
        h ^= (this.driver == null ? 0:this.driver.hashCode());
        h *=1000003;
        h ^=(this.statement == null ? 0:this.statement.hashCode());
        h *=1000003;
        h ^=(int)(this.batchSize >>> 32^this.batchSize);
        return h;
    }

    static final class Builder<T> extends Neo4jIO.Write.Builder<T> {
        private Driver driver;
        private String statement;
        private Long batchSize;

        Builder() {
        }

        private Builder(Neo4jIO.Write<T> source) {
            this.driver = source.getDriver();
            this.statement = source.getStatement();
            this.batchSize = source.getBatchSize();
        }

        @Override
        Neo4jIO.Write.Builder<T> setDriver(Driver driver) {
            this.driver = driver;
            return this;
        }

        @Override
        Neo4jIO.Write.Builder<T> setStatement(String statement) {
            this.statement = statement;
            return this;
        }

        @Override
        Neo4jIO.Write.Builder<T> setBatchSize(long batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        @Override
        Neo4jIO.Write<T> build() {
            String missing="";
            if(this.batchSize == null){
                missing = missing+" batchSize";
            }
            if(!missing.isEmpty()){
                throw new IllegalStateException("Missing required properties:" + missing);
            }
            return new AutoValue_Neo4jIO_Write(this.driver,this.batchSize,this.statement);
        }
    }



}
