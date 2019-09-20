package com.lm.beam.neo4j;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.Duration;
import org.neo4j.driver.v1.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static org.neo4j.driver.v1.Values.parameters;

/**
 * @Author: limeng
 * @Date: 2019/9/19 19:16
 */
public class Neo4jIO {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcIO.class);

    private static final long DEFAULT_BATCH_SIZE = 1000L;
    private static final int DEFAULT_FETCH_SIZE = 50_000;

    private Neo4jIO() {}

    public static <T>Write<T> write(){
        return new AutoValue_Neo4jIO_Write.Builder<T>()
                .setBatchSize(DEFAULT_BATCH_SIZE)
                .build();
    }

    @AutoValue
    public abstract static class DriverConfiguration implements Serializable{
        @Nullable
        abstract  Driver getDriver();

        @AutoValue.Builder
        abstract static class Builder{
            abstract  Builder setDriver(Driver driver);
            abstract DriverConfiguration build();
        }


        public static DriverConfiguration create(Driver driver){
            return null;
        }
    }


    @AutoValue
    public abstract static class Write<T> extends PTransform<PCollection<T>, PDone> {
        @Nullable
        abstract Driver getDriver();
        @Nullable
        abstract String getStatement();

        abstract long getBatchSize();

        abstract Builder<T> toBuilder();

        @AutoValue.Builder
        abstract static class Builder<T>{
            abstract Builder<T> setDriver(Driver driver);
            abstract Builder<T> setStatement(String statement);
            abstract Builder<T> setBatchSize(long batchSize);
            abstract Write<T> build();
        }

        public Write<T> withDriver(Driver driver){
            return toBuilder().setDriver(driver).build();
        }

        public Write<T> withStatement(String statement){
            return toBuilder().setStatement(statement).build();
        }
        public Write<T> withBatchSize(long batchSize){
            //checkArgument(batchSize > 0, "batchSize must be > 0, but was %s", batchSize);
            return toBuilder().setBatchSize(batchSize).build();
        }

        @Override
        public PDone expand(PCollection<T> input) {
            input.apply(ParDo.of(new WriteFn<T>(this)));
            return PDone.in(input.getPipeline());
        }
    }


    private static class WriteFn<T> extends DoFn<T,Void>{
        private final Write spec;
        private Driver driver;
        private Session session;
        private List<T> records = new ArrayList<>();
        private static final int MAX_RETRIES = 5;
        private static final FluentBackoff BUNDLE_WRITE_BACKOFF =
                FluentBackoff.DEFAULT
                        .withMaxRetries(MAX_RETRIES)
                        .withInitialBackoff(Duration.standardSeconds(5));

        public WriteFn(Write spec) {
            this.spec = spec;
        }

        @Setup
        public void setup()  throws Exception {
            driver = spec.getDriver();
        }

        @StartBundle
        public void startBundle() throws Exception {
            session = driver.session();
        }

        @ProcessElement
        public void processElement(ProcessContext context) throws Exception {
            T element = context.element();
            records.add(element);
            if(records.size() >= spec.getBatchSize() ){
                executeBatch();
            }
        }

        @FinishBundle
        public void finishBundle() throws Exception {
            executeBatch();
            try {
                if(session != null){
                    session.close();
                }
            }catch (Exception e){
                LOG.warn(e.getMessage());
            }

        }

        private void executeBatch() throws IOException, InterruptedException {
            if (records.isEmpty()) {
                return;
            }
            Sleeper sleeper = Sleeper.DEFAULT;
            BackOff backoff = BUNDLE_WRITE_BACKOFF.backoff();
            while (true){
                try (Transaction tx = session.beginTransaction()){
                    try {
                        for(T record:records){
                            Value value = processRecord(record);
                            session.run(spec.getStatement(),value);
                        }
                        tx.success();
                        break;
                    }catch (Exception exception){
                        LOG.warn("Deadlock detected, retrying", exception);
                        tx.failure();
                        if (!BackOffUtils.next(sleeper, backoff)) {
                            // we tried the max number of times
                            throw exception;
                        }
                    }
                }
            }

            records.clear();
        }

        private Value processRecord(T record) {
            try {
                return parameters(record);
            }catch (Exception e){
                throw new RuntimeException(e);
            }
        }


        @Teardown
        public void teardown() throws Exception {
            if (driver instanceof AutoCloseable) {
                ((AutoCloseable) driver).close();
            }
        }

    }
}
