package com.lm.beam.sql.model;

import org.apache.beam.sdk.io.hdfs.HadoopFileSystemOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

/**
 * @Author: limeng
 * @Date: 2019/7/23 20:59
 */
public interface WordCountOptions extends HadoopFileSystemOptions {
    @Description("input file")
    String getInputFile();
    void setInputFile(String in);

    @Description("output")
    @Default.String("hdfs://localhost:9000/tmp/hdfsWordCount")
    String getOutput();
    void setOutput(String out);
}
