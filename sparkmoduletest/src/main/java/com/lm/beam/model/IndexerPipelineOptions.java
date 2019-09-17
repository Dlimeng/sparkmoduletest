package com.lm.beam.model;

import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.sdk.options.Description;

import java.util.Map;

/**
 * @Author: limeng
 * @Date: 2019/8/29 15:56
 */
public interface IndexerPipelineOptions  extends SparkPipelineOptions {


    @Description("方式import/export")
    String getPattern();
    void setPattern(String pattern);

    @Description("源名称")
    String getSourceName();
    void setSourceName(String sourceName);

    String getInputFile();
    void setInputFile(String inputFile);

    String getMapPar();
    void setMapPar(String map);
}
