package com.lm.beam.sql.model;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * @Author: limeng
 * @Date: 2019/7/23 21:01
 */
public interface IndexerPipelineOptions extends PipelineOptions {
    @Description("input file")
    String getInputFile();
    void setInputFile(String in);
}
