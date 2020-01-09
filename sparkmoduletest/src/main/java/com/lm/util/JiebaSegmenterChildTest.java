package com.lm.util;

import com.huaban.analysis.jieba.JiebaSegmenter;
import com.huaban.analysis.jieba.SegToken;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * @Classname JiebaSegmenterChildTest
 * @Description TODO
 * @Date 2020/1/9 14:15
 * @Created by limeng
 */
public class JiebaSegmenterChildTest {
    private JiebaSegmenter segmenter = new JiebaSegmenterChild(); // 分词 名词动词形容词

    @Test
    public void init(){
        List<SegToken> lsegStr = segmenter.process("testdata", JiebaSegmenter.SegMode.SEARCH);
        Assert.assertNotNull(lsegStr);
    }
}
