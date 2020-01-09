package com.lm.util;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * @Classname JsonConverter
 * @Description TODO
 * @Date 2019/12/31 18:15
 * @Created by limeng
 */
public class JsonConverter {
    private static Logger logger = LoggerFactory.getLogger(JsonConverter.class);
    private static ObjectMapper objectMapper;

    static {
        objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public static String write(Object object) throws Exception {
        return objectMapper.writeValueAsString(object);
    }

    public static <T> T read(Class<T> type, InputStream is) throws Exception {
        return objectMapper.readValue(is, objectMapper.constructType(type));
    }

    public static Map<String, String> read(String jsonMap) throws Exception {
        return objectMapper.readValue(jsonMap, objectMapper.constructType(Map.class));
    }

    public static <T> T read(Class<T> type, String json) throws Exception {
        return read(type, new ByteArrayInputStream(json.getBytes(Charsets.UTF_8)));
    }

    public static <T> T read(Class<T> type, byte[] data) throws IOException {
        try {
            return objectMapper.readValue(data, objectMapper.constructType(type));
        } catch (IOException e) {
            logger.error("json read parse error", e);
            throw e;
        }
    }
}
