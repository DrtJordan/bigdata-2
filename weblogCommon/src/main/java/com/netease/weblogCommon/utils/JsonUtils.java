package com.netease.weblogCommon.utils;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.math.NumberUtils;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.type.TypeFactory;


public class JsonUtils {

    private static final ObjectMapper defaultObjectMapper;
    private static final ObjectMapper numAsStringObjectMapper;

    static {
        numAsStringObjectMapper = new ObjectMapper();
        numAsStringObjectMapper.configure(JsonGenerator.Feature.WRITE_NUMBERS_AS_STRINGS, true);
        defaultObjectMapper = new ObjectMapper().configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        // 设置日期转换格式
        defaultObjectMapper.getDeserializationConfig().setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
        // 允许字段名不用引号
        defaultObjectMapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true)
        // 允许使用单引号
                .configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)
                // 允许有不对应的属性
                .configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public static String toFilterNumberString(Object object) {
        return toString(object, numAsStringObjectMapper);
    }

    public static String toString(Object object, ObjectMapper mapper) {
        String json = "";
        try {
            json = mapper.writeValueAsString(object);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return json;
    }

    /**
     * @param object
     * @return
     */
    public static String toJson(Object object) {
        return toString(object, defaultObjectMapper);
    }

    public static long getLong(Object o, long defaltValue) {
        if (o instanceof String) {
            return NumberUtils.toLong((String) o, defaltValue);
        } else if (o instanceof Number) {
            return ((Number) o).longValue();
        }
        return defaltValue;
    }

    /**
     * @param remarks
     * @return
     */
    public static String getStr(String remarks) {
        if (remarks == null || remarks.trim().equalsIgnoreCase("null")) {
            return null;
        }
        return remarks;
    }

    /**
     * json转clazz类型实例
     * 
     * @param <T>
     * @param json
     * @param clazz 实例类型
     * @return clazz实例
     */
    public static <T> T json2Bean(String json, Class<T> clazz) {
        try {
            return (T) defaultObjectMapper.readValue(json, clazz);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * json转Map对象
     * 
     * @param json
     * @return Map对象
     */
    public static Map<String, String> json2Map(String json) {
        try {
            return defaultObjectMapper.readValue(json, TypeFactory.mapType(Map.class, String.class, String.class));
        } catch (Exception e) {
//            e.printStackTrace();
            return null;
        }
    }

    public static Map<String, Object> json2ObjMap(String json) {
        try {
            return defaultObjectMapper.readValue(json, TypeFactory.mapType(Map.class, String.class, Object.class));
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
    
    public static Map<String, String> json2StrMap(String json) {
        Map<String, String> result = new HashMap<String, String>();
        
        try {
            Map<String, Object> tempMap = json2ObjMap(json);
            if(null != tempMap){
                for(Map.Entry<String, Object> entry : tempMap.entrySet()){
                    result.put(entry.getKey(), entry.getValue().toString());
                } 
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        return result;
    }

    /**
     * json转List对象
     * 
     * @param <T>
     * @param json
     * @param clazz 元素类型
     * @return List对象
     */
    public static <T> List<T> json2List(String json, Class<T> clazz) {
        try {
            return defaultObjectMapper.readValue(json, TypeFactory.collectionType(List.class, clazz));
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

    }

    /**
     * json转List对象,元素为Map类型
     * 
     * @param json
     * @return List对象
     */
    public static List<Map<String, String>> json2List(String json) {
        try {
            return defaultObjectMapper.readValue(json,
                    TypeFactory.collectionType(List.class, TypeFactory.mapType(Map.class, String.class, String.class)));
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * json转数组
     * 
     * @param <T>
     * @param json
     * @param clazz 元素类型
     * @return clazz类型数组
     */
    public static <T> T[] json2Array(String json, Class<T> clazz) {
        try {
            return defaultObjectMapper.readValue(json, TypeFactory.arrayType(clazz));
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * json转数组,元素为Map类型
     * 
     * @param json
     * @return Map数组
     */
    public static Map<String, String>[] json2Array(String json) {
        try {
            return defaultObjectMapper.readValue(json,
                    TypeFactory.arrayType(TypeFactory.mapType(Map.class, String.class, String.class)));
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

    }
}

