package com.devgurupk.kafka.connect.transform;


import com.fasterxml.jackson.core.type.TypeReference;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class SnowFlakeRecord {
    public static Map<String,Object> processTheRecord(Object record) {
        Field[] fields = record.getClass().getDeclaredFields();
//If the field is private make the field to accessible true
        fields[0].setAccessible(true);
//Get the field name
        System.out.println(fields[2].getName());
//Get the field value
        try {

            Method setNameMethod = record.getClass().getMethod("getData");
            setNameMethod.setAccessible(true);
            Arrays.stream(record.getClass().getDeclaredMethods()).forEach(method -> {
                System.out.println(method.getName());
            });
            final JsonNode[] arrNode = (JsonNode[]) setNameMethod.invoke(record);
            System.out.println(arrNode[0]);
            //Map<String, Object> result = new ObjectMapper().convertValue(arrNode[0], new TypeReference<Map<String, Object>>(){});


            System.out.println(); // pass arg
            String json = new ObjectMapper().writeValueAsString(arrNode[0]);
            System.out.println(json);
            String json2 = arrNode[0].toPrettyString();
            System.out.println(json);
            return new ObjectMapper().readValue(json2, HashMap.class);
        } catch (Exception e) {
            System.out.println(e.fillInStackTrace());
            throw new RuntimeException(e);
        }

    }
}
