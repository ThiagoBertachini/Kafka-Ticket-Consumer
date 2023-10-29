package org.beprojects.deserializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.beprojects.model.Sell;

import java.io.IOException;

public class SellDeserializer implements Deserializer<Sell> {

    @Override
    public Sell deserialize(String s, byte[] sellByte) {
        try {
            return new ObjectMapper().readValue(sellByte, Sell.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
