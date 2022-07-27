package org.onedatashare.transferservice.odstransferservice.config;

import com.google.gson.*;
import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.Queue;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Date;

@Configuration
public class RabbitMQConfig {

    @Value("${ods.rabbitmq.queue}")
    String queueName;

    @Bean
    public Gson gson() {
        GsonBuilder builder = new GsonBuilder()
                .registerTypeAdapter(Date.class, new JsonDeserializer<Date>() {
                    @Override
                    public Date deserialize(JsonElement json, java.lang.reflect.Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
                        return new Date(json.getAsJsonPrimitive().getAsLong());
                    }
                });
        return builder.create();
    }

    @Bean
    Queue userQueue(){
        //String name, boolean durable, boolean exclusive, boolean autoDelete
        return new Queue(this.queueName, true, false, true);
    }
}
