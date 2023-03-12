package org.onedatashare.transferservice.odstransferservice.config;

import com.google.gson.*;
import org.springframework.amqp.core.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Date;
import java.util.Locale;

@Configuration
public class RabbitMQConfig {

    @Value("${ods.rabbitmq.queue}")
    String queueName;

    @Value("${ods.rabbitmq.exchange}")
    String exchange;

    @Value("${ods.rabbitmq.routingkey}")
    String routingKey;

    @Value("${ods.rabbitmq.dead-letter-routing-key}")
    private String deadLetterRoutingKey;

    @Value("${ods.rabbitmq.dead-letter-queue}")
    private String deadLetterQueueName;

    @Bean
    public Gson gson() {
        GsonBuilder builder = new GsonBuilder()
                .registerTypeAdapter(Date.class, (JsonDeserializer<Date>) (json, typeOfT, context) -> new Date(json.getAsJsonPrimitive().getAsLong()));
        return builder.create();
    }

    @Bean
    Queue userQueue(){
        //String name, boolean durable, boolean exclusive, boolean autoDelete
        return new Queue(this.queueName, true, false, false);
    }

    @Bean
    public Queue deadLetterQueue() {
        return new Queue(this.deadLetterQueueName, true,false, false);
    }


    @Bean
    public DirectExchange exchange(){
        return new DirectExchange(exchange);
    }

    @Bean
    public Binding binding(DirectExchange exchange, Queue userQueue){
        return BindingBuilder.bind(userQueue)
                .to(exchange)
                .with(routingKey);
    }


    @Bean
    public Binding deadLetterBinding(DirectExchange exchange, Queue deadLetterQueue){
        return BindingBuilder.bind(deadLetterQueue)
                .to(exchange)
                .with(deadLetterRoutingKey);
    }
}
