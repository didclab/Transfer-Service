package org.onedatashare.transferservice.odstransferservice.config;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class RabbitMQConfig {

    @Value("${ods.rabbitmq.queue}")
    String queueName;

    @Value("${ods.rabbitmq.exchange}")
    String exchange;

    @Value("${ods.rabbitmq.routingkey}")
    String routingKey;

    @Bean
    Queue userQueue() {
        return new Queue(this.queueName, true, false, false);
    }

    @Bean
    public DirectExchange exchange() {
        return new DirectExchange(exchange);
    }

    @Bean
    public Binding binding(DirectExchange exchange, Queue userQueue) {
        return BindingBuilder.bind(userQueue)
                .to(exchange)
                .with(routingKey);
    }
}
