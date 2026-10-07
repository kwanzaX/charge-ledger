package dev.kwanza.chargeledger.outbox;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
class RabbitConfig {

    @Bean
    TopicExchange billingExchange(@Value("${charge-ledger.exchange}") String exchange) {
        return new TopicExchange(exchange, true, false);
    }

    /** Queue for the customer-notification service: one message per invoiced session. */
    @Bean
    Queue invoicedNotifications() {
        return QueueBuilder.durable("billing.session-invoiced.notifications").build();
    }

    @Bean
    Binding invoicedBinding(Queue invoicedNotifications, TopicExchange billingExchange) {
        return BindingBuilder.bind(invoicedNotifications).to(billingExchange).with("session.invoiced");
    }
}
