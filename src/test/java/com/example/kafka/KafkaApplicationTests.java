package com.example.kafka;

import com.example.kafka.schemas.Message1;
import com.example.kafka.schemas.Message2;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.example.kafka.KafkaApplicationTests.TOPIC1;
import static com.example.kafka.KafkaApplicationTests.TOPIC2;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@EmbeddedKafka(partitions = 1,
        topics = {TOPIC1, TOPIC2})
@ExtendWith(SpringExtension.class)
class KafkaApplicationTests {

    public static final String TOPIC1 = "topic1";
    public static final String TOPIC2 = "topic2";

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    // Shared among tests
    private static final SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();

    @Test
    void kafkaTest1() throws InterruptedException {

        // Consumer
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("testGroup", "false", embeddedKafka);
        Map<String, Object> deserializerProps = deserializerProps();
        DefaultKafkaConsumerFactory<String, Message1> cf =
                new DefaultKafkaConsumerFactory<>(consumerProps,
                        new StringDeserializer(),
                        new KafkaProtobufDeserializer<>(schemaRegistryClient, deserializerProps));
        ContainerProperties containerProperties = new ContainerProperties(TOPIC1);
        KafkaMessageListenerContainer<String, Message1>
                container =
                new KafkaMessageListenerContainer<>(cf, containerProperties);
        final BlockingQueue<ConsumerRecord<String, Message1>> records = new LinkedBlockingQueue<>();
        container.setupMessageListener((MessageListener<String, Message1>) records::add);
        container.start();
        ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());

        // Producer
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafka);
        producerProps.put(KafkaProtobufSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://localhost");

        ProducerFactory<String, Message1> pf =
                new DefaultKafkaProducerFactory<>(producerProps, new StringSerializer(),
                        new KafkaProtobufSerializer<>(schemaRegistryClient, producerProps));

        var template = new KafkaTemplate<>(pf);

        var message1 = Message1.newBuilder().setAmount(1).setYear(2021).build();
        Message<Message1> msg = MessageBuilder
                .withPayload(message1)
                .setHeader(KafkaHeaders.TOPIC, TOPIC1)
                .build();

        template.send(msg);

        var received = records.poll(10, TimeUnit.SECONDS);

        assertNotNull(received);
        assertThat(received.value()).isInstanceOf(Message1.class);

        container.stop();
    }

    @Test
    void kafkaTest2() throws InterruptedException {

        // Consumer
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("testGroup", "false", embeddedKafka);
        Map<String, Object> deserializerProps = deserializerProps();
        DefaultKafkaConsumerFactory<String, Message2> cf =
                new DefaultKafkaConsumerFactory<>(consumerProps,
                        new StringDeserializer(),
                        new KafkaProtobufDeserializer<>(schemaRegistryClient, deserializerProps));
        ContainerProperties containerProperties = new ContainerProperties(TOPIC2);
        KafkaMessageListenerContainer<String, Message2> container =
                new KafkaMessageListenerContainer<>(cf, containerProperties);
        final BlockingQueue<ConsumerRecord<String, Message2>> records = new LinkedBlockingQueue<>();
        container.setupMessageListener((MessageListener<String, Message2>) records::add);
        container.start();
        ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());

        // Producer
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafka);
        producerProps.put(KafkaProtobufSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://localhost");

        ProducerFactory<String, Message2> pf =
                new DefaultKafkaProducerFactory<>(producerProps, new StringSerializer(),
                        new KafkaProtobufSerializer<>(schemaRegistryClient, producerProps));

        var template = new KafkaTemplate<>(pf);

        var message2 = Message2.newBuilder().setDescription("my description").build();
        Message<Message2> msg = MessageBuilder
                .withPayload(message2)
                .setHeader(KafkaHeaders.TOPIC, TOPIC2)
                .build();
        template.send(msg);

        var received = records.poll(10, TimeUnit.SECONDS);

        assertNotNull(received);
        assertThat(received.value()).isInstanceOf(Message2.class);

        container.stop();
    }

    private Map<String, Object> deserializerProps() {
        Map<String, Object> deserializerProps = new HashMap<>();
        deserializerProps.put(KafkaProtobufDeserializerConfig.DERIVE_TYPE_CONFIG, true);
        deserializerProps.put(KafkaProtobufDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://localhost");
        return deserializerProps;
    }

}
