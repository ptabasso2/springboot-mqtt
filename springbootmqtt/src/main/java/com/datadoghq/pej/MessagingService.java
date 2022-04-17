package com.datadoghq.pej;

import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import io.opentracing.propagation.TextMapAdapter;
import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttPersistenceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Service
public class MessagingService {

    private static final Logger logger = LoggerFactory.getLogger(MessagingService.class);

    final int MAPSIZE = 203;

    @Autowired
    Tracer tracer;

    @Autowired
    private IMqttClient mqttClient;

    public void publish(final String topic, final String data, int qos, boolean retained)
            throws MqttPersistenceException, MqttException, IOException, InterruptedException {

        /* Filling mapinject with header values */
        Map<String,String> mapinject =new HashMap<>();
        Span span = tracer.buildSpan("Publish").start();
        tracer.inject(span.context(), Format.Builtin.TEXT_MAP, new TextMapAdapter(mapinject));

        /* Serializing mapinject and converting the HashMap to a byte array */
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(byteOut);
        out.writeObject(mapinject);
        byte[] map = byteOut.toByteArray();

        /* Serializing string message to a byte array */
        byte[] message = data.getBytes();

        /* JOin both byte arrays (map and message) to form a payload byte array */
        ByteArrayOutputStream outInternal = new ByteArrayOutputStream();
        outInternal.write(map);
        outInternal.write(message);
        byte[] payload = outInternal.toByteArray();

        /* Wrapping the payload in the MqttMessage object */
        MqttMessage mqttMessage = new MqttMessage();
        mqttMessage.setPayload(payload);
        mqttMessage.setQos(qos);
        mqttMessage.setRetained(retained);

        /* Building the parent span for the publish operation */
        try (Scope scope = tracer.activateSpan(span)) {
            span.setTag("service.name", "Upstream");
            span.setTag("span.type", "custom");
            span.setTag("resource.name", "mqtt.publish");
            span.setTag("resource", "mqtt.publish");
            span.setTag("customer_id", "45678");

            mqttClient.publish(topic, mqttMessage);

            Thread.sleep(2000L);
        } finally {
            span.finish();
        }

        //mqttClient.publish(topic, payload.getBytes(), qos, retained);

        //mqttClient.disconnect();
    }

    public void subscribe(final String topic) throws MqttException, InterruptedException {

        mqttClient.subscribeWithResponse(topic, (tpic, msg) -> {

            /* Splitting the payload and extracting the first 203 bytes (fixed length) representing the serialized map */
            byte[] serializedmap = Arrays.copyOfRange(msg.getPayload(), 0, MAPSIZE);

            /* Extracting the next 4 bytes (variable length) which are representing the string message */
            byte[] stringmessage = Arrays.copyOfRange(msg.getPayload(), MAPSIZE, msg.getPayload().length);

            /* Deserializing the map */
            ByteArrayInputStream bIn = new ByteArrayInputStream(serializedmap);
            ObjectInputStream in = new ObjectInputStream(bIn);
            Map<String, String> mapextract = (Map<String, String>) in.readObject();

            /* Deserializing the string */
            String messageReceived = new String(stringmessage);

            /* Retrieving the context of the parent span after */
            SpanContext parentSpan = tracer.extract(Format.Builtin.TEXT_MAP, new TextMapAdapter(mapextract));

            /* Building the child span and nesting it under the parentspan */
            Span childspan = tracer.buildSpan("Subscribe").asChildOf(parentSpan).start();
            try (Scope scope = tracer.activateSpan(childspan)) {
                childspan.setTag("service.name", "Downstream");
                childspan.setTag("span.type", "custom");
                childspan.setTag("resource.name", "mqtt.subscribe");
                childspan.setTag("resource", "mqtt.subscribe");
                childspan.setTag("customer_id", "45678");
                Thread.sleep(2000L);
                logger.info("Message received: " + messageReceived);

            } finally {
                childspan.finish();
            }
        });

    }

}
