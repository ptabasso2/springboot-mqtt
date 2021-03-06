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
import java.nio.ByteBuffer;
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

        /* Extracting ids from mapinject and building the payload byte array to be sent */
        String stid = mapinject.get("x-datadog-trace-id");
        long ltid = Long.valueOf(stid);
        ByteBuffer bbftid = ByteBuffer.allocate(Long.BYTES);
        bbftid.putLong(ltid);
        byte[] btid = bbftid.array();


        String spid = mapinject.get("x-datadog-parent-id");
        long lpid = Long.valueOf(spid);
        ByteBuffer bbfpid = ByteBuffer.allocate(Long.BYTES);
        bbfpid.putLong(lpid);
        byte[] bpid = bbfpid.array();


        String messageSent = "Un autre exemple de messsage";
        byte[] bmsg = messageSent.getBytes();

        /* Building the payload by merging the above arrays */
        byte[] payload = new byte[btid.length + bpid.length + bmsg.length];

        int pos = 0;
        for (byte element : btid) {
            payload[pos] = element;
            pos++;
        }

        for (byte element : bpid) {
            payload[pos] = element;
            pos++;
        }

        for (byte element : bmsg) {
            payload[pos] = element;
            pos++;
        }


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

    }

    public void subscribe(final String topic) throws MqttException, InterruptedException {

        mqttClient.subscribeWithResponse(topic, (tpic, msg) -> {

            /* Extracing the payload */
            byte[] payload = msg.getPayload();

            /* Splitting the payload into byte arrays */
            byte[] traceid = new byte[Long.BYTES];
            byte[] parentid = new byte[Long.BYTES];
            byte[] messageReceived = new byte[payload.length - traceid.length - parentid.length];

            int tidlen = traceid.length;
            int pidlen = parentid.length;

            for (int i = 0; i < tidlen; i++) {
                traceid[i] = payload[i];
            }

            for (int i = tidlen; i < tidlen + pidlen; i++) {
                parentid[i - tidlen] = payload[i];
            }

            for (int i = tidlen + pidlen; i < payload.length; i++) {
                messageReceived[i - tidlen - pidlen] = payload[i];
            }

            /* Extracting the content by generating two longs and the string message */
            ByteBuffer buftid = ByteBuffer.allocate(Long.BYTES);
            buftid.put(traceid);
            buftid.flip();//need flip
            long tid = buftid.getLong();


            ByteBuffer bufpid = ByteBuffer.allocate(Long.BYTES);
            bufpid.put(parentid);
            bufpid.flip();//need flip
            long pid = bufpid.getLong();

            String message = new String(messageReceived);


            /* Building the map to be used with tracer.extract() */
            Map<String, String> mapextract = new HashMap<>();

            mapextract.put("x-datadog-trace-id", Long.toString(tid));
            mapextract.put("x-datadog-parent-id", Long.toString(pid));
            mapextract.put("x-datadog-sampling-priority", "1");

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
                logger.info("Message received: " + message);

            } finally {
                childspan.finish();
            }
        });

    }

}
