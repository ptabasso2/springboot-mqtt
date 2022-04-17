package com.datadoghq.pej;


import io.opentracing.Tracer;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.io.*;


@RestController
public class MqttController {

    private static final Logger logger = LoggerFactory.getLogger(MqttController.class);

    @Autowired
    private MessagingService messagingService;

    @Autowired
    private ConfigurableApplicationContext context;

    @RequestMapping("/Mqtt")
    public String mqttcall() throws MqttException, InterruptedException, IOException {

        final String topic = "pejman/topic/event";

        messagingService.subscribe(topic);

        messagingService.publish(topic, "This is a sample message published to topic pejman/topic/event", 0, true);

        logger.info("Publish/subscribe steps in Controller");
        //context.close();

        return "OK\n";
    }

}