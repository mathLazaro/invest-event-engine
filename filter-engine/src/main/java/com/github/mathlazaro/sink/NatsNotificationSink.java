package com.github.mathlazaro.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.mathlazaro.model.Notification;
import io.nats.client.Connection;
import io.nats.client.Nats;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;


public class NatsNotificationSink extends RichSinkFunction<Notification> {

    private transient Connection connection;
    private transient ObjectMapper mapper;

    @Override
    public void open(Configuration parameters) throws Exception {

        connection = Nats.connect("nats://localhost:4222");
        mapper = new ObjectMapper();
    }

    @Override
    public void invoke(Notification value, Context context) throws Exception {

        String subject = "user." + value.userId() + ".notifications";
        byte[] payload = mapper.writeValueAsBytes(value.investment());
        connection.publish(subject, payload);
    }

    @Override
    public void close() throws Exception {

        if (connection != null) connection.close();
    }
}
