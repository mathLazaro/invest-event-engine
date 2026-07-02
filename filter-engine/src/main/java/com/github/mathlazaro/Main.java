package com.github.mathlazaro;

import com.github.mathlazaro.converter.InvestmentsSourceConverter;
import com.github.mathlazaro.converter.SubscriptionsSourceConverter;
import com.github.mathlazaro.model.Investments;
import com.github.mathlazaro.model.Subscriptions;
import com.github.mathlazaro.processor.FilterEngine;
import com.github.mathlazaro.sink.NatsNotificationSink;
import io.synadia.flink.source.JetStreamSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.UUID;

public class Main {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        JetStreamSource<Investments> investmentsSource = JetStreamFactory.sourceFromJsonConfig("stream_investments.json", new InvestmentsSourceConverter());
        JetStreamSource<Subscriptions> subscriptionsSource = JetStreamFactory.sourceFromJsonConfig("stream_subscriptions.json", new SubscriptionsSourceConverter());

        DataStream<Investments> investments = env.fromSource(
                investmentsSource,
                WatermarkStrategy.noWatermarks(),
                "JS_INVESTMENTS"
        );

        DataStream<Subscriptions> subscriptions = env.fromSource(
                subscriptionsSource,
                WatermarkStrategy.noWatermarks(),
                "JS_SUBSCRIPTIONS"
        );

        BroadcastStream<Subscriptions> broadcast = subscriptions.broadcast(new MapStateDescriptor<>(
                "subscriptions",
                TypeInformation.of(UUID.class),
                TypeInformation.of(Subscriptions.class)
        ));

        investments
                .connect(broadcast)
                .process(new FilterEngine())
                .addSink(new NatsNotificationSink());

        try {
            env.execute("Investments streaming");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
