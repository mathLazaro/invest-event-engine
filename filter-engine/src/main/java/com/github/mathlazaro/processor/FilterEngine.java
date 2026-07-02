package com.github.mathlazaro.processor;

import com.github.mathlazaro.model.*;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;
import java.util.Map;

public class FilterEngine extends BroadcastProcessFunction<Investments, Subscriptions, Notification> {

    private static final Logger log = LogManager.getLogger(FilterEngine.class);

    @Override
    public void processBroadcastElement(
            Subscriptions value,
            BroadcastProcessFunction<Investments, Subscriptions, Notification>.Context ctx,
            Collector<Notification> out
    ) throws Exception {

        log.info("Subscription {}", value);
        BroadcastState<String, Subscriptions> state = ctx.getBroadcastState(new MapStateDescriptor<>(
                "subscriptions",
                TypeInformation.of(String.class),
                TypeInformation.of(Subscriptions.class)
        ));
        state.put(value.userId(), value);
    }

    @Override
    public void processElement(
            Investments investment,
            BroadcastProcessFunction<Investments, Subscriptions, Notification>.ReadOnlyContext ctx,
            Collector<Notification> out
    ) throws Exception {

        log.info("Investment {}", investment);

        ReadOnlyBroadcastState<String, Subscriptions> state = ctx.getBroadcastState(new MapStateDescriptor<>(
                "subscriptions",
                TypeInformation.of(String.class),
                TypeInformation.of(Subscriptions.class)
        ));

        for (Map.Entry<String, Subscriptions> entry : state.immutableEntries()) {
            Subscriptions sub = entry.getValue();
            BigDecimal minimalPrice = sub.higherThan();
            BigDecimal maximalPrice = sub.smallerThan();

            boolean sectorMatches = sub.sector().equals(investment.sector()) || sub.sector().equals(Sector.ANY);
            boolean tickerMatches = sub.ticker().equals(investment.ticker()) || sub.ticker().equals(Ticker.ANY);
            boolean minPriceMatches = minimalPrice == null || investment.price().compareTo(minimalPrice) >= 0;
            boolean maxPriceMatches = maximalPrice == null || investment.price().compareTo(maximalPrice) <= 0;

            if (sectorMatches && tickerMatches && minPriceMatches && maxPriceMatches) {
                log.info("Sub matches found");
                out.collect(new Notification(sub.userId(), investment));
            }

        }
    }

}
