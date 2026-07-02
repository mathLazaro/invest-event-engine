package com.github.mathlazaro.converter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import com.github.mathlazaro.model.Sector;
import com.github.mathlazaro.model.Subscriptions;
import com.github.mathlazaro.model.Ticker;
import io.nats.client.Message;
import io.synadia.flink.message.SourceConverter;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.math.BigDecimal;

public class SubscriptionsSourceConverter implements SourceConverter<Subscriptions> {

    private final ObjectMapper mapper = new ObjectMapper().registerModule(new ParameterNamesModule());

    @Override
    public Subscriptions convert(Message message) {

        try {
            Subscriptions subscriptions = mapper.readValue(message.getData(), Subscriptions.class);

            Ticker ticker = subscriptions.ticker() == null ? Ticker.ANY : subscriptions.ticker();
            Sector sector = subscriptions.sector() == null ? Sector.ANY : subscriptions.sector();
            BigDecimal higherThan = subscriptions.higherThan();
            BigDecimal smallerThan = subscriptions.smallerThan();

            return new Subscriptions(subscriptions.userId(), ticker, sector, higherThan, smallerThan);
        } catch (IOException e) {
            throw new RuntimeException("Error converting message to Event", e);
        }

    }

    @Override
    public TypeInformation<Subscriptions> getProducedType() {

        return TypeInformation.of(Subscriptions.class);
    }
}
