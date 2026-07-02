package com.github.mathlazaro.converter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import com.github.mathlazaro.model.Investments;
import io.nats.client.Message;
import io.synadia.flink.message.SourceConverter;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class InvestmentsSourceConverter implements SourceConverter<Investments> {

    private final ObjectMapper mapper = new ObjectMapper().registerModule(new ParameterNamesModule());

    @Override
    public Investments convert(Message message) {

        try {
            return mapper.readValue(message.getData(), Investments.class);
        } catch (IOException e) {
            throw new RuntimeException("Error converting message to Event", e);
        }

    }

    @Override
    public TypeInformation<Investments> getProducedType() {

        return TypeInformation.of(Investments.class);
    }
}
