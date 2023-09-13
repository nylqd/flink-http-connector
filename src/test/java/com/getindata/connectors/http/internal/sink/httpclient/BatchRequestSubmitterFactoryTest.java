package com.getindata.connectors.http.internal.sink.httpclient;

import com.getindata.connectors.http.internal.config.ConfigException;
import com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BatchRequestSubmitterFactoryTest {

    @ParameterizedTest
    @ValueSource(ints = {0, -1})
    public void shouldThrowIfInvalidDefaultSize(int invalidArgument) {
        assertThrows(
            IllegalArgumentException.class,
            () -> new BatchRequestSubmitterFactory(invalidArgument)
        );
    }

    @Test
    public void shouldCreateSubmitterWithDefaultBatchSize() {

        int defaultBatchSize = 10;
        BatchRequestSubmitter submitter = new BatchRequestSubmitterFactory(defaultBatchSize)
            .createSubmitter(new Properties(), new HashMap<>());

        assertThat(submitter.getBatchSize()).isEqualTo(defaultBatchSize);
    }

    @ParameterizedTest
    @ValueSource(strings = {"1", "2"})
    public void shouldCreateSubmitterWithCustomBatchSize(String batchSize) {

        Properties properties = new Properties();
        properties.setProperty(
            HttpConnectorConfigConstants.SINK_HTTP_BATCH_REQUEST_SIZE,
            batchSize
        );

        BatchRequestSubmitter submitter = new BatchRequestSubmitterFactory(10)
            .createSubmitter(properties, new HashMap<>());

        assertThat(submitter.getBatchSize()).isEqualTo(Integer.valueOf(batchSize));
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "-1"})
    public void shouldThrowIfBatchSizeToSmall(String invalidBatchSize) {

        Properties properties = new Properties();
        properties.setProperty(
            HttpConnectorConfigConstants.SINK_HTTP_BATCH_REQUEST_SIZE,
            invalidBatchSize
        );

        BatchRequestSubmitterFactory factory = new BatchRequestSubmitterFactory(10);

        assertThrows(
            ConfigException.class,
            () -> factory.createSubmitter(properties, new HashMap<>())
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {"1.1", "2,2", "hello"})
    public void shouldThrowIfInvalidBatchSize(String invalidBatchSize) {

        Properties properties = new Properties();
        properties.setProperty(
            HttpConnectorConfigConstants.SINK_HTTP_BATCH_REQUEST_SIZE,
            invalidBatchSize
        );

        BatchRequestSubmitterFactory factory = new BatchRequestSubmitterFactory(10);

        assertThrows(
            ConfigException.class,
            () -> factory.createSubmitter(properties, new HashMap<>())
        );
    }
}
