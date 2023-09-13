package com.getindata.connectors.http.internal.table.sink;

import com.getindata.connectors.http.internal.table.sink.HttpDynamicSink.HttpDynamicTableSinkBuilder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.TestFormatFactory;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.logical.BooleanType;
import org.junit.jupiter.api.Test;

import static com.getindata.connectors.http.internal.table.sink.HttpDynamicSinkConnectorOptions.INSERT_METHOD;
import static com.getindata.connectors.http.internal.table.sink.HttpDynamicSinkConnectorOptions.URL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class HttpDynamicSinkTest {

    @Test
    public void testAsSummaryString() {
        TestFormatFactory.EncodingFormatMock mockFormat = new TestFormatFactory.EncodingFormatMock(",", ChangelogMode.insertOnly());

        HttpDynamicSink dynamicSink = new HttpDynamicTableSinkBuilder()
            .setTableOptions(new Configuration())
            .setConsumedDataType(
                new AtomicDataType(new BooleanType(false)))
            .setEncodingFormat(mockFormat)
            .setHttpPostRequestCallback(new Slf4jHttpPostRequestCallback())
            .build();

        assertThat(dynamicSink.asSummaryString()).isEqualTo("HttpSink");
    }

    @Test
    public void copyEqualityTest() {
        TestFormatFactory.EncodingFormatMock mockFormat = new TestFormatFactory.EncodingFormatMock(",", ChangelogMode.insertOnly());
        HttpDynamicSink sink = new HttpDynamicSink
            .HttpDynamicTableSinkBuilder()
            .setTableOptions(
                new Configuration() {
                    {
                        this.set(URL, "localhost:8123");
                        this.set(INSERT_METHOD, "POST");
                        this.set(FactoryUtil.FORMAT, "json");
                    }
                }
            )
            .setConsumedDataType(
                new AtomicDataType(new BooleanType(false)))
            .setEncodingFormat(mockFormat)
            .setHttpPostRequestCallback(new Slf4jHttpPostRequestCallback())
            .build();

        assertEquals(sink, sink.copy());
        assertEquals(sink.hashCode(), sink.copy().hashCode());
    }

    private HttpDynamicSink.HttpDynamicTableSinkBuilder getSinkBuilder() {
        TestFormatFactory.EncodingFormatMock mockFormat = new TestFormatFactory.EncodingFormatMock(",", ChangelogMode.insertOnly());
        AtomicDataType consumedDataType = new AtomicDataType(new BooleanType(false));

        return new HttpDynamicSink.HttpDynamicTableSinkBuilder()
            .setTableOptions(
                new Configuration() {
                    {
                        this.set(URL, "localhost:8123");
                        this.set(INSERT_METHOD, "POST");
                        this.set(FactoryUtil.FORMAT, "json");
                    }
                }
            )
            .setConsumedDataType(consumedDataType)
            .setEncodingFormat(mockFormat)
            .setHttpPostRequestCallback(new Slf4jHttpPostRequestCallback())
            .setMaxBatchSize(1);
    }

    @Test
    public void nonEqualsTest() {
        HttpDynamicSink sink = getSinkBuilder().build();
        HttpDynamicSink sinkBatchSize = getSinkBuilder().setMaxBatchSize(10).build();
        HttpDynamicSink sinkSinkConfig = getSinkBuilder().setTableOptions(
            new Configuration() {
                {
                    this.set(URL, "localhost:8124");
                    this.set(INSERT_METHOD, "POST");
                    this.set(FactoryUtil.FORMAT, "json");
                }
            }
        ).build();
        HttpDynamicSink sinkDataType =
            getSinkBuilder().setConsumedDataType(new AtomicDataType(new BooleanType(true))).build();
        HttpDynamicSink sinkFormat = getSinkBuilder().setEncodingFormat(
            new TestFormatFactory.EncodingFormatMock(";", ChangelogMode.all())).build();
        HttpDynamicSink sinkHttpPostRequestCallback =
            getSinkBuilder()
                .setHttpPostRequestCallback(new Slf4jHttpPostRequestCallback()).build();

        assertEquals(sink, sink);
        assertNotEquals(null, sink);
        assertNotEquals("test-string", sink);
        assertNotEquals(sink, sinkBatchSize);
        assertNotEquals(sink, sinkSinkConfig);
        assertNotEquals(sink, sinkDataType);
        assertNotEquals(sink, sinkFormat);
        assertNotEquals(sink, sinkHttpPostRequestCallback);
    }
}
