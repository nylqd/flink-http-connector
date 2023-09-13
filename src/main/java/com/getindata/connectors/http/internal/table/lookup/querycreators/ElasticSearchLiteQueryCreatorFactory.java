package com.getindata.connectors.http.internal.table.lookup.querycreators;

import com.getindata.connectors.http.LookupQueryCreator;
import com.getindata.connectors.http.LookupQueryCreatorFactory;
import com.getindata.connectors.http.internal.table.lookup.LookupRow;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;

import java.util.HashSet;
import java.util.Set;

/**
 * Factory for creating {@link ElasticSearchLiteQueryCreator}.
 */
public class ElasticSearchLiteQueryCreatorFactory implements LookupQueryCreatorFactory {

    public static final String IDENTIFIER = "elasticsearch-lite";

    @Override
    public LookupQueryCreator createLookupQueryCreator(
            ReadableConfig readableConfig,
            LookupRow lookupRow) {
        return new ElasticSearchLiteQueryCreator(lookupRow);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>();
    }
}
