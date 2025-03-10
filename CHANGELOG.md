# Changelog

## [Unreleased]

## [0.10.0] - 2023-07-05

### Added

-   Add support for batch request submission in HTTP sink. The mode can be changed by setting
    `gid.connector.http.sink.writer.request.mode` with value `single` or `batch`. The default value
    is `batch` bode which is breaking change comparing to previous versions. Additionally,
    `gid.connector.http.sink.request.batch.size` option can be used to set batch size. By default,
    batch size is 500 which is same as default value of HttpSink `maxBatchSize` parameter. 

### Changed

-   Changed API for public HttpSink builder. The `setHttpPostRequestCallback` expects a `PostRequestCallback`
    of generic type [HttpRequest](src/main/java/com/getindata/connectors/http/internal/sink/httpclient/HttpRequest.java)
    instead `HttpSinkRequestEntry`.
-   Changed HTTP sink request and response processing thread pool sizes from 16 to 1.

## [0.9.0] - 2023-02-10

-   Add support for Flink 1.16.
-   Add [SchemaLifecycleAwareElementConverter](src/main/java/com/getindata/connectors/http/SchemaLifecycleAwareElementConverter.java) that can be used for createing
    schema lifecycle aware Element converters for Http Sink.

## [0.8.1] - 2022-12-22

### Fixed

-   Fixed issue with not printing HttpRequest body/parameters for Lookup Source by
    [Slf4JHttpLookupPostRequestCallback](src/main/java/com/getindata/connectors/http/internal/table/lookup/Slf4JHttpLookupPostRequestCallback.java) - <https://github.com/getindata/flink-http-connector/issues/45>

### Removed

-   Removed unused reference to EncodingFormat from HttpLookupTableSource

## [0.8.0] - 2022-12-06

### Added

-   Add new parameters for HTTP timeout configuration and thread pool size for Sink and Lookup source http requests.

### Fixed

-   Fix issue with not cleaning Flink's internal task queue for AsyncIO requests after HTTP timeout in
    Lookup source - <https://github.com/getindata/flink-http-connector/issues/38>

## [0.7.0] - 2022-10-27

-   Add to Lookup Source support for performing lookup on columns with complex types such as ROW, Map etc.
-   Add support for custom Json Serialization format for SQL Lookup Source when using [GenericJsonQueryCreator](src/main/java/com/getindata/connectors/http/internal/table/lookup/querycreators/GenericJsonQueryCreator.java)
    The custom format can be defined using Flink's Factory mechanism. The format name can be defined using
    `lookup-request.format` option. The default format is `json` which means that connector will use FLink's [json-format](https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/table/formats/json/) 

## [0.6.0] - 2022-10-05

### Added

-   Add support for other REST methods like PUT and POST to lookup source connector. The request method can be set using
    new optional lookup-source property `lookup-method`. If property is not specified in table DDL, GET method will be used for
    lookup queries.

## [0.5.0] - 2022-09-22

### Added

-   Add Http Header value preprocessor mechanism, that can preprocess defined header value before setting it on the request.
-   Allow user to specify `Authorization` header for Basic Authentication. The value will be converted to Base64,
    or if it starts from prefix `Basic `, it will be used as is (without any extra modification).
-   Add TLS and mTLS support for Http Sink and Lookup Source connectors.  
    New properties are:
    -   `gid.connector.http.security.cert.server` - path to server's certificate.
    -   `gid.connector.http.security.cert.client` - path to connector's certificate.
    -   `gid.connector.http.security.key.client` - path to connector's private key.
    -   `gid.connector.http.security.cert.server.allowSelfSigned` - allowing for self-signed certificates without adding them to KeyStore (not recommended for a production).
-   Add [LookupQueryCreator](src/main/java/com/getindata/connectors/http/LookupQueryCreator.java) and
    [LookupQueryCreatorFactory](src/main/java/com/getindata/connectors/http/LookupQueryCreatorFactory.java) interfaces 
    (along with a "default"
    [GenericGetQueryCreator](src/main/java/com/getindata/connectors/http/internal/table/lookup/querycreators/GenericGetQueryCreator.java)
    implementation) for customization of queries prepared by Lookup Source for its HTTP requests.
-   Add [ElasticSearchLiteQueryCreator](src/main/java/com/getindata/connectors/http/internal/table/lookup/querycreators/ElasticSearchLiteQueryCreator.java)
    that prepares [`q` parameter query](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-search.html#search-api-query-params-q)
    using Lucene query string syntax (in first versions of ElasticSearch called
    [Search _Lite_](https://www.elastic.co/guide/en/elasticsearch/guide/current/search-lite.html)).

## [0.4.0] - 2022-08-31

### Added

-   Add new properties `gid.connector.http.sink.error.code`,`gid.connector.http.sink.error.code.exclude`,
    `gid.connector.http.source.lookup.error.code` and `gid.connector.http.source.lookup.error.code.exclude`
    to set HTTP status codes that should be interpreted as errors both for HTTP Sink and HTTP Lookup Source.
-   Use Flink's format support to Http Lookup Source.
-   Add HTTP Lookup source client header configuration via properties.
-   Add [HttpPostRequestCallback](src/main/java/com/getindata/connectors/http/HttpPostRequestCallback.java) and
    [HttpPostRequestCallbackFactory](src/main/java/com/getindata/connectors/http/HttpPostRequestCallbackFactory.java)
    interfaces (along with a "default"
    [Slf4jHttpPostRequestCallback](src/main/java/com/getindata/connectors/http/internal/table/sink/Slf4jHttpPostRequestCallback.java)
    implementation) for customizable processing of HTTP Sink requests and responses in Table API.

### Changed

-   Change dependency scope for `org.apache.flink.flink-connector-base` from `compile` to `provided`.
-   Changed DDL of `rest-lookup` connector. Dropped `json-path` properties, and add mandatory `format` property.

### Removed

-   Remove dependency on `org.apache.httpcomponents.httpclient`from production code. Dependency is only for test scope.
-   Removed dependency on `com.jayway.jsonpath.json-path`

### Fixed

-   Fix JavaDoc errors.

## [0.3.0] - 2022-07-21

-   Package refactoring. Hide internal classes that does not have to be used by API users under "internal" package.
    Methods defined in classes located outside "internal" package are considered "public API".
    Any changes to those methods should be communicated as "not backward compatible" and should be avoided.
-   Add checkstyle configuration to "dev" folder. Add checkstyle check during maven build
-   Add HTTP sink client header configuration via properties.

## [0.2.0] - 2022-07-06

-   Implement [HttpSink](src/main/java/com/getindata/connectors/http/HttpSink.java) deriving from [AsyncSinkBase](https://cwiki.apache.org/confluence/display/FLINK/FLIP-171%3A+Async+Sink) introduced in Flink 1.15.
-   Add support for Table API in HttpSink in the form of [HttpDynamicSink](src/main/java/com/getindata/connectors/http/internal/table/sink/HttpDynamicSink.java). 

## [0.1.0] - 2022-05-26

-   Implement basic support for Http connector for Flink SQL

[Unreleased]: https://github.com/getindata/flink-http-connector/compare/0.10.0...HEAD

[0.10.0]: https://github.com/getindata/flink-http-connector/compare/0.9.0...0.10.0

[0.9.0]: https://github.com/getindata/flink-http-connector/compare/0.8.1...0.9.0

[0.8.1]: https://github.com/getindata/flink-http-connector/compare/0.8.0...0.8.1

[0.8.0]: https://github.com/getindata/flink-http-connector/compare/0.7.0...0.8.0

[0.7.0]: https://github.com/getindata/flink-http-connector/compare/0.6.0...0.7.0

[0.6.0]: https://github.com/getindata/flink-http-connector/compare/0.5.0...0.6.0

[0.5.0]: https://github.com/getindata/flink-http-connector/compare/0.4.0...0.5.0

[0.4.0]: https://github.com/getindata/flink-http-connector/compare/0.3.0...0.4.0

[0.3.0]: https://github.com/getindata/flink-http-connector/compare/0.2.0...0.3.0

[0.2.0]: https://github.com/getindata/flink-http-connector/compare/0.1.0...0.2.0

[0.1.0]: https://github.com/getindata/flink-http-connector/compare/dfe9bfeaa73e77b1de14cd0cb0546a925583e23e...0.1.0
