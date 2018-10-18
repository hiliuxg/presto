/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.elasticsearch;

import com.facebook.presto.Session;
import com.facebook.presto.client.Column;
import com.facebook.presto.client.QueryData;
import com.facebook.presto.client.QueryStatusInfo;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.Varchars;
import com.facebook.presto.tests.AbstractTestingPrestoClient;
import com.facebook.presto.tests.ResultsSession;
import io.airlift.log.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.common.xcontent.XContentType.JSON;

public class ElasticsearchLoader
        extends AbstractTestingPrestoClient<Void>
{
    private static final Logger log = Logger.get(ElasticsearchLoader.class);

    private final String tableName;
    private final Client client;

    public ElasticsearchLoader(Client client,
            String tableName,
            TestingPrestoServer prestoServer,
            Session defaultSession)
    {
        super(prestoServer, defaultSession);

        this.tableName = tableName;
        this.client = client;
    }

    @Override
    public ResultsSession<Void> getResultSession(Session session)
    {
        requireNonNull(session, "session is null");
        return new ElasticsearchLoadingSession(session);
    }

    private class ElasticsearchLoadingSession
            implements ResultsSession<Void>
    {
        private final AtomicReference<List<Type>> types = new AtomicReference<>();

        private final TimeZoneKey timeZoneKey;

        private ElasticsearchLoadingSession(Session session)
        {
            this.timeZoneKey = session.getTimeZoneKey();
        }

        @Override
        public void addResults(QueryStatusInfo statusInfo, QueryData data)
        {
            if (types.get() == null && statusInfo.getColumns() != null) {
                types.set(getTypes(statusInfo.getColumns()));
            }

            if (data.getData() != null) {
                checkState(types.get() != null, "Data without types received!");
                List<Column> columns = statusInfo.getColumns();
                for (List<Object> fields : data.getData()) {
                    try {
                        XContentBuilder dataBuilder = jsonBuilder().startObject();
                        for (int i = 0; i < fields.size(); i++) {
                            Type type = types.get().get(i);
                            Object value = convertValue(fields.get(i), type);
                            dataBuilder.field(columns.get(i).getName(), value);
                        }
                        dataBuilder.endObject();
                        client.prepareIndex(tableName, "doc").setSource(dataBuilder.string(), JSON).get();
                    }
                    catch (IOException e) {
                        throw new RuntimeException("Failure loading data into Elasticsearch index: " + tableName, e);
                    }
                }
            }
        }

        @Override
        public Void build(Map<String, String> setSessionProperties, Set<String> resetSessionProperties)
        {
            return null;
        }

        private Object convertValue(Object value, Type type)
        {
            if (value == null) {
                return null;
            }

            if (BOOLEAN.equals(type) || Varchars.isVarcharType(type)) {
                return value;
            }
            if (BIGINT.equals(type)) {
                return ((Number) value).longValue();
            }
            if (INTEGER.equals(type)) {
                return ((Number) value).intValue();
            }
            if (DOUBLE.equals(type)) {
                return ((Number) value).doubleValue();
            }
            if (DATE.equals(type)) {
                return value;
            }
            throw new AssertionError("unhandled type: " + type);
        }
    }
}
