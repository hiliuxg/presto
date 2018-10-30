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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.units.Duration;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import static com.facebook.presto.elasticsearch.ElasticsearchErrorCode.ELASTIC_SEARCH_EXCEEDS_MAX_HIT_ERROR;
import static com.facebook.presto.elasticsearch.ElasticsearchErrorCode.ELASTIC_SEARCH_JSON_PROCESSING_ERROR;
import static com.facebook.presto.elasticsearch.ElasticsearchUtils.serializeObject;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Objects.requireNonNull;

public class ElasticsearchRecordCursor
        implements RecordCursor
{
    private static final Logger log = Logger.get(ElasticsearchRecordCursor.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get();

    private final List<ElasticsearchColumnHandle> columnHandles;
    private final Map<String, Integer> jsonPathToIndex = new HashMap<>();
    private final TupleDomain<ColumnHandle> tupleDomain;
    private final int maxHits;
    private final Iterator<SearchHit> searchHits;
    private final Duration timeout;
    private final ElasticsearchQueryBuilder builder;

    private long totalBytes;
    private List<Object> fields;

    public ElasticsearchRecordCursor(List<ElasticsearchColumnHandle> columnHandles, ElasticsearchConnectorConfig config, ElasticsearchSplit split)
    {
        this.columnHandles = requireNonNull(columnHandles, "columnHandle is null");
        this.maxHits = requireNonNull(config, "config is null").getMaxHits();
        this.tupleDomain = requireNonNull(split, "split is null").getTupleDomain();
        this.timeout = config.getRequestTimeout();

        for (int i = 0; i < columnHandles.size(); i++) {
            jsonPathToIndex.put(columnHandles.get(i).getColumnJsonPath(), i);
        }
        this.builder = new ElasticsearchQueryBuilder(columnHandles, config, split);
        searchHits = sendElasticQuery(builder).iterator();
    }

    @Override
    public long getCompletedBytes()
    {
        return totalBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (!searchHits.hasNext()) {
            return false;
        }

        SearchHit hit = searchHits.next();
        fields = new ArrayList(Collections.nCopies(columnHandles.size(), null));

        setFieldIfExists("_id", hit.getId());
        setFieldIfExists("_index", hit.getIndex());

        extractFromSource(hit);
        totalBytes += fields.size();
        return true;
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkFieldType(field, ImmutableSet.of(BOOLEAN));
        return (Boolean) getFieldValue(field);
    }

    @Override
    public long getLong(int field)
    {
        checkFieldType(field, ImmutableSet.of(BIGINT, INTEGER));
        return (Integer) getFieldValue(field);
    }

    @Override
    public double getDouble(int field)
    {
        checkFieldType(field, ImmutableSet.of(DOUBLE));
        return (Double) getFieldValue(field);
    }

    @Override
    public Slice getSlice(int field)
    {
        checkFieldType(field, ImmutableSet.of(VARCHAR));

        Object value = getFieldValue(field);
        ElasticsearchColumnHandle column = columnHandles.get(field);
        if (value instanceof Collection) {
            try {
                return utf8Slice(OBJECT_MAPPER.writeValueAsString((List<Map<String, Object>>) value));
            }
            catch (IOException e) {
                throw new PrestoException(ELASTIC_SEARCH_JSON_PROCESSING_ERROR, e);
            }
        }
        return utf8Slice(String.valueOf(value));
    }

    @Override
    public Object getObject(int field)
    {
        return serializeObject(columnHandles.get(field).getColumnType(), null, getFieldValue(field));
    }

    @Override
    public boolean isNull(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return getFieldValue(field) == null;
    }

    private void checkFieldType(int field, Set<Type> expectedTypes)
    {
        checkArgument(expectedTypes.contains(getType(field)), "Field %s expected type inconsistent with %s", field, getType(field));
    }

    @Override
    public void close()
    {
        builder.close();
    }

    private List<SearchHit> sendElasticQuery(ElasticsearchQueryBuilder queryBuilder)
    {
        long now = System.currentTimeMillis();
        ImmutableList.Builder<SearchHit> result = ImmutableList.builder();
        SearchResponse response = queryBuilder.buildScrollSearchRequest().execute().actionGet(timeout.toMillis());
        if (response.getHits().getTotalHits() > maxHits) {
            throw new PrestoException(ELASTIC_SEARCH_EXCEEDS_MAX_HIT_ERROR, "The number of hits for the query: " + response.getHits().getTotalHits() + " exceeds the configured max hits: " + maxHits);
        }
        while (true) {
            for (SearchHit hit : response.getHits().getHits()) {
                result.add(hit);
            }
            response = queryBuilder.prepareSearchScroll(response.getScrollId()).execute().actionGet(timeout.toMillis());
            if (response.getHits().getHits().length == 0) {
                break;
            }
        }
        log.info("request elasticsearch returns totalhits -> " + response.getHits().getTotalHits() + " and spend " + (System.currentTimeMillis()-now)/1000.00 + "s");
        return result.build();
    }

    private void setFieldIfExists(String jsonPath, Object jsonValue)
    {
        if (jsonPathToIndex.containsKey(jsonPath)) {
            fields.set(jsonPathToIndex.get(jsonPath), jsonValue);
        }
    }

    private Object getFieldValue(int field)
    {
        checkState(fields != null, "Cursor has not been advanced yet");
        return fields.get(field);
    }

    private void extractFromSource(SearchHit hit)
    {
        Map<String, Object> map = hit.getSourceAsMap();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String jsonPath = entry.getKey();
            Object entryValue = entry.getValue();

            setFieldIfExists(jsonPath, entryValue);
        }
    }
}
