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

import com.facebook.presto.spi.SchemaTableName;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.nio.file.Files.readAllBytes;
import static java.util.Objects.requireNonNull;

public class ElasticsearchTableDescriptionSupplier
        implements Supplier<Map<SchemaTableName, ElasticsearchTableDescription>>
{
    private static final Logger log = Logger.get(ElasticsearchTableDescriptionSupplier.class);

    private final JsonCodec<ElasticsearchTableDescription> codec;
    private final ElasticsearchConnectorConfig config;

    @Inject
    ElasticsearchTableDescriptionSupplier(ElasticsearchConnectorConfig config, JsonCodec<ElasticsearchTableDescription> codec)
    {
        this.codec = requireNonNull(codec, "codec is null");
        this.config = requireNonNull(config, "config is null");
    }

    @Override
    public Map<SchemaTableName, ElasticsearchTableDescription> get()
    {
        return createElasticsearchTableDescriptions(config.getTableDescriptionDir(), config.getDefaultSchema(), ImmutableSet.copyOf(config.getTableNames()), codec);
    }

    public static Map<SchemaTableName, ElasticsearchTableDescription> createElasticsearchTableDescriptions(File tableDescriptionDir, String defaultSchema, Set<String> tableNames, JsonCodec<ElasticsearchTableDescription> tableDescriptionCodec)
    {
        ImmutableMap.Builder<SchemaTableName, ElasticsearchTableDescription> builder = ImmutableMap.builder();

        log.debug("Loading elasticsearch table definitions from %s", tableDescriptionDir.getAbsolutePath());

        try {
            for (File file : listFiles(tableDescriptionDir)) {
                if (file.isFile() && file.getName().endsWith(".json")) {
                    ElasticsearchTableDescription table = tableDescriptionCodec.fromJson(readAllBytes(file.toPath()));
                    String schemaName = firstNonNull(table.getSchemaName(), defaultSchema);
                    log.debug("Elasticsearch table %s.%s: %s", schemaName, table.getTableName(), table);
                    builder.put(new SchemaTableName(schemaName, table.getTableName()), table);
                }
            }

            Map<SchemaTableName, ElasticsearchTableDescription> tableDefinitions = builder.build();

            log.debug("Loaded Table definitions: %s", tableDefinitions.keySet());

            builder = ImmutableMap.builder();
            for (String definedTable : tableNames) {
                SchemaTableName tableName;
                tableName = parseTableName(definedTable);

                if (!tableDefinitions.containsKey(tableName)) {
                    throw new IOException("Missing table definition for: " + tableName);
                }
                ElasticsearchTableDescription elasticsearchTable = tableDefinitions.get(tableName);
                builder.put(tableName, elasticsearchTable);
            }
            return builder.build();
        }
        catch (IOException e) {
            log.warn(e, "Error: ");
            throw new UncheckedIOException(e);
        }
    }

    private static List<File> listFiles(File dir)
    {
        if ((dir != null) && dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null) {
                return ImmutableList.copyOf(files);
            }
        }
        return ImmutableList.of();
    }

    private static SchemaTableName parseTableName(String schemaTableName)
    {
        checkArgument(!isNullOrEmpty(schemaTableName), "schemaTableName is null or is empty");
        List<String> parts = Splitter.on('.').splitToList(schemaTableName);
        checkArgument(parts.size() == 2, "Invalid schemaTableName: %s", schemaTableName);
        return new SchemaTableName(parts.get(0), parts.get(1));
    }
}
