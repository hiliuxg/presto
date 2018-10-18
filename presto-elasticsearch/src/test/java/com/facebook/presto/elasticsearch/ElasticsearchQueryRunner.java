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
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tests.TestingPrestoClient;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.airlift.tpch.TpchTable;

import java.io.File;
import java.net.URI;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.elasticsearch.ElasticsearchTableDescriptionSupplier.createElasticsearchTableDescriptions;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.airlift.units.Duration.nanosSince;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertNotNull;

public final class ElasticsearchQueryRunner
{
    private ElasticsearchQueryRunner()
    {
    }

    private static final Logger log = Logger.get("TestQueries");
    private static final String TPCH_SCHEMA = "tpch";

    public static DistributedQueryRunner createElasticsearchQueryRunner(EmbeddedElasticsearch embeddedElasticsearch, TpchTable<?>... tables)
            throws Exception
    {
        return createElasticsearchQueryRunner(embeddedElasticsearch, ImmutableList.copyOf(tables));
    }

    public static DistributedQueryRunner createElasticsearchQueryRunner(EmbeddedElasticsearch embeddedElasticsearch, Iterable<TpchTable<?>> tables)
            throws Exception
    {
        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner = new DistributedQueryRunner(createSession(), 2);

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            embeddedElasticsearch.start();

            Map<SchemaTableName, ElasticsearchTableDescription> tableDescriptions = createTableDescriptions(queryRunner.getCoordinator().getMetadata(), tables);

            installElasticsearchPlugin(embeddedElasticsearch, queryRunner, tables, tableDescriptions);

            TestingPrestoClient prestoClient = queryRunner.getClient();

            log.info("Loading data...");
            long startTime = System.nanoTime();
            for (TpchTable<?> table : tables) {
                loadTpchTopic(embeddedElasticsearch, prestoClient, table);
            }
            log.info("Loading complete in %s", nanosSince(startTime).toString(SECONDS));

            return queryRunner;
        }
        catch (Exception e) {
            closeAllSuppress(e, queryRunner, embeddedElasticsearch);
            throw e;
        }
    }

    private static Map<SchemaTableName, ElasticsearchTableDescription> createTableDescriptions(Metadata metadata, Iterable<TpchTable<?>> tables)
            throws Exception
    {
        JsonCodec<ElasticsearchTableDescription> codec = new CodecSupplier<>(ElasticsearchTableDescription.class, metadata).get();

        URL metadataUrl = Resources.getResource(ElasticsearchQueryRunner.class, "/queryrunner");
        assertNotNull(metadataUrl, "metadataUrl is null");
        URI metadataUri = metadataUrl.toURI();

        ImmutableSet.Builder<String> tableNames = ImmutableSet.builder();
        for (TpchTable<?> table : tables) {
            tableNames.add(TPCH_SCHEMA + "." + table.getTableName());
        }

        return createElasticsearchTableDescriptions(new File(metadataUri), TPCH_SCHEMA, tableNames.build(), codec);
    }

    private static void installElasticsearchPlugin(EmbeddedElasticsearch embeddedElasticsearch, QueryRunner queryRunner, Iterable<TpchTable<?>> tables, Map<SchemaTableName, ElasticsearchTableDescription> tableDescriptions)
            throws Exception
    {
        ElasticsearchPlugin plugin = new ElasticsearchPlugin();
        plugin.setTableDescriptionSupplier(() -> tableDescriptions);
        queryRunner.installPlugin(plugin);

        ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
        for (TpchTable<?> table : tables) {
            tableNames.add(new SchemaTableName(TPCH_SCHEMA, table.getTableName()));
        }

        URL metadataUrl = Resources.getResource(ElasticsearchQueryRunner.class, "/queryrunner");
        assertNotNull(metadataUrl, "metadataUrl is null");
        URI metadataUri = metadataUrl.toURI();
        Map<String, String> config = new HashMap<>();
        config.put("elasticsearch.default-schema", TPCH_SCHEMA);
        config.put("elasticsearch.table-names", Joiner.on(",").join(tableNames.build()));
        config.put("elasticsearch.table-description-dir", metadataUri.toString());
        config.put("elasticsearch.scroll-size", "1000");
        config.put("elasticsearch.scroll-time", "1m");
        config.put("elasticsearch.max-hits", "1000000");
        config.put("elasticsearch.request-timeout", "2m");

        queryRunner.createCatalog("elasticsearch", "elasticsearch", config);
    }

    private static void loadTpchTopic(EmbeddedElasticsearch embeddedElasticsearch, TestingPrestoClient prestoClient, TpchTable<?> table)
    {
        long start = System.nanoTime();
        log.info("Running import for %s", table.getTableName());
        ElasticsearchLoader loader = new ElasticsearchLoader(embeddedElasticsearch.getClient(), table.getTableName().toLowerCase(ENGLISH), prestoClient.getServer(), prestoClient.getDefaultSession());
        loader.execute(format("SELECT * from %s", new QualifiedObjectName(TPCH_SCHEMA, TINY_SCHEMA_NAME, table.getTableName().toLowerCase(ENGLISH))));
        log.info("Imported %s in %s", table.getTableName(), nanosSince(start).convertToMostSuccinctTimeUnit());
    }

    public static Session createSession()
    {
        return testSessionBuilder().setCatalog("elasticsearch").setSchema(TPCH_SCHEMA).build();
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();
        DistributedQueryRunner queryRunner = createElasticsearchQueryRunner(EmbeddedElasticsearch.createEmbeddedElasticsearch(), TpchTable.getTables());
        Thread.sleep(10);
        Logger log = Logger.get(ElasticsearchQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
