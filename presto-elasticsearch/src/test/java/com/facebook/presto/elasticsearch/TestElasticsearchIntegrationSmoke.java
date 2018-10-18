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

import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestIntegrationSmokeTest;
import com.google.common.io.Closer;
import io.airlift.tpch.TpchTable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;

import static com.facebook.presto.elasticsearch.EmbeddedElasticsearch.createEmbeddedElasticsearch;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;

public class TestElasticsearchIntegrationSmoke
        extends AbstractTestIntegrationSmokeTest
{
    public static final String SCHEMA = "tpch";
    private final EmbeddedElasticsearch embeddedElasticsearch;

    private QueryRunner queryRunner;

    public TestElasticsearchIntegrationSmoke()
            throws Exception
    {
        this(createEmbeddedElasticsearch());
    }

    public TestElasticsearchIntegrationSmoke(EmbeddedElasticsearch embeddedElasticsearch)
    {
        super(() -> ElasticsearchQueryRunner.createElasticsearchQueryRunner(embeddedElasticsearch, TpchTable.getTables()));
        this.embeddedElasticsearch = embeddedElasticsearch;
    }

    @BeforeClass
    public void setUp()
    {
        queryRunner = getQueryRunner();
    }

    @Test
    @Override
    public void testDescribeTable()
    {
        MaterializedResult actualColumns = this.computeActual("DESC orders").toTestTypes();
        MaterializedResult.Builder builder = MaterializedResult.resultBuilder(this.getQueryRunner().getDefaultSession(), VarcharType.VARCHAR, VarcharType.VARCHAR, VarcharType.VARCHAR, VarcharType.VARCHAR);
        for (MaterializedRow row : actualColumns.getMaterializedRows()) {
            builder.row(row.getField(0), row.getField(1), "", "");
        }
        MaterializedResult filteredActual = builder.build();
        builder = MaterializedResult.resultBuilder(this.getQueryRunner().getDefaultSession(), VarcharType.VARCHAR, VarcharType.VARCHAR, VarcharType.VARCHAR, VarcharType.VARCHAR);
        MaterializedResult expectedColumns = builder
                .row("orderkey", "bigint", "", "")
                .row("custkey", "bigint", "", "")
                .row("orderstatus", "varchar", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "varchar", "", "")
                .row("orderpriority", "varchar", "", "")
                .row("clerk", "varchar", "", "")
                .row("shippriority", "bigint", "", "")
                .row("comment", "varchar", "", "").build();
        assertEquals(filteredActual, expectedColumns, String.format("%s != %s", filteredActual, expectedColumns));
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
            throws IOException
    {
        try (Closer closer = Closer.create()) {
            closer.register(queryRunner::close);
            closer.register(embeddedElasticsearch::close);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
