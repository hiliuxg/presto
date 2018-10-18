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

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TestElasticsearchConnectorConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(ElasticsearchConnectorConfig.class)
                .setTableDescriptionDir(new File("etc/elasticsearch/"))
                .setDefaultSchema("default")
                .setTableNames("")
                .setScrollSize(1000)
                .setScrollTime(new Duration(20, TimeUnit.SECONDS))
                .setMaxHits(1000000)
                .setRequestTimeout(new Duration(10, TimeUnit.SECONDS)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("elasticsearch.table-description-dir", "/etc/elasticsearch/")
                .put("elasticsearch.table-names", "test.table1,test.table2,test.table3")
                .put("elasticsearch.default-schema", "test")
                .put("elasticsearch.scroll-size", "4000")
                .put("elasticsearch.scroll-time", "2m")
                .put("elasticsearch.max-hits", "20000")
                .put("elasticsearch.request-timeout", "2m")
                .build();

        ElasticsearchConnectorConfig expected = new ElasticsearchConnectorConfig()
                .setTableDescriptionDir(new File("/etc/elasticsearch/"))
                .setTableNames("test.table1,test.table2,test.table3")
                .setDefaultSchema("test")
                .setScrollSize(4000)
                .setScrollTime(new Duration(120, TimeUnit.SECONDS))
                .setMaxHits(20000)
                .setRequestTimeout(new Duration(120, TimeUnit.SECONDS));

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
