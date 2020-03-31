/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.metrics;

import java.io.IOException;
import java.util.Random;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.EmbeddedCassandraService;

import static org.apache.cassandra.cql3.statements.BatchStatement.metrics;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(OrderedJUnit4ClassRunner.class)
public class BatchMetricsTest extends SchemaLoader
{
    private static EmbeddedCassandraService cassandra;

    private static Cluster cluster;
    private static Session session;

    private static String KEYSPACE = "junit";
    private static final String LOGGER_TABLE = "loggerbatchmetricstest";
    private static final String COUNTER_TABLE = "counterbatchmetricstest";

    private static PreparedStatement psLogger;
    private static PreparedStatement psCounter;

    private final long seed = System.nanoTime();
    private final Random rand = new Random(seed);

    @BeforeClass()
    public static void setup() throws ConfigurationException, IOException
    {
        Schema.instance.clear();

        cassandra = new EmbeddedCassandraService();
        cassandra.start();

        cluster = Cluster.builder().addContactPoint("127.0.0.1").withPort(DatabaseDescriptor.getNativeTransportPort()).build();
        session = cluster.connect();

        session.execute("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + " WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        session.execute("USE " + KEYSPACE);
        session.execute("CREATE TABLE IF NOT EXISTS " + LOGGER_TABLE + " (id int PRIMARY KEY, val text);");
        session.execute("CREATE TABLE IF NOT EXISTS " + COUNTER_TABLE + " (id int PRIMARY KEY, val counter);");

        psLogger = session.prepare("INSERT INTO " + KEYSPACE + '.' + LOGGER_TABLE + " (id, val) VALUES (?, ?);");
        psCounter = session.prepare("UPDATE " + KEYSPACE + '.' + COUNTER_TABLE + " SET val = val + 1 WHERE id = ?;");
    }

    private void executeLoggerBatch(BatchStatement.Type batchStatementType, int distinctPartitions, int statementsPerPartition)
    {
        BatchStatement batch = new BatchStatement(batchStatementType);

        for (int i = 0; i < distinctPartitions; i++)
        {
            for (int j = 0; j < statementsPerPartition; j++)
            {
                if (batchStatementType == BatchStatement.Type.UNLOGGED || batchStatementType == BatchStatement.Type.LOGGED)
                    batch.add(psLogger.bind(i, "aaaaaaaa"));
                else if (batchStatementType == BatchStatement.Type.COUNTER)
                    batch.add(psCounter.bind(i));
                else
                    throw new IllegalStateException("There is no a case for BatchStatement.Type." + batchStatementType.name());
            }
        }

        session.execute(batch);
    }

    @Test
    public void testLoggedPartitionsPerBatch()
    {
        assertPartitionsPerBatch(BatchStatement.Type.LOGGED);
    }

    @Test
    public void testUnloggedPartitionsPerBatch()
    {
        assertPartitionsPerBatch(BatchStatement.Type.UNLOGGED);
    }

    @Test
    public void testCounterPartitionsPerBatch()
    {
        assertPartitionsPerBatch(BatchStatement.Type.COUNTER);
    }

    /**
     * Tries a range of batches (up to 10) of the specified batch type, each with a range of statement sizes (up to
     * 50), and asserts the counts of each of the {@code BatchMetrics} given that specified batch type.
     */
    private void assertPartitionsPerBatch(BatchStatement.Type batchTypeTested)
    {
        rand.ints(rand.nextInt(10) + 1, 1, 50).forEach(i -> assertMetrics(batchTypeTested, i));
    }

    private void assertMetrics(BatchStatement.Type batchTypeTested, int statementsPerPartition)
    {
        int partitionsPerLoggedBatchCountPre = (int) metrics.partitionsPerLoggedBatch.getCount();
        int partitionsPerLoggedBatchCountPost = partitionsPerLoggedBatchCountPre + (batchTypeTested == BatchStatement.Type.LOGGED ? 1 : 0);
        int partitionsPerUnloggedBatchCountPre = (int) metrics.partitionsPerUnloggedBatch.getCount();
        int partitionsPerUnloggedBatchCountPost = partitionsPerUnloggedBatchCountPre + (batchTypeTested == BatchStatement.Type.UNLOGGED ? 1 : 0);
        int partitionsPerCounterBatchCountPre = (int) metrics.partitionsPerCounterBatch.getCount();
        int partitionsPerCounterBatchCountPost = partitionsPerCounterBatchCountPre + (batchTypeTested == BatchStatement.Type.COUNTER ? 1 : 0);

        int distinctPartitions = rand.nextInt(10) + 1;
        executeLoggerBatch(batchTypeTested, distinctPartitions, statementsPerPartition);

        String assertionMessage = String.format("Seed was %s producing %s distinctPartitions and %s statementsPerPartition",
                                                seed,
                                                distinctPartitions,
                                                statementsPerPartition);

        assertEquals(assertionMessage,partitionsPerUnloggedBatchCountPost, metrics.partitionsPerUnloggedBatch.getCount());
        assertEquals(assertionMessage, partitionsPerLoggedBatchCountPost, metrics.partitionsPerLoggedBatch.getCount());
        assertEquals(assertionMessage, partitionsPerCounterBatchCountPost, metrics.partitionsPerCounterBatch.getCount());

        // decayingBuckets may not have exact value
        assertTrue(assertionMessage,partitionsPerLoggedBatchCountPre <= metrics.partitionsPerLoggedBatch.getSnapshot().getMax());
        assertTrue(assertionMessage,partitionsPerUnloggedBatchCountPre <= metrics.partitionsPerUnloggedBatch.getSnapshot().getMax());
        assertTrue(assertionMessage,partitionsPerCounterBatchCountPre <= metrics.partitionsPerCounterBatch.getSnapshot().getMax());
    }
}
