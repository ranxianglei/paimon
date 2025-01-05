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

package org.apache.paimon.benchmark.deletionvectors;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.benchmark.Benchmark;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.IntType;
import org.apache.paimon.utils.Pair;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/** Benchmark for deletion vector push down table read. */
public class DeletionVectorsWithIndexResultPushDownBenchmark {

    private static final int VALUE_COUNT = 20;

    private final int rowCount = 1000000;
    java.nio.file.Path tempFile = new File("/halo/workspaces/for_remote/tmp").toPath();

    private final RandomDataGenerator random = new RandomDataGenerator();

    @Test
    public void testParquetRead() throws Exception {
        System.out.println(tempFile);
        //        int[] pkBounds = new int[]{10000000, 30000000, 50000000, 100000000};
        int[] pkBounds = new int[] {10000000};
        //        int[] indexBounds = new int[]{50000, 100000, 300000, 800000};
        int[] indexBounds = new int[] {800000};
        for (int pkBound : pkBounds) {
            for (int indexBound : indexBounds) {
                                Table table =
                                        prepareData(
                                                pkBound,
                                                indexBound,
                                                parquet(),
                                                String.format("parquet_%s_%s", pkBound,
                 indexBound));
//                Table table = getTable(String.format("parquet_%s_%s", pkBound, indexBound));
                Map<String, Table> tables = new LinkedHashMap<>();

                Map<String, String> p1 = new HashMap<>();
                p1.put(CoreOptions.DELETION_VECTORS_PUSH_DOWN.key(), "false");
                p1.put(CoreOptions.FILE_INDEX_READ_ENABLED.key(), "false");
                tables.put("normal", table.copy(p1));

                Map<String, String> p2 = new HashMap<>();
                p2.put(CoreOptions.DELETION_VECTORS_PUSH_DOWN.key(), "true");
                p2.put(CoreOptions.FILE_INDEX_READ_ENABLED.key(), "false");
                tables.put("dv-push-down", table.copy(p2));

                Map<String, String> p3 = new HashMap<>();
                p3.put(CoreOptions.DELETION_VECTORS_PUSH_DOWN.key(), "false");
                p3.put(CoreOptions.FILE_INDEX_READ_ENABLED.key(), "true");
                tables.put("index-push-down", table.copy(p3));

                Map<String, String> p4 = new HashMap<>();
                p4.put(CoreOptions.DELETION_VECTORS_PUSH_DOWN.key(), "true");
                p4.put(CoreOptions.FILE_INDEX_READ_ENABLED.key(), "true");
                tables.put("dv-and-index-push-down", table.copy(p4));

                int[] values = new int[] {788897};
                for (int i = 0; i < values.length; i++) {
                    //                for (int i = 0; i < 10; i++) {
                    //                    int value = random.nextInt(0, indexBound);
                    int value = values[i];
                    Predicate predicate = new PredicateBuilder(table.rowType()).equal(1, value);
                    innerTest(tables, pkBound, indexBound, value, predicate);
                    testResult(
                            tables.get("normal"), tables.get("dv-and-index-push-down"), predicate);
                }
            }
        }

        // new File(tempFile.toString()).delete();
    }

    private Options parquet() {
        Options options = new Options();
        options.set(CoreOptions.BUCKET, 1);
        options.set(CoreOptions.WRITE_BUFFER_SIZE, MemorySize.parse("1 GB"));
        options.set(CoreOptions.FILE_FORMAT, CoreOptions.FILE_FORMAT_PARQUET);
        options.set(CoreOptions.DELETION_VECTORS_ENABLED, true);
        options.set("file-index.bsi.columns", "index");
        options.set(CoreOptions.SNAPSHOT_TIME_RETAINED, Duration.ofMinutes(1));
        options.set(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN, 6);
        return options;
    }

    private void innerTest(
            Map<String, Table> tables,
            int pkBound,
            int indexBound,
            int value,
            Predicate predicate) {
        int readTime = 3;
        Benchmark benchmark =
                new Benchmark("read", readTime * rowCount)
                        .setNumWarmupIters(1)
                        .setOutputPerIteration(true);

        for (String name : tables.keySet()) {
            benchmark.addCase(
                    "read-" + name + "-" + pkBound + "-" + indexBound + "-" + value,
                    3,
                    () -> {
                        Table table = tables.get(name);
                        for (int i = 0; i < readTime; i++) {
                            List<Split> splits = table.newReadBuilder().newScan().plan().splits();
                            AtomicLong readCount = new AtomicLong(0);
                            try {
                                for (Split split : splits) {
                                    RecordReader<InternalRow> reader =
                                            table.newReadBuilder()
                                                    .withFilter(predicate)
                                                    .newRead()
                                                    .createReader(split);
                                    reader.forEachRemaining(row -> readCount.incrementAndGet());
                                }
                                System.out.printf("Finish read %d rows.\n", readCount.get());
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        }
                    });
        }
        benchmark.run();
    }

    private void testResult(Table actual, Table expect, Predicate predicate) {
        Map<Integer, Pair<Integer, String>> actualMap = new HashMap<>();
        try {
            List<Split> splits = actual.newReadBuilder().newScan().plan().splits();
            for (Split split : splits) {
                RecordReader<InternalRow> reader =
                        actual.newReadBuilder()
                                .withFilter(predicate)
                                .newRead()
                                .executeFilter()
                                .createReader(split);
                reader.forEachRemaining(
                        row -> {
                            actualMap.put(
                                    row.getInt(0),
                                    Pair.of(row.getInt(1), row.getString(2).toString()));
                        });
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        try {
            List<Split> splits = expect.newReadBuilder().newScan().plan().splits();
            for (Split split : splits) {
                RecordReader<InternalRow> reader =
                        expect.newReadBuilder().withFilter(predicate).newRead().createReader(split);
                reader.forEachRemaining(
                        row -> {
                            int key = row.getInt(0);
                            assertThat(actualMap).containsKey(key);
                            Pair<Integer, String> pair = actualMap.get(key);
                            assertThat(pair.getLeft()).isEqualTo(row.getInt(1));
                            assertThat(pair.getRight()).isEqualTo(row.getString(2).toString());
                        });
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Table prepareData(int pkBound, int indexBound, Options options, String tableName)
            throws Exception {
        Table table = createTable(options, tableName);
        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder();
        StreamTableWrite write =
                (StreamTableWrite)
                        writeBuilder
                                .newWrite()
                                .withIOManager(new IOManagerImpl(tempFile.toString()));
        StreamTableCommit commit = writeBuilder.newCommit();

        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < rowCount; j++) {
                try {
                    write.write(newRandomRow(pkBound, indexBound));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            List<CommitMessage> commitMessages = write.prepareCommit(true, i);
            commit.commit(i, commitMessages);
        }

        write.close();
        commit.close();
        return table;
    }

    protected Table createTable(Options tableOptions, String tableName) throws Exception {
        Options catalogOptions = new Options();
        catalogOptions.set(CatalogOptions.WAREHOUSE, tempFile.toUri().toString());
        Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(catalogOptions));
        String database = "default";
        catalog.createDatabase(database, true);

        List<DataField> fields = new ArrayList<>();
        fields.add(new DataField(0, "pk", new IntType()));
        fields.add(new DataField(1, "index", new IntType()));
        for (int i = 2; i <= VALUE_COUNT; i++) {
            fields.add(new DataField(i, "f" + i, DataTypes.STRING()));
        }
        Schema schema =
                new Schema(
                        fields,
                        Collections.emptyList(),
                        Collections.singletonList("pk"),
                        tableOptions.toMap(),
                        "");
        Identifier identifier = Identifier.create(database, tableName);
        catalog.createTable(identifier, schema, false);
        return catalog.getTable(identifier);
    }

    protected Table getTable(String tableName) throws Catalog.TableNotExistException {
        Options catalogOptions = new Options();
        catalogOptions.set(CatalogOptions.WAREHOUSE, tempFile.toUri().toString());
        Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(catalogOptions));
        return catalog.getTable(Identifier.create("default", tableName));
    }

    protected InternalRow newRandomRow(int pkBound, int indexBound) {
        GenericRow row = new GenericRow(2 + VALUE_COUNT);
        row.setField(0, random.nextInt(0, pkBound));
        row.setField(1, random.nextInt(0, indexBound));
        for (int i = 2; i <= VALUE_COUNT; i++) {
            row.setField(i, BinaryString.fromString(random.nextHexString(10)));
        }
        return row;
    }
}
