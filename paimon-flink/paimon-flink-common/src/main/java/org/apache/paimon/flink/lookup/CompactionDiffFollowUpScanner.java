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

package org.apache.paimon.flink.lookup;

import org.apache.paimon.Snapshot;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.snapshot.FollowUpScanner;
import org.apache.paimon.table.source.snapshot.SnapshotReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** {@link FollowUpScanner} for read all changed files after compact. */
public class CompactionDiffFollowUpScanner implements FollowUpScanner {

    private static final Logger LOG = LoggerFactory.getLogger(CompactionDiffFollowUpScanner.class);

    @Override
    public boolean shouldScanSnapshot(Snapshot snapshot) {
        if (snapshot.commitKind() == Snapshot.CommitKind.COMPACT) {
            return true;
        }

        LOG.debug(
                "Next snapshot id {} is not COMPACT, but is {}, check next one.",
                snapshot.id(),
                snapshot.commitKind());
        return false;
    }

    @Override
    public SnapshotReader.Plan scan(Snapshot snapshot, SnapshotReader snapshotReader) {
        return snapshotReader.withMode(ScanMode.DELTA).withSnapshot(snapshot).readChanges();
    }
}
