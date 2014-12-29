/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.junit.Test;

/**
 * Various index reset tests.
 */
public class VanillaIndexResetTest extends VanillaChronicleTestBase {

	/**
	 * Count total number of files in the queue folder.
	 */
	static int countFiles(final String baseDir) {
		final File base = new File(baseDir);
		final String[] baseList = base.list();
		if (baseList == null) {
			return 0; // Missing folder.
		}
		assertEquals(1, baseList.length);
		final File cycle = new File(base, baseList[0]);
		final String[] cycleList = cycle.list();
		return cycleList.length;
	}

	/**
	 * Should not happen: when index provider creates next index block on disk
	 * (say index-0 followed by index-1), the last write index value is getting
	 * reset to the original value used by the first entry written to index.
	 */
	@Test
	public void testIndexJumpWhenCreatingNewIndexBlock() throws Exception {

		final int RUNS = 30;

		final String baseDir = getTestPath();
		assertNotNull(baseDir);

		final VanillaChronicle chronicle = (VanillaChronicle) ChronicleQueueBuilder
				.vanilla(baseDir) //
				.defaultMessageSize(128) //
				.indexBlockSize(128) //
				.dataBlockSize(16 * 1024) //
				.build();

		chronicle.clear();

		assertEquals(0, countFiles(baseDir)); // No data, no index.

		// Index history while only index-0 present.
		final long[] indexPrior = new long[RUNS];

		// Index history when both index-0 and index-1 present.
		final long[] indexAfter = new long[RUNS];

		try {
			int historyPrior = 0;
			int historyAfter = 0;
			final ExcerptAppender appender = chronicle.createAppender();
			for (int entry = 0; entry < RUNS; entry++) {
				appender.startExcerpt();
				appender.writeLong(entry);
				appender.finish();
				chronicle.checkCounts(1, 2);
				final long last = appender.lastWrittenIndex();
				if (countFiles(baseDir) < 3) {
					indexPrior[historyPrior++] = last;
					LOGGER.info("PRIOR last=" + last);
				} else {
					indexAfter[historyAfter++] = last;
					LOGGER.info("AFTER last=" + last);
				}
			}
			appender.close();
			chronicle.checkCounts(1, 1);
		} finally {
			chronicle.close();
			assertEquals(3, countFiles(baseDir)); // 1 data, 2 index.
			chronicle.clear();
			assertFalse(new File(baseDir).exists());
		}

		// Verify few first index history pairs.
		for (int history = 0; history < 5; history++) {
			final long valuePrior = indexPrior[history];
			final long valueAfter = indexAfter[history];
			assertTrue(
					"Indext written prior and after index block creation should be different: "
							+ valuePrior + " vs " + valueAfter,
					valuePrior != valueAfter);
		}

	}
}
