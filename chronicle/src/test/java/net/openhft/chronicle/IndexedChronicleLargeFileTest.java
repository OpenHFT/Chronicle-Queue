/*
 * Copyright 2015 Higher Frequency Trading
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

import net.openhft.lang.Jvm;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;

public class IndexedChronicleLargeFileTest extends IndexedChronicleTestBase {

	private static byte[] generateByteArray(int dataSize) throws UnsupportedEncodingException {
		byte[] result = new byte[dataSize];
		for (int i = 0; i < dataSize; i++) {
			result[i] = (byte) (i % 128);
		}
		return result;
	}

    private static void close(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException e) {
                LOGGER.warn("",e);
            }
        }
    }

    private static void close(ExcerptCommon excerpt) {
        if (excerpt != null) {
            excerpt.close();
        }
    }

	@Test
	public void testLargeFile() throws Exception {
		Assume.assumeTrue(Jvm.is64Bit());

        String basePath = getTestPath();

		Chronicle indexedChronicle = null;
		ExcerptAppender appender = null;
		Excerpt excerpt = null;

		final int dataSize = 1024;
		final long kilo = 1024L;

		try {
			final byte[] dataToWrite = generateByteArray(dataSize);
            final byte[] readBuffer = new byte[dataSize];

			indexedChronicle = ChronicleQueueBuilder.indexed(basePath).build();

			// create appender and write data to file
			// write > 4M times, each time 1K is written
			// i.e. file is > 4 GB
			appender = indexedChronicle.createAppender();
			long numberOfTimes = (kilo * kilo * 4L) + kilo;
			for (long i = 0; i < numberOfTimes; i++) {
				appender.startExcerpt(dataSize);
				appender.write(dataToWrite);
				appender.finish();
			}

			// create excerpt and read back data
			excerpt = indexedChronicle.createExcerpt();
			long index = 0;
			while (excerpt.nextIndex()) {
				int bytesRead = excerpt.read(readBuffer);
				excerpt.finish();
				// use Arrays.equals() to avoid using extremely slow Assert.assertArrayEquals()
				if (bytesRead != dataSize || !Arrays.equals(dataToWrite, readBuffer)) {
					Assert.fail("Array not equal at index " + index + "\r\n"
							+ "bytes read: " + bytesRead + "\r\n"
							+ "expected : " + Arrays.toString(dataToWrite) + "\r\n"
							+ "actual : " + Arrays.toString(readBuffer));
				}

				index++;
			}
		} finally {
			close(appender);
			close(excerpt);
			close(indexedChronicle);

			excerpt = null;
			appender = null;
			indexedChronicle = null;

            assertClean(basePath);
		}
	}
}
