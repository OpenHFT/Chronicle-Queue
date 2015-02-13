package net.openhft.chronicle;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.TimeZone;

import org.junit.Assert;
import org.junit.Test;

public class IndexedChronicleLargeFileTest {
	private static SimpleDateFormat getSimpleDateFormat() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd_HHmmssSSS");
		sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
		return sdf;
	}

	private static void deleteDir(File dir, boolean gcAndSleepBeforeDeletion) {
		if (dir == null) {
			return;
		}
		if (gcAndSleepBeforeDeletion) {
			System.gc();
			try {
				Thread.sleep(1000);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		File[] files = dir.listFiles();
		if (files != null) {
			for (File oneSubFile : files) {
				if (oneSubFile.isDirectory()) {
					deleteDir(oneSubFile, false);
				} else {
					oneSubFile.delete();
				}
			}
		}
		dir.delete();
	}

	private static void close(Closeable closeable) {
		if (closeable != null) {
			try {
				closeable.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private static void close(Excerpt closeable) {
		try {
			if (closeable != null) {
				closeable.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void close(ExcerptAppender closeable) {
		try {
			if (closeable != null) {
				closeable.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static byte[] generateByteArray(int dataSize) throws UnsupportedEncodingException {
		byte[] result = new byte[dataSize];
		for (int i = 0; i < dataSize; i++) {
			result[i] = (byte) (i % 128);
		}
		return result;
	}

	@Test
	public void testLargeFile() throws Exception {
		Chronicle indexedChronicle = null;
		ExcerptAppender appender = null;
		Excerpt excerpt = null;
		File testDir = null;
		final int dataSize = 1024;
		final long kilo = 1024L;
		try {
			byte[] dataToWrite = generateByteArray(dataSize);
			byte[] readBuffer = new byte[dataSize];

			File systemTmpDir = new File(System.getProperty("java.io.tmpdir"));
			testDir = new File(systemTmpDir, this.getClass().getSimpleName() + "_" + getSimpleDateFormat().format(new Date()));
			testDir.mkdirs();
			String basePath = testDir.getAbsolutePath() + File.separator + "LargeFile";
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
					// @formatter:off
					Assert.fail("Array not equal at index " + index + "\r\n"
							+ "bytes read: " + bytesRead + "\r\n"
							+ "expected : " + Arrays.toString(dataToWrite) + "\r\n" 
							+ "actual : " + Arrays.toString(readBuffer));
					// @formatter:on
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
			deleteDir(testDir, true);
		}
	}
}
