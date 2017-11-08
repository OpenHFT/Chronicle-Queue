/*
 * Copyright 2016 higherfrequencytrading.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package net.openhft.chronicle.queue;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;

import org.junit.Test;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.HeapBytesStore;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.wire.DocumentContext;

/**
 * This tests demonstrates that when "peeking" data (using tailer with direction none), if we peek data over a roll cycle, the returned data is
 * not at the right index when teh chronicle file is rolled and calling tailer.readingDocument() the returned index is the same twice. The
 * implementation works perfectly fine with the RollCycles.MINUTELY and fails with RollCycles.TEST_SECONDLY. If you run the test with
 * RollCycles.MINUTELY during a minute switch it will also fail.
 */
public class ChronicleRollingIssueWithDirectionsTest {

	@Test
	public void runTestWithMinuteRoll() {
		// will work
		runTestWithRollCycleStrategy(RollCycles.MINUTELY, 250, 8);
	}

	@Test
	public void runTestWithSecondRoll() {
		// will crash
		runTestWithRollCycleStrategy(RollCycles.TEST_SECONDLY, 250, 8);
	}

	@Test
	public void runTestWithMinuteRollDuringAMinuteSwitch() throws InterruptedException {
		// will crash
		long now = System.currentTimeMillis();
		Calendar c = GregorianCalendar.getInstance();
		c.set(Calendar.MILLISECOND, 0);
		c.set(Calendar.SECOND, 0);
		c.add(Calendar.MINUTE, 1);
		long timeInAMinute = c.getTimeInMillis();
		long timeToWait = timeInAMinute - now - 500; // start test 500 ms before time switch
		System.out.println("Waiting for " + timeToWait + " ms for next minute to begin");
		Thread.sleep(timeToWait);
		System.out.println("Starting test ");
		runTestWithRollCycleStrategy(RollCycles.MINUTELY, 250, 8); // test should last 4 sec max
	}

	void runTestWithRollCycleStrategy(RollCycles cycles, long sleepTimeAfterOffers, int testsIterations) {
		String path = OS.TARGET + "/" + getClass().getSimpleName() + "-" + System.nanoTime();
		final ChronicleQueue writeQueue = ChronicleQueueBuilder.single(path).testBlockSize()
				.rollCycle(cycles).build();
		ExcerptTailer tailer = writeQueue.createTailer();

		// generate some data to put into chronicle
		byte[][] datas = new byte[10][];
		for (int i = 0; i < datas.length; i++) {
			datas[i] = ("some data " + i).getBytes();
		}
		for (int j = 0; j < testsIterations; j++) {
			for (int i = 0; i < datas.length; i++) {
				offer(datas[i], writeQueue);
			}
			// pause is important to make sure we
			Jvm.pause(sleepTimeAfterOffers);
			for (int i = 0; i < datas.length; i++) {
				byte[] peekedData = peek(tailer);
				String actual = new String(peekedData);
				String expected = ("some data " + i);
				org.junit.Assert.assertEquals("Peek failed at iteration " + j + ":" + i, expected, actual);
				if (i % 2 == 0) {
					byte[] polledData = poll(tailer);
					assertTrue("Polled data (" + polledData.length + ") different from expected data ("
							+ datas[i].length + ")", Arrays.equals(datas[i], polledData));
				} else {
					assertTrue(skip(tailer));
				}
			}
		}
	}

	void offer(byte[] data, ChronicleQueue q) {
		ExcerptAppender a = q.acquireAppender();
		HeapBytesStore<byte[]> heapBytesStore = new HeapBytesStore<>();
		heapBytesStore.init(data);
		a.writeBytes(heapBytesStore);
		heapBytesStore.uninit();
	}

	byte[] poll(ExcerptTailer tailer) {
		byte[] result = null;
		ensureTailerForwardDirection(tailer);
		try (DocumentContext dc = tailer.readingDocument()) {
			if (dc.isPresent()) {
				result = readDocument(dc);
			}
		}
		return result;
	}

	boolean skip(ExcerptTailer tailer) {
		ensureTailerForwardDirection(tailer);
		try (DocumentContext dc = tailer.readingDocument()) {
			if (dc.isPresent()) {
				return true;
			}
		}
		return false;
	}

	byte[] peek(ExcerptTailer tailer) {
		if (!tailer.direction().equals(TailerDirection.NONE)) {
			tailer.direction(TailerDirection.NONE);
		}
		try (DocumentContext dc = tailer.readingDocument()) {
			if (dc.isPresent()) {
				System.out.println(dc.index());
				return readDocument(dc);
			}
		}
		return null;
	}

	byte[] readDocument(DocumentContext dc) {
		Bytes<?> bytes = dc.wire().bytes();
		byte[] result = new byte[(int) bytes.readRemaining()];
		bytes.read(result);
		return result;
	}

	void ensureTailerForwardDirection(ExcerptTailer tailer) {
		if (tailer.direction().equals(TailerDirection.NONE)) {
			tailer.direction(TailerDirection.BACKWARD);
			tailer.direction(TailerDirection.FORWARD);
		}
	}
}