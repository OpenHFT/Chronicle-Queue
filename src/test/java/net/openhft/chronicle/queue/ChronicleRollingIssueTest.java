/*
 * Copyright 2016-2020 chronicle.software
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

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.annotation.RequiredForClient;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.queue.impl.StoreFileListener;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/*
java.lang.AssertionError: Closeables still open

	at net.openhft.chronicle.core.io.AbstractCloseable.assertCloseablesClosed(AbstractCloseable.java:79)
	at net.openhft.chronicle.core.io.AbstractReferenceCounted.assertReferencesReleased(AbstractReferenceCounted.java:55)
	at net.openhft.chronicle.queue.QueueTestCommon.assertReferencesReleased(QueueTestCommon.java:27)
	at net.openhft.chronicle.queue.QueueTestCommon.afterChecks(QueueTestCommon.java:70)
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.lang.reflect.Method.invoke(Method.java:498)
	at org.junit.runners.model.FrameworkMethod$1.runReflectiveCall(FrameworkMethod.java:50)
	at org.junit.internal.runners.model.ReflectiveCallable.run(ReflectiveCallable.java:12)
	at org.junit.runners.model.FrameworkMethod.invokeExplosively(FrameworkMethod.java:47)
	at org.junit.internal.runners.statements.RunAfters.evaluate(RunAfters.java:33)
	at org.junit.runners.ParentRunner.runLeaf(ParentRunner.java:325)
	at org.junit.runners.BlockJUnit4ClassRunner.runChild(BlockJUnit4ClassRunner.java:78)
	at org.junit.runners.BlockJUnit4ClassRunner.runChild(BlockJUnit4ClassRunner.java:57)
	at org.junit.runners.ParentRunner$3.run(ParentRunner.java:290)
	at org.junit.runners.ParentRunner$1.schedule(ParentRunner.java:71)
	at org.junit.runners.ParentRunner.runChildren(ParentRunner.java:288)
	at org.junit.runners.ParentRunner.access$000(ParentRunner.java:58)
	at org.junit.runners.ParentRunner$2.evaluate(ParentRunner.java:268)
	at org.junit.runners.ParentRunner.run(ParentRunner.java:363)
	at org.junit.runner.JUnitCore.run(JUnitCore.java:137)
	at com.intellij.junit4.JUnit4IdeaTestRunner.startRunnerWithArgs(JUnit4IdeaTestRunner.java:68)
	at com.intellij.rt.junit.IdeaTestRunner$Repeater.startRunnerWithArgs(IdeaTestRunner.java:53)
	at com.intellij.rt.junit.JUnitStarter.prepareStreamsAndStart(JUnitStarter.java:230)
	at com.intellij.rt.junit.JUnitStarter.main(JUnitStarter.java:58)
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.lang.reflect.Method.invoke(Method.java:498)
	at com.intellij.rt.execution.application.AppMainV2.main(AppMainV2.java:128)
	Suppressed: java.lang.IllegalStateException: Not closed BinaryTwoLongReference@733
		at net.openhft.chronicle.core.io.AbstractCloseable.assertCloseablesClosed(AbstractCloseable.java:93)
		... 30 more
	Caused by: net.openhft.chronicle.core.StackTrace: Created Here on appender-5
		at net.openhft.chronicle.core.io.AbstractCloseable.<init>(AbstractCloseable.java:53)
		at net.openhft.chronicle.bytes.ref.AbstractReference.<init>(AbstractReference.java:31)
		at net.openhft.chronicle.bytes.ref.BinaryLongReference.<init>(BinaryLongReference.java:26)
		at net.openhft.chronicle.bytes.ref.BinaryTwoLongReference.<init>(BinaryTwoLongReference.java:22)
		at net.openhft.chronicle.wire.BinaryWire.newTwoLongReference(BinaryWire.java:1212)
		at net.openhft.chronicle.queue.impl.single.SingleChronicleQueueStore.loadWritePosition(SingleChronicleQueueStore.java:146)
		at net.openhft.chronicle.queue.impl.single.SingleChronicleQueueStore.<init>(SingleChronicleQueueStore.java:74)
		at sun.reflect.GeneratedConstructorAccessor1.newInstance(Unknown Source)
		at sun.reflect.DelegatingConstructorAccessorImpl.newInstance(DelegatingConstructorAccessorImpl.java:45)
		at java.lang.reflect.Constructor.newInstance(Constructor.java:423)
		at net.openhft.chronicle.wire.Demarshallable.newInstance(Demarshallable.java:57)
		at net.openhft.chronicle.wire.BinaryWire$BinaryValueIn.demarshallable(BinaryWire.java:3347)
		at net.openhft.chronicle.wire.BinaryWire$BinaryValueIn.typedMarshallable0(BinaryWire.java:3113)
		at net.openhft.chronicle.wire.BinaryWire$BinaryValueIn.typedMarshallable(BinaryWire.java:3078)
		at net.openhft.chronicle.queue.impl.single.SingleChronicleQueue$StoreSupplier.acquire(SingleChronicleQueue.java:901)
		at net.openhft.chronicle.queue.impl.WireStorePool.acquire(WireStorePool.java:52)
		at net.openhft.chronicle.queue.impl.single.StoreAppender.setCycle2(StoreAppender.java:230)
		at net.openhft.chronicle.queue.impl.single.StoreAppender.rollCycleTo(StoreAppender.java:592)
		at net.openhft.chronicle.queue.impl.single.StoreAppender.writingDocument(StoreAppender.java:336)
		at net.openhft.chronicle.queue.impl.single.StoreAppender.writingDocument(StoreAppender.java:310)
		at net.openhft.chronicle.wire.MarshallableOut.writeMap(MarshallableOut.java:153)
		at net.openhft.chronicle.queue.ChronicleRollingIssueTest.lambda$test$0(ChronicleRollingIssueTest.java:55)
		at java.lang.Thread.run(Thread.java:748)
	Suppressed: java.lang.IllegalStateException: Not closed SingleTableStore@aa
		at net.openhft.chronicle.core.io.AbstractCloseable.assertCloseablesClosed(AbstractCloseable.java:93)
		... 30 more
	Caused by: net.openhft.chronicle.core.StackTrace: Created Here on appender-5
		at net.openhft.chronicle.core.io.AbstractCloseable.<init>(AbstractCloseable.java:53)
		at net.openhft.chronicle.queue.impl.table.SingleTableStore.<init>(SingleTableStore.java:72)
		at sun.reflect.GeneratedConstructorAccessor3.newInstance(Unknown Source)
		at sun.reflect.DelegatingConstructorAccessorImpl.newInstance(DelegatingConstructorAccessorImpl.java:45)
		at java.lang.reflect.Constructor.newInstance(Constructor.java:423)
		at net.openhft.chronicle.wire.Demarshallable.newInstance(Demarshallable.java:57)
		at net.openhft.chronicle.wire.BinaryWire$BinaryValueIn.demarshallable(BinaryWire.java:3347)
		at net.openhft.chronicle.wire.BinaryWire$BinaryValueIn.typedMarshallable0(BinaryWire.java:3113)
		at net.openhft.chronicle.wire.BinaryWire$BinaryValueIn.typedMarshallable(BinaryWire.java:3078)
		at net.openhft.chronicle.queue.impl.table.SingleTableBuilder.readTableStore(SingleTableBuilder.java:136)
		at net.openhft.chronicle.queue.impl.table.SingleTableBuilder.lambda$build$2(SingleTableBuilder.java:118)
		at net.openhft.chronicle.queue.impl.table.SingleTableStore.doWithLock(SingleTableStore.java:125)
		at net.openhft.chronicle.queue.impl.table.SingleTableStore.doWithExclusiveLock(SingleTableStore.java:112)
		at net.openhft.chronicle.queue.impl.table.SingleTableBuilder.build(SingleTableBuilder.java:113)
		at net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.initializeMetadata(SingleChronicleQueueBuilder.java:416)
		at net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.preBuild(SingleChronicleQueueBuilder.java:1017)
		at net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.build(SingleChronicleQueueBuilder.java:314)
		at net.openhft.chronicle.queue.ChronicleRollingIssueTest.lambda$test$0(ChronicleRollingIssueTest.java:46)
		at java.lang.Thread.run(Thread.java:748)
 */
@RequiredForClient
public class ChronicleRollingIssueTest extends QueueTestCommon {
    @Test
    public void test() throws InterruptedException {
        int threads = Math.min(64, Runtime.getRuntime().availableProcessors() * 4) - 1;
        int messages = 100;

        String path = OS.getTarget() + "/" + getClass().getSimpleName() + "-" + Time.uniqueId();
        AtomicInteger count = new AtomicInteger();
        StoreFileListener storeFileListener = (cycle, file) -> {
        };
        Runnable appendRunnable = () -> {
            try (final ChronicleQueue writeQueue = ChronicleQueue
                    .singleBuilder(path)
                    .testBlockSize()
                    .storeFileListener(storeFileListener)
                    .rollCycle(RollCycles.TEST_SECONDLY).build()) {
                for (int i = 0; i < messages; i++) {
                    long millis = System.currentTimeMillis() % 100;
                    if (millis > 1 && millis < 99) {
                        Jvm.pause(99 - millis);
                    }
                    ExcerptAppender appender = writeQueue.acquireAppender();
                    Map<String, Object> map = new HashMap<>();
                    map.put("key", Thread.currentThread().getName() + " - " + i);
                    appender.writeMap(map);
                    count.incrementAndGet();
                }
            }
        };

        List<Thread> threadList = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            Thread thread = new Thread(appendRunnable, "appender-" + i);
            thread.start();
            threadList.add(thread);
        }
        long start = System.currentTimeMillis();
        long lastIndex = 0;
        try (final ChronicleQueue queue = ChronicleQueue
                .singleBuilder(path)
                .testBlockSize()
                .storeFileListener(storeFileListener)
                .rollCycle(RollCycles.TEST_SECONDLY).build()) {
            ExcerptTailer tailer = queue.createTailer();
            int count2 = 0;
            while (count2 < threads * messages) {
                Map<String, Object> map = tailer.readMap();
                long index = tailer.index();
                if (map != null) {
                    count2++;
                } else if (index >= 0) {
                    if (RollCycles.TEST_SECONDLY.toCycle(lastIndex) != RollCycles.TEST_SECONDLY.toCycle(index)) {
/*
                       // System.out.println("Wrote: " + count
                                + " read: " + count2
                                + " index: " + Long.toHexString(index));
*/
                        lastIndex = index;
                    }
                }
                if (System.currentTimeMillis() > start + 60000) {
                    throw new AssertionError("Wrote: " + count
                            + " read: " + count2
                            + " index: " + Long.toHexString(index));
                }
            }
        } finally {
            for (Thread thread : threadList) {
                thread.interrupt();
                thread.join(1000);
            }
            try {
                IOTools.deleteDirWithFiles(path, 2);
            } catch (IORuntimeException todoFixOnWindows) {

            }
        }
    }
}