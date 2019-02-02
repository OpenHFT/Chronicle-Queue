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

package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.core.Maths;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.annotation.UsedViaReflection;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.onoes.ExceptionHandler;
import net.openhft.chronicle.core.onoes.Slf4jExceptionHandler;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;

import java.io.EOFException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/*
 * Created by Peter Lawrey on 21/05/16.
 */
public class TimedStoreRecovery extends AbstractMarshallable implements StoreRecovery, Demarshallable {
    public static final StoreRecoveryFactory FACTORY = TimedStoreRecovery::new;
    private final LongValue timeStamp;

    @UsedViaReflection
    public TimedStoreRecovery(@NotNull WireIn in) {
        timeStamp = in.read("timeStamp").int64ForBinding(in.newLongReference());
    }

    public TimedStoreRecovery(@NotNull WireType wireType) {
        timeStamp = wireType.newLongReference().get();
    }

    @NotNull
    private static ExceptionHandler warn() {
        // prevent a warning to be logged back to the same queue potentially corrupting it.
        // return Jvm.warn();
        return Slf4jExceptionHandler.WARN;
    }

    @Override
    public void writeMarshallable(@NotNull WireOut out) {
        out.write("timeStamp").int64forBinding(0);
    }

    @SuppressWarnings("resource")
    @Override
    public long recoverAndWriteHeader(@NotNull Wire wire, long timeoutMS, final LongValue lastPosition, Sequence sequence) throws UnrecoverableTimeoutException {
        Bytes<?> bytes = wire.bytes();

        long offset = bytes.writePosition();
        int num = bytes.readVolatileInt(offset);

        // header number is only updated after successful write
        final long targetHeaderNumber = wire.headerNumber() + 1;
        String msgStart = "Unable to write a header at header number: 0x" + Long.toHexString(targetHeaderNumber) + " position: " + offset;
        if (Wires.isNotComplete(num)) {
            // TODO Determine what the safe size should be.
            int sizeToSkip = 32 << 10;
            if (bytes instanceof MappedBytes) {
                MappedBytes mb = (MappedBytes) bytes;
                sizeToSkip = Maths.toUInt31(mb.mappedFile().overlapSize() / 2);
            }
            // pad to a 4 byte word.
            sizeToSkip = (sizeToSkip + 3) & ~3;
            sizeToSkip -= (int) (offset & 3);

            // clearing the start of the data so the meta data will look like 4 zero values with no event names.
            long pos = bytes.writePosition();
            try {
                bytes.writeSkip(4);
                final String debugMessage = "!! Skipped due to recovery of locked header !! By thread " +
                        Thread.currentThread().getName() + ", pid " + OS.getProcessId();
                wire.getValueOut().text(debugMessage);
                final StringWriter stackVisitor = new StringWriter();
                new RuntimeException().printStackTrace(new PrintWriter(stackVisitor));
                final String stackTrace = stackVisitor.toString();
                // ensure there is enough space to record a stack trace for debugging purposes
                if (debugMessage.length() + stackTrace.length() + 16 < sizeToSkip) {
                    wire.getValueOut().text(stackTrace);
                }
                wire.addPadding(Math.toIntExact(sizeToSkip + (pos + 4) - bytes.writePosition()));
            } finally {
                bytes.writePosition(pos);
            }

            int emptyMetaData = Wires.META_DATA | sizeToSkip;
            if (bytes.compareAndSwapInt(offset, num, emptyMetaData)) {
                warn().on(getClass(), msgStart + " switching to a corrupt meta data message");
                bytes.writeSkip(sizeToSkip + 4);
            } else {
                int num2 = bytes.readVolatileInt(offset);
                warn().on(getClass(), msgStart + " already set to " + Integer.toHexString(num2));
            }

        } else {
            warn().on(getClass(), msgStart + " but message now exists.");
        }

        try {
            return wire.writeHeaderOfUnknownLength(timeoutMS, TimeUnit.MILLISECONDS, lastPosition, sequence);

        } catch (TimeoutException e) {
            warn().on(getClass(), e);
            // Could happen if another thread recovers, writes 2 messages but the second one is corrupt.
            return recoverAndWriteHeader(wire, timeoutMS, lastPosition, sequence);

        } catch (EOFException e) {
            throw new AssertionError(e);
        }
    }

    @Override
    public void close() {
        Closeable.closeQuietly(timeStamp);
    }
}
