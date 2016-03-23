/*
 *
 *  *     Copyright (C) ${YEAR}  higherfrequencytrading.com
 *  *
 *  *     This program is free software: you can redistribute it and/or modify
 *  *     it under the terms of the GNU Lesser General Public License as published by
 *  *     the Free Software Foundation, either version 3 of the License.
 *  *
 *  *     This program is distributed in the hope that it will be useful,
 *  *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  *     GNU Lesser General Public License for more details.
 *  *
 *  *     You should have received a copy of the GNU Lesser General Public License
 *  *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package net.openhft.chronicle.queue;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by peter on 15/03/16.
 */
public class ReadmeTest {
    @Test
    public void createAQueue() {
        try (ChronicleQueue queue = ChronicleQueueBuilder.single("queue-dir").build()) {
            // Obtain an ExcerptAppender
            ExcerptAppender appender = queue.createAppender();

            // write - {msg: TestMessage}
            appender.writeDocument(w -> w.write(() -> "msg").text("TestMessage"));

            // write - TestMessage
            appender.writeText("TestMessage");

            ExcerptTailer tailer = queue.createTailer();

            tailer.readDocument(w -> System.out.println("msg: " + w.read(() -> "msg").text()));

            assertEquals("TestMessage", tailer.readText());
        }
    }
}
