/*
 * Copyright 2014 Higher Frequency Trading
 *
 *       https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.queue.main;

import net.openhft.chronicle.queue.ChronicleHistoryReaderMain;
import org.jetbrains.annotations.NotNull;

/**
 * Reads @see MessageHistory from a queue and outputs histograms for
 * <ul>
 * <li>latencies for each component that has processed a message</li>
 * <li>latencies between each component that has processed a message</li>
 * </ul>
 *
 * @author Jerry Shea
 *
 */
public final class HistoryMain {

    public static void main(@NotNull String[] args) {
        ChronicleHistoryReaderMain.main(args);
    }
}
