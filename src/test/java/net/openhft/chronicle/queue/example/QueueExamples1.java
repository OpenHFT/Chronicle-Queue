/*
 * Copyright 2016-2022 chronicle.software
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

package net.openhft.chronicle.queue.example;

import net.openhft.chronicle.queue.ChronicleQueue;

public class QueueExamples1 {

    public static void main(String[] args) {

        ChronicleQueue queue = ChronicleQueue.single("./myQueueDir");
        Printer printer = queue.methodWriter(Printer.class);
        printer.print("hello world");
    }

    // this interface has to be deployed to both java processes
    interface Printer {
        void print(String message);
    }
}
