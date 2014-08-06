/*
 * Copyright 2014 Higher Frequency Trading
 * <p/>
 * http://www.higherfrequencytrading.com
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.sandbox;

/**
 * File structure
 * 64 byte header of
 * long magic number: JOURNAL\\u001
 * int header size:      64
 * int index count:     e.g. 4096
 * int data shift:        e.g. 0
 * int data allocations: e.g. 16 M
 * padding
 * rotating indexes
 * data
 */
class VanillaJournal {
    public VanillaJournal(String fileName, JournalConfig config) {

    }
}
