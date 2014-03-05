/*
 * Copyright 2013 Peter Lawrey
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.sandbox;

class JournalFile {
    // format
    // long magic number
    static final byte[] MAGIC_NUMBER = "JOURNAL1".getBytes();
    private static final int MAGIC_OFFSET = 0;
    // int allowedReaderWriters
    private static final int ALLOWED_READER_WRITERS = MAGIC_OFFSET + 8;
    // int last reader writer
    private static final int LAST_READER_WRITER = ALLOWED_READER_WRITERS + 4;
    // int record size
    private static final int RECORD_SIZE = LAST_READER_WRITER + 4;
    // int record number
    private static final int RECORD_NUMBER = RECORD_SIZE + 4;
    // int nextAllocate
    static final int NEXT_ALLOCATE = RECORD_NUMBER + 4;

    // -- new cache line for each writer
    // int state
    private static final int STATE = 0;
    // int writingTo
    private static final int WRITING_TO = STATE + 4;
    // int processId
    static final int PROCESS_ID = WRITING_TO + 4;

    // -- new cache line for each reader
    // int state
    // int readingFrom
    static final int READING_FROM = STATE + 4;
    // int processId
}
