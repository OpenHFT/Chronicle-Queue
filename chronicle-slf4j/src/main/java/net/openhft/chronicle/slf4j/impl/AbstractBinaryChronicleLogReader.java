/*
 * Copyright 2014 Peter Lawrey
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
package net.openhft.chronicle.slf4j.impl;

import net.openhft.lang.io.Bytes;

import java.util.Date;

/**
 *
 */
public abstract class AbstractBinaryChronicleLogReader extends AbstractChronicleLogReader {
    @Override
    public void read(Bytes bytes) {
        Date ts = new Date(bytes.readLong());
        int level = bytes.readByte();
        long tid = bytes.readLong();
        String tname = bytes.readEnum(String.class);
        String name = bytes.readEnum(String.class);
        String msg = bytes.readEnum(String.class);
        int nbargs = bytes.readInt();
        Object[] args = nbargs > 0 ? new Object[nbargs] : ChronicleLogWriters.NULL_ARGS;

        for (int i = 0; i < nbargs; i++) {
            args[i] = bytes.readObject();
        }

        this.process(ts, level, tid, tname, name, msg, args);
    }

    @Override
    public void process(String message) {
        throw new UnsupportedOperationException();
    }
}
