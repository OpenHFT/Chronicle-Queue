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
package net.openhft.chronicle.slf4j.rw;

import net.openhft.chronicle.slf4j.ChronicleLogReader;
import net.openhft.lang.io.Bytes;

import java.util.Date;

/**
 *
 */
public abstract class AbstractBinaryChronicleLogReader implements ChronicleLogReader {
    @Override
    public void process(Bytes bytes) {
        Date   ts    = new Date(bytes.readLong());
        int    level = bytes.readByte();
        long   tid   = bytes.readLong();
        String tname = bytes.readEnum(String.class);
        String name  = bytes.readEnum(String.class);
        String msg   = bytes.readEnum(String.class);

        this.read(ts,level,tid,tname,name,msg,null);
    }
}
