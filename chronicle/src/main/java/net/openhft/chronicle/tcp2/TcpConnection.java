/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.tcp2;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

public interface TcpConnection {
    public boolean open() throws IOException;
    public boolean close() throws IOException;
    public boolean isOpen();

    public boolean write(ByteBuffer buffer) throws IOException;
    public void writeAllOrEOF(ByteBuffer bb) throws IOException;
    public void writeAll(ByteBuffer bb) throws IOException;

    public boolean read(ByteBuffer buffer, int size) throws IOException ;
    public boolean read(ByteBuffer buffer, int threshod, int size) throws IOException;
}
