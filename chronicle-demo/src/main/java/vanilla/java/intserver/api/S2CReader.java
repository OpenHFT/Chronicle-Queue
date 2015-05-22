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

package vanilla.java.intserver.api;

import net.openhft.chronicle.ExcerptTailer;

public class S2CReader {
    final IClient client;

    public S2CReader(IClient client) {
        this.client = client;
    }

    public boolean readOne(ExcerptTailer tailer) {
        if (!tailer.nextIndex())
            return false;
        char ch = (char) tailer.readUnsignedByte();
        switch (ch) {
            case S2CWriter.RESPONSE: {
                int request = tailer.readInt();
                int response = tailer.readInt();
                int argsLength = tailer.readInt();
                Object[] args = new Object[argsLength];
                for (int i = 0; i < argsLength; i++)
                    args[i] = tailer.readObject();
                client.response(request, response, args);
                break;
            }

            default:
                System.err.println("Unknown command " + ch);
                break;
        }
        return true;
    }
}
