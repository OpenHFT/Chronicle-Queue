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

package demo;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by daniel on 19/06/2014.
 */
public interface ChronicleUpdatable {
    public void setFileNames(List<String> files);

    public void addTimeMillis(long l);

    public void incrMessageRead();

    public void incrTcpMessageRead();

    AtomicLong tcpMessageRead();

    AtomicLong count1();

    AtomicLong count2();
}
