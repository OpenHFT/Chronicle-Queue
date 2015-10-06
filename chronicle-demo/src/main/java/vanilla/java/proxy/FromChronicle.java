/*
 * Copyright 2015 Higher Frequency Trading
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

package vanilla.java.proxy;

import net.openhft.chronicle.ExcerptTailer;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public class FromChronicle<T> {
    private final T instance;
    private final ExcerptTailer tailer;
    private final Map<String, Method> methodMap = new HashMap<>();

    public FromChronicle(T instance, ExcerptTailer tailer) {
        this.instance = instance;
        this.tailer = tailer;
        for (Method m : instance.getClass().getMethods())
            if (m.getDeclaringClass() != Object.class)
                methodMap.put(m.getName(), m);
    }

    public void readOne() {
        if (tailer.nextIndex()) {
            MetaData.get().readMarshallable(tailer);
            String metaName = tailer.readUTFΔ();
            Method m = findMethod(metaName);
            Object[] args = null;
            int len = (int) tailer.readStopBit();
            if (len > 0) {
                for (int i = 0; i < len; i++)
                    args[i] = tailer.readObject();
            }
            try {
                m.invoke(instance, args);
            } catch (IllegalAccessException e) {
                throw new AssertionError(e);
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            }
        }
    }

    private Method findMethod(String metaName) {
        return methodMap.get(metaName);
    }
}
