/*
 * Copyright 2014 Higher Frequency Trading
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
package net.openhft.chronicle.tools;

import net.openhft.chronicle.ExcerptCommon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.reflect.Field;

public class ExcerptPhantomReference extends PhantomReference<ExcerptCommon> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExcerptPhantomReference.class);

    protected ExcerptPhantomReference(final ExcerptCommon referent, final ReferenceQueue<ExcerptCommon> q) {
        super(referent, q);
    }

    protected void cleanup() {
        try {

            final Field f = Reference.class.getDeclaredField("referent") ;
            f.setAccessible(true) ;
            ((ExcerptCommon)(f.get(this))).close();
        } catch (Exception e) {
            LOGGER.warn("Exception", e);
        }
    }
}
