/*
 * Copyright 2016-2025 chronicle.software
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

package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.rollcycles.TestRollCycles;
import org.junit.Test;

import java.io.File;
import java.time.LocalDate;
import java.time.temporal.WeekFields;
import java.util.Locale;

import static org.junit.Assert.*;

public class RollingResourcesCacheTest extends QueueTestCommon {

    @Test
    public void resourceLookupIsCached() {
        final File dir = getTmpDir();
        final RollingResourcesCache cache = newCache(dir);

        final RollingResourcesCache.Resource first = cache.resourceFor(0);
        final RollingResourcesCache.Resource repeat = cache.resourceFor(0);
        final RollingResourcesCache.Resource next = cache.resourceFor(1);

        assertSame("Expected identical instance for cached cycle", first, repeat);
        assertNotSame("Different cycle should produce a new resource", first, next);

        final int firstCount = cache.parseCount(first.text);
        final int cachedCount = cache.parseCount(first.text);
        assertEquals(firstCount, cachedCount);
    }

    @Test
    public void toLongCachesAndClearsWhenFull() {
        final File dir = getTmpDir();
        final RollingResourcesCache cache = newCache(dir);

        final RollingResourcesCache.Resource base = cache.resourceFor(0);
        final File baseFile = new File(dir, base.text + ".cq4");
        final Long initial = cache.toLong(baseFile);
        assertNotNull(initial);

        // Populate with enough unique entries to trigger cache eviction logic
        for (int i = 1; i < 40; i++) {
            final RollingResourcesCache.Resource resource = cache.resourceFor(i);
            final File file = new File(dir, resource.text + ".cq4");
            assertNotNull(cache.toLong(file));
        }

        final Long afterEviction = cache.toLong(baseFile);
        assertEquals(initial, afterEviction);
    }

    @Test
    public void parseWeeklyFormatValid() {
        final File dir = getTmpDir();
        final RollingResourcesCache cache = new RollingResourcesCache(
                RollCycles.WEEKLY,
                0,
                name -> new File(dir, name + ".cq4"),
                file -> file.getName().replaceFirst("\\.cq4$", "")
        );

        final String name = "2025W01"; // week-based year and week
        final int parsed = cache.parseCount(name);

        final WeekFields wf = WeekFields.of(Locale.getDefault());
        final LocalDate ld = LocalDate.now()
                .withYear(2025)
                .with(wf.weekOfYear(), 1)
                .with(wf.dayOfWeek(), 1);
        final int expected = Math.toIntExact(ld.toEpochDay());

        assertEquals(expected, parsed);
        // Re-parsing should use lastParseCount cache
        assertEquals(parsed, cache.parseCount(name));
    }

    @Test(expected = RuntimeException.class)
    public void parseInvalidFormatThrows() {
        final File dir = getTmpDir();
        final RollingResourcesCache cache = new RollingResourcesCache(
                TestRollCycles.TEST_SECONDLY,
                0,
                name -> new File(dir, name + ".cq4"),
                file -> file.getName().replaceFirst("\\.cq4$", "")
        );

        cache.parseCount("not-a-valid-name");
    }

    private RollingResourcesCache newCache(File dir) {
        return new RollingResourcesCache(
                TestRollCycles.TEST_SECONDLY,
                0,
                name -> new File(dir, name + ".cq4"),
                file -> file.getName().replaceFirst("\\.cq4$", "")
        );
    }
}
