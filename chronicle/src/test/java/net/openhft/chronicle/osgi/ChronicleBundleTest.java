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

package net.openhft.chronicle.osgi;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.ops4j.pax.exam.CoreOptions.*;

/**
 * @author lburgazzoli
 *         <p/>
 *         Thank for adding OSGi testing to Chronicle.
 */
@RunWith(PaxExam.class)
public class ChronicleBundleTest {
    @Inject
    BundleContext context;

    @Configuration
    public Option[] config() {
        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.INFO);

        return options(
                //systemProperty("org.osgi.framework.storage").value("/tmp/felix-cache"),
                systemProperty("org.osgi.framework.storage.clean").value("true"),
                systemProperty("org.ops4j.pax.logging.DefaultServiceLog.level").value("WARN"),
                mavenBundle("net.openhft", "lang", "6.1.1"),
                mavenBundle("net.openhft", "chronicle", "2.0.2"),
                junitBundles(),
                systemPackage("sun.misc"),
                systemPackage("sun.nio.ch"),
                cleanCaches()
        );
    }

    @Test
    public void checkInject() {
        assertNotNull(context);
    }

    @Test
    public void checkHelloBundle() {
        Boolean bundleChronicleFound = false;
        Boolean bundleChronicleActive = false;

        Bundle[] bundles = context.getBundles();
        for (Bundle bundle : bundles) {
            if (bundle != null) {
                if (bundle.getSymbolicName().equals("net.openhft.chronicle")) {
                    bundleChronicleFound = true;
                    if (bundle.getState() == Bundle.ACTIVE) {
                        bundleChronicleActive = true;
                    }
                }
            }
        }

        assertTrue(bundleChronicleFound);
        assertTrue(bundleChronicleActive);
    }
}
