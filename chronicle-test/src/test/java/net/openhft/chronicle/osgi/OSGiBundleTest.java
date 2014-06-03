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

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;

import javax.inject.Inject;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.ops4j.pax.exam.CoreOptions.*;

/**
 * @author lburgazzoli
 */
//@Ignore
@RunWith(PaxExam.class)
public class OSGiBundleTest extends OSGiTestBase {
    @Inject
    BundleContext context;

    @Configuration
    public Option[] config() {
        return options(
            systemProperty("org.osgi.framework.storage.clean").value("true"),
            systemProperty("org.ops4j.pax.logging.DefaultServiceLog.level").value("WARN"),
            mavenBundleAsInProject("org.slf4j"  ,"slf4j-api"),
            mavenBundleAsInProject("org.slf4j"  ,"slf4j-simple").noStart(),
            mavenBundleAsInProject("net.openhft","affinity"),
            mavenBundleAsInProject("net.openhft","compiler"),
            mavenBundleAsInProject("net.openhft","lang"),
            mavenBundleAsInProject("net.openhft","chronicle"),
            workspaceBundle("chronicle-test"),
            junitBundles(),
            systemPackage("sun.misc"),
            systemPackage("sun.nio.ch"),
            systemPackage("com.sun.jna"),
            systemPackage("com.sun.jna.ptr"),
            systemPackage("com.sun.tools.javac.api"),
            cleanCaches()
        );
    }

    @Test
    public void checkInject() {
        assertNotNull(context);
    }

    @Test
    @Ignore
    public void checkBundle() {
        Boolean bundleFound = false;

        Bundle[] bundles = context.getBundles();
        for (Bundle bundle : bundles) {
            if (bundle != null) {
                if (bundle.getSymbolicName().equals("net.openhft.chronicle")) {
                    bundleFound = true;
                    assertEquals(bundle.getState(),Bundle.ACTIVE);
                }
            }
        }

        assertTrue(bundleFound);
    }
}
