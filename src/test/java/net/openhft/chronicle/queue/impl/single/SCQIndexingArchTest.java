/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaField;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;

class SCQIndexingArchTest {

    private static final Map<String, Class<?>> REQUIRED_ATOMIC_FIELDS;

    static {
        REQUIRED_ATOMIC_FIELDS = new HashMap<String, Class<?>>();
        REQUIRED_ATOMIC_FIELDS.put("linearScanCount", AtomicInteger.class);
        REQUIRED_ATOMIC_FIELDS.put("linearScanByPositionCount", AtomicInteger.class);
        REQUIRED_ATOMIC_FIELDS.put("lastScannedIndex", AtomicLong.class);
    }

    @Test
    void diagnosticCountersMustBeAtomic() {
        JavaClasses importedClasses = new ClassFileImporter().importClasses(SCQIndexing.class);

        classes().that().haveSimpleName("SCQIndexing")
                .should(new ArchCondition<JavaClass>("have atomic diagnostic counters") {
                    @Override
                    public void check(JavaClass javaClass, ConditionEvents events) {
                        for (Map.Entry<String, Class<?>> entry : REQUIRED_ATOMIC_FIELDS.entrySet()) {
                            String fieldName = entry.getKey();
                            Class<?> expectedType = entry.getValue();
                            try {
                                JavaField field = javaClass.getField(fieldName);
                                if (!field.getRawType().isEquivalentTo(expectedType)) {
                                    events.add(SimpleConditionEvent.violated(field,
                                            fieldName + " must be " + expectedType.getSimpleName()
                                                    + " but was " + field.getRawType().getName()));
                                }
                            } catch (IllegalArgumentException e) {
                                events.add(SimpleConditionEvent.violated(javaClass,
                                        "field " + fieldName + " not found in SCQIndexing"));
                            }
                        }
                    }
                }).check(importedClasses);
    }

    @Test
    void sequenceForPositionMustNotCallGetSecondaryAddress() {
        JavaClasses importedClasses = new ClassFileImporter().importClasses(SCQIndexing.class);

        classes().that().haveSimpleName("SCQIndexing")
                .should(new ArchCondition<JavaClass>("not call getSecondaryAddress from sequenceForPosition") {
                    @Override
                    public void check(JavaClass javaClass, ConditionEvents events) {
                        javaClass.getMethods().stream()
                                .filter(m -> m.getName().equals("sequenceForPosition"))
                                .flatMap(m -> m.getMethodCallsFromSelf().stream())
                                .filter(call -> call.getTarget().getName().equals("getSecondaryAddress"))
                                .forEach(call -> events.add(SimpleConditionEvent.violated(call,
                                        "sequenceForPosition must use getSecondaryAddressReadOnly, " +
                                        "not getSecondaryAddress (write-on-read bug)")));
                    }
                }).check(importedClasses);
    }

    @Test
    void searchHelperMethodsMustExist() {
        JavaClasses importedClasses = new ClassFileImporter().importClasses(SCQIndexing.class);
        List<String> requiredHelpers = Arrays.asList(
                "findBestSecondarySlotForPosition",
                "findBestSecondarySlotByReverseScan",
                "toSequenceIndex"
        );

        classes().that().haveSimpleName("SCQIndexing")
                .should(new ArchCondition<JavaClass>("have extracted search helper methods") {
                    @Override
                    public void check(JavaClass javaClass, ConditionEvents events) {
                        Set<String> methodNames = javaClass.getMethods().stream()
                                .map(m -> m.getName())
                                .collect(Collectors.toSet());
                        for (String helper : requiredHelpers) {
                            if (!methodNames.contains(helper)) {
                                events.add(SimpleConditionEvent.violated(javaClass,
                                        "SCQIndexing must contain helper method " + helper));
                            }
                        }
                    }
                }).check(importedClasses);
    }
}
