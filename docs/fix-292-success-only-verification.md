# FIX-292: JUnit owns the test outcome

`QueueTestCommon` always attempts owned cleanup and restores global test state.
Its outer JUnit 4 `Verifier` runs resource assertions only when setup, body,
teardown and inner rules (including `ErrorCollector`, expected exceptions and
timeouts) succeed. An outer `TestWatcher.finished` resets fixture state for
every outcome. Jupiter tests inherit an equivalent lifecycle extension using
`ExtensionContext.getExecutionException()` after teardown and inner extensions;
the extension also runs the setup that Jupiter does not inherit from JUnit 4.

Unexpected operational warnings are checked before closeable, reference and
thread assertions. Passing tests must still release their resources. Failed
or aborted tests skip verification but still run cleanup, delete owned paths,
reset the clock, exception handlers and tracing. Cleanup failures remain
failures, including after aborts; existing failures retain cleanup failures.

The real JUnit engine contracts cover body/setup/teardown failures, cleanup
failures, aborts, native-reference leaks, warnings, expected and collected
exceptions, timeouts, nested Jupiter tests and a following clean test.

## Retiring finishedNormally

All normal test assignments and fixture initialisation have been removed.
`finishedNormally` and `assumeFinishedNormally()` are inert, deprecated bridges
for already compiled consumers of the published test JAR. They no longer affect
verification. Remove both in a coordinated test-JAR API update after downstream
consumers have been rebuilt and qualified. Do not recreate per-test flags.

Removing the flag exposed `QueueLockTest.testRecover`: it caught its expected
exception but left the flag false, bypassing verification even though JUnit
reported success. The test now requires the specific warning from the original
writer releasing its forcibly recovered lock, matching WARN level, the emitting
class, the complete message including this test's metadata-file path and no
throwable, alongside the existing forced-unlock expectation. Other warnings and resource failures remain errors.

## Supporting migration changes

The Jupiter engine and platform launcher dependencies are test-scoped: they
provide explicit engine execution and real-engine contract tests. Cleanup may
now run after setup fails before the target directory allow-list exists, so
hugetlbfs cleanup checks that the allow-list was initialised before using it.
This avoids replacing the setup failure with a null-pointer exception.

The lock-warning expectation belongs to the migration because the old flag
silently bypassed it on a JUnit-successful test. Its complete path match limits
it to the queue owned by this recovery test. A warning-format change causes
verification to fail and require review; it does not widen the allowance.
