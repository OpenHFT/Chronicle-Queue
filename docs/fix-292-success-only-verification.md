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
writer releasing its forcibly recovered lock, alongside the existing forced
unlock expectation. Other warnings and resource failures remain errors.
