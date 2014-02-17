chronicle-slf4j
===============

- levels:
  - trace
  - debug
  - info
  - warn
  - error

```properties
# logger : root
slf4j.chronicle.path      = ${java.io.tmpdir}/chronicle/${today}/${pid}/root
slf4j.chronicle.level     = debug
slf4j.chronicle.shortName = false
slf4j.chronicle.append    = false

# logger : Logger1
slf4j.chronicle.logger.Logger1.path = ${java.io.tmpdir}/chronicle/${today}/${pid}/logger_1
slf4j.chronicle.logger.Logger1.level = info
```