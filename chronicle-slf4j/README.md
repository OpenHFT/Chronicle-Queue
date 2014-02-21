chronicle-slf4j
===============
Simple implementation of Logger that sends all enabled log messages, for all
defined loggers, to one or more VanillaChronicle.

---

To configure this sl4j binding you need to specify the location of a properties files via system properties:
```
-Dslf4j.chronicle.properties=
```

The following properties are supported to configure the behavior of this logger:
  * slf4j.chronicle.path - the base target of a VanillaChronicle
  * slf4j.chronicle.level - the default log level for all instances of ChdonicleLogger. Must be one of ("trace", "debug", "info", "warn", or "error"). If not specified, defaults to "info".
  * slf4j.chronicle.shortName - Set to true if you want the last component of the name to be included in output messages
  * slf4j.chronicle.append. Must be one of ("true","false")
  * slf4j.chronicle.type. Must be one of ("binary","text")

Every entry starting with slf4j.chronicle.logger can be used to customize the behavior of a specific logger using _path_,_level_,_shortName_,_append_,_type_ as above.

```properties
# default
slf4j.chronicle.base = ${java.io.tmpdir}/chronicle/${today}/${pid}

# logger : root
slf4j.chronicle.path      = ${slf4j.chronicle.base}/root
slf4j.chronicle.level     = debug
slf4j.chronicle.shortName = false
slf4j.chronicle.append    = false
slf4j.chronicle.type      = binary

# logger : Logger1
slf4j.chronicle.logger.Logger1.path = ${slf4j.chronicle.base}/logger_1
slf4j.chronicle.logger.Logger1.level = info

# logger : TextLogger
slf4j.chronicle.logger.TextLogger.path        = ${slf4j.chronicle.base}/text
slf4j.chronicle.logger.TextLogger.level       = debug
slf4j.chronicle.logger.TextLogger.type        = text
slf4j.chronicle.logger.TextLogger.dateFormat  = yyyyMMdd-HHmmss-S
```

###Notes



###Availablility
This module will be available on maven central as

```xml
<dependency>
    <groupId>net.openhft</groupId>
    <artifactId>chronicle-slfj</artifactId>
    <version>2.0.4</version>
</dependency>
```
    