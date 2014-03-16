chronicle-slf4j
===============
Simple implementation of Logger that sends all enabled log messages, for all defined loggers, to one or more VanillaChronicle.

---

To configure this sl4j binding you need to specify the location of a properties files via system properties:
```
-Dslf4j.chronicle.properties=${pathOfYourPropertiesFile}
```

The following properties are supported to configure the behavior of this logger:
  * slf4j.chronicle.path - the base target of a VanillaChronicle
  * slf4j.chronicle.level - the default log level for all instances of ChdonicleLogger. Must be one of ("trace", "debug", "info", "warn", or "error"). If not specified, defaults to "info".
  * slf4j.chronicle.shortName - Set to true if you want the last component of the name to be included in output messages
  * slf4j.chronicle.append. Must be one of ("true","false")
  * slf4j.chronicle.format. Must be one of ("binary","text")
  * slf4j.chronicle.dateFormat. Defines the date format for text loggers

```properties
# default
slf4j.chronicle.base      = ${java.io.tmpdir}/chronicle/${today}/${pid}

# logger : root
slf4j.chronicle.path      = ${slf4j.chronicle.base}/main
slf4j.chronicle.level     = debug
slf4j.chronicle.shortName = false
slf4j.chronicle.append    = false
slf4j.chronicle.format    = binary
```

The configuration of chronicle-slf4j supports variable interpolation where the variables are replaced with the corresponding values from the same configuration file, the system properties and from some predefined values. System properties have the precedence in placeholder replacement so one can override a value via system properties.

Predefined values are:
  * pid which will replaced by the process id
  * today wich will be replaced by the current date (yyyyMMdd)


You can setup per-logger settings using slf4j.chronicle.logger as prefix:

```properties
# logger : Logger1
slf4j.chronicle.logger.Logger1.path           = ${slf4j.chronicle.base}/logger_1
slf4j.chronicle.logger.Logger1.level          = info

# logger : TextLogger
slf4j.chronicle.logger.TextLogger.path        = ${slf4j.chronicle.base}/text
slf4j.chronicle.logger.TextLogger.level       = debug
slf4j.chronicle.logger.TextLogger.format        = text
slf4j.chronicle.logger.TextLogger.dateFormat  = yyyyMMdd-HHmmss-S
```

You can use _path_, _level_, _shortName_, _append_, _format_ as for the main logger.


###Notes
  * Loggers are not hierarchical grouped so my.domain.package.MyClass1 and my.domain are two distinct entities.
  * The _path_ is used to track the underlying VanillaChronicle so two loggers configured with the same _path_ will share the same VanillaChronicle, only _level_ and _shortName_ can be different.  


###Availablility
This module will be available on maven central as

```xml
<dependency>
    <groupId>net.openhft</groupId>
    <artifactId>chronicle-slfj</artifactId>
    <version>2.0.4</version>
</dependency>
```
    
