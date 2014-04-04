chronicle-slf4j
===============
Simple implementation of Logger that sends all enabled log messages, for all defined loggers, to one or more VanillaChronicle.

===

To configure this sl4j binding you need to specify the location of a properties files via system properties:
```
-Dslf4j.chronicle.properties=${pathOfYourPropertiesFile}
```

The following properties are supported to configure the behavior of the logger:

 **Property** | **Description**                          | **Values**                       | **Per-Logger**
--------------|------------------------------------------|----------------------------------|----------------
type          | the type of the underlying Chronicle     | indexed, vanilla                 | no
path          | the base directory of a Chronicle        |                                  | yes
level         | the default log level                    | trace, debug, info, warn, error  | yes
append        |                                          | true, false                      | yes
format        | write log as text or binary              | binary, text                     | yes
binaryFormat  | format or serialize log arguments        | formatted, serialized            | no
dateFormat    | the date format for text loggers         |                                  | no 
synchronous   | synchronous mode                         | true, false                      | yes

The default configuration needs slf4j.chronicle as prefix but you can set per-logger settings using slf4j.chronicle.logger as prefix, here an example:

```properties
# default
slf4j.chronicle.base         = ${java.io.tmpdir}/chronicle/${today}/${pid}

# logger : root
slf4j.chronicle.type         = vanilla
slf4j.chronicle.path         = ${slf4j.chronicle.base}/main
slf4j.chronicle.level        = debug
slf4j.chronicle.shortName    = false
slf4j.chronicle.append       = false
slf4j.chronicle.format       = binary
slf4j.chronicle.binaryFormat = formatted

# logger : Logger1
slf4j.chronicle.logger.Logger1.path           = ${slf4j.chronicle.base}/logger_1
slf4j.chronicle.logger.Logger1.level          = info

# logger : TextLogger
slf4j.chronicle.logger.TextLogger.path        = ${slf4j.chronicle.base}/text
slf4j.chronicle.logger.TextLogger.level       = debug
slf4j.chronicle.logger.TextLogger.format      = text
slf4j.chronicle.logger.TextLogger.dateFormat  = yyyyMMdd-HHmmss-S
```


The configuration of chronicle-slf4j supports variable interpolation where the variables are replaced with the corresponding values from the same configuration file, the system properties and from some predefined values. System properties have the precedence in placeholder replacement so one can override a value via system properties.

Predefined values are:
  * pid which will replaced by the process id
  * today wich will be replaced by the current date (yyyyMMdd)

###Notes
  * Loggers are not hierarchical grouped so my.domain.package.MyClass1 and my.domain are two distinct entities.
  * The _path_ is used to track the underlying VanillaChronicle so two loggers configured with the same _path_ will share the same Chronicle  


###Availablility
This module will be available on maven central as

```xml
<dependency>
    <groupId>net.openhft</groupId>
    <artifactId>chronicle-slfj</artifactId>
    <version>3.0b</version>
</dependency>
```
    
