chronicle-slf4j
===============

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

###Tools

  * net.openhft.chronicle.slf4j.tools.ChroniTail
  ```
    ChroniTail [-t|-i] path
        -t = text chronicle, default binary
        -i = IndexedCronicle, default VanillaChronicle

    mvn exec:java -Dexec.mainClass="net.openhft.chronicle.slf4j.tools.ChroniTail" -Dexec.args="..."
  ```

  * net.openhft.chronicle.slf4j.tools.ChroniCat
  ```
      ChroniCat [-t|-i] path
        -t = text chronicle, default binary
        -i = IndexedCronicle, default VanillaChronicle

      mvn exec:java -Dexec.mainClass="net.openhft.chronicle.slf4j.tools.ChroniCat" -Dexec.args="..."
  ```

  * net.openhft.chronicle.slf4j.tools.ChroniGrep
  ```
      ChroniCat [-t|-i] regexp1 ... regexpN path
        -t = text chronicle, default binary
        -i = IndexedCronicle, default VanillaChronicle

      mvn exec:java -Dexec.mainClass="net.openhft.chronicle.slf4j.tools.ChroniCat" -Dexec.args="..."
  ```


Output:
  ```
  2014.04.06-13:30:03.306|debug|1|th-test-logging_1|readwrite|debug
  2014.04.06-13:30:03.313|info|1|th-test-logging_1|readwrite|info
  2014.04.06-13:30:03.314|warn|1|th-test-logging_1|readwrite|warn
  2014.04.06-13:30:03.314|error|1|th-test-logging_1|readwrite|error
  ```


###Writing a simple LogSearch with Groovy and Grape

  * Binary log search
  ```groovy
  import net.openhft.chronicle.VanillaChronicle
  import net.openhft.chronicle.IndexedChronicle
  import net.openhft.chronicle.slf4j.ChronicleLogProcessor
  import net.openhft.chronicle.slf4j.tools.ChroniTool

  @Grapes([
     @Grab(group='net.openhft', module='chronicle'      , version='3.0b-SNAPSHOT'),
     @Grab(group='net.openhft', module='chronicle-slf4j', version='3.0b-SNAPSHOT'),
  ])
  class LogSearch {
      static def main(String[] args) {
          def processor = { ts,level,thId,thName,logName,msg,msgArgs ->
              if(msg =~ '.*n.*') {
                  printf("%s => %s\n",ts,msg)
              }
          }

          try {
              if(args.length == 1) {
                  ChroniTool.process(
                      new VanillaChronicle(args[0]),
                      ChroniTool.binaryReader(processor as ChronicleLogProcessor),
                      false,
                      false)
              }
          } catch(Exception e) {
              e.printStackTrace(System.err);
          }
      }
  }
  ```

  * Text log search
  ```groovy
  import net.openhft.chronicle.VanillaChronicle
  import net.openhft.chronicle.IndexedChronicle
  import net.openhft.chronicle.slf4j.ChronicleLogProcessor
  import net.openhft.chronicle.slf4j.tools.ChroniTool

  @Grapes([
     @Grab(group='net.openhft', module='chronicle'      , version='3.0b-SNAPSHOT'),
     @Grab(group='net.openhft', module='chronicle-slf4j', version='3.0b-SNAPSHOT'),
  ])
  class LogSearch {
      static def main(String[] args) {
          def processor = { msg ->
              if(msg =~ '.*n.*') {
                  printf("%s => %s\n",ts,msg)
              }
          }

          try {
              if(args.length == 1) {
                  ChroniTool.process(
                      new VanillaChronicle(args[0]),
                      ChroniTool.binaryReader(processor as ChronicleLogProcessor),
                      false,
                      false)
              }
          } catch(Exception e) {
              e.printStackTrace(System.err);
          }
      }
  }
  ```

###Availablility
This module will be available on maven central as

```xml
<dependency>
    <groupId>net.openhft</groupId>
    <artifactId>chronicle-slfj</artifactId>
    <version>3.0b</version>
</dependency>
```
