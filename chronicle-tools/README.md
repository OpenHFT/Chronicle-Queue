chronicle-tools
===============

  * net.openhft.chronicle.tools.slf4j.ChroniTail
  ``` 
    ChroniTail [-t] path
        -t = text chronicle, default binary

    mvn exec:java -Dexec.mainClass="net.openhft.chronicle.tools.slf4j.ChroniTail" -Dexec.args="..."
  ```

  * net.openhft.chronicle.tools.slf4j.ChroniCat
  ``` 
      ChroniCat [-t] path
        -t = text chronicle, default binary

      mvn exec:java -Dexec.mainClass="net.openhft.chronicle.tools.slf4j.ChroniCat" -Dexec.args="..."
  ``` 
  Output:
  ``` 
    2014.02.26-15:14:54.548|warn|org.slf4j.impl.chronicle.Slf4jVanillaChronicleLoggerTest|something to slf4j
    2014.02.26-15:14:54.548|warn|org.slf4j.impl.chronicle.Slf4jVanillaChronicleLoggerTest|something to slf4j
    2014.02.26-15:14:54.548|warn|org.slf4j.impl.chronicle.Slf4jVanillaChronicleLoggerTest|something to slf4j
    2014.02.26-15:14:54.548|warn|org.slf4j.impl.chronicle.Slf4jVanillaChronicleLoggerTest|something to slf4j
    2014.02.26-15:14:54.548|warn|org.slf4j.impl.chronicle.Slf4jVanillaChronicleLoggerTest|something to slf4j
    2014.02.26-15:14:54.548|warn|org.slf4j.impl.chronicle.Slf4jVanillaChronicleLoggerTest|something to slf4j
  ``` 

  * net.openhft.chronicle.tools.ChroniDump
  ``` 
      ChroniDump [-i] path
        -i = IndexedCronicle, default VanillaChronicle

      mvn exec:java -Dexec.mainClass="net.openhft.chronicle.tools.ChroniDump" -Dexec.args="..."
  ``` 
  Output:
  ``` 
    8E 55 8C 6E 44 01 00 00 1E 31 6F 72 67 2E 73 6C ==> .U.nD....1org.sl
    66 34 6A 2E 69 6D 70 6C 2E 63 68 72 6F 6E 69 63 ==> f4j.impl.chronic
    6C 65 2E 53 6C 66 34 6A 43 68 72 6F 6E 69 63 6C ==> le.Slf4jChronicl
    65 4C 6F 67 67 65 72 54 65 73 74 12 73 6F 6D 65 ==> eLoggerTest.some
    74 68 69 6E 67 20 74 6F 20 73 6C 66 34 6A       ==> thing.to.slf4j
  ```
