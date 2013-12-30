Vanilla Chronicle
============

Vanilla Chronicle is a designed for more features rather than just speed. It it is fast enough, it should be easier to use/manage.  It supports

 - rolling files on a daily, weekly or hourly basis.
 - concurrent writers on the same machine.
 - concurrent readers on the same machine or across multiple machines via TCP replication.
 - zero copy serialization and deserialization.
 - one million writes/reads per second on commodity hardware.
 - persistence as required.
 - reader on source can wait for replication. i.e. source reader sees excerpts after replication acknowledge.
 - data files have more information for rebuilding indexes.
 - exact length of entries

File Format
------------

The directory structure is as follows.

   base-directory /
       cycle-name /
            index-{n}          - multiple index files from 0 .. {n}
            data-{tid}-{m}  - multiple data files for each thread id (matches the process id) from 0 .. {n}

The index file format is an sequence of 8-byte values which consist of a 16-bit {tid} and the offset in bytes of the start of the record.

The data file format has a 4-byte length of record. The length is the inverted bits of the 4-byte value.
This is used to avoid seeing regular data as a length and detect corruption.  The length always starts of a 4-byte boundary.

TCP Replication
---------------

