HadoopSequenceFile .net reader
================

Hadoop sequence file reader

Credit to jcustenborder for HDFS writer, https://github.com/jcustenborder/HadoopSequenceFile
I did simplified rewrite of java SequenceFile.Reader class to .net
I have used very good documentation of the interface on http://blog.cloudera.com/blog/2011/01/hadoop-io-sequence-map-set-array-bloommap-files
thanks


use at own risk, developed for internal batch processing of the remote HDFS files


to compile you need to have referenced zlib.net package from the nuget online repository

*** Current version supports compressed and block compressed file format (SEQ6+)
