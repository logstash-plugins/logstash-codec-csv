# 0.1.1
  - New dependency requirements for logstash-core for the 5.0 release
## 0.1.0
  - Initial version of the codec, this first version include feature parity with the current CSV filter, this the ability to set column names, the column separator, a quote char, decide if autogeneration of columns is ok, type conversion and skip empty columns.
  - This initial version also include an option to treat the first
chunk of data seen as containing the headers. This functionality
will be useful when reading CSV files (all at once, usually) that
contain this information.
