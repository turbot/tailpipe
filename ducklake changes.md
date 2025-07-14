
- move deletion of parquet files for collection range into collector
- remove all tailpipe db generation code
- update introspection to use ducklake
- update partition deletion for ducklake
- minimise database creation - share instances where possible
- remove DeleteParquetFiles manual deletion code
  removed tpIndex migration