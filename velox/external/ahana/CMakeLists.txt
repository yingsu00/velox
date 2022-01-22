

add_library(ahana_parquet ParquetReader.cpp ParquetColumnReader.cpp)

set_property(TARGET ahana_parquet PROPERTY JOB_POOL_COMPILE high_memory_pool)