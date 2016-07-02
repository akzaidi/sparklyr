spark_api_read_csv <- function(api, path, columns = NULL, header = TRUE, delimiter = ",") {
  read <- invoke(spark_sql_or_hive(api), "read")
  format <- invoke(read, "format", "com.databricks.spark.csv")
  optionHeader <- invoke(format, "option", "header", tolower(as.character(header)))

  if (identical(columns, NULL)) {
    optionSchema <- invoke(optionHeader, "option", "inferSchema", "true")
  }
  else {
    columnDefs <- spark_api_build_types(api, columns)
    optionSchema <- invoke(optionHeader, "schema", columnDefs)
  }
  
  optionDelimer <- invoke(optionSchema, "option", "delimiter", delimiter)

  invoke(optionSchema, "load", path)
}

spark_api_write_csv <- function(df, path) {
  write <- invoke(df, "write")
  format <- invoke(write, "format", "com.databricks.spark.csv")
  optionHeader <- invoke(format, "option", "header", "true")
  invoke(optionHeader, "save", path)

  invisible(TRUE)
}
