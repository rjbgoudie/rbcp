#' Get Table Schema
#' @keywords internal
#' @importFrom DBI dbGetQuery
#' @importFrom glue glue
get_table_schema <- function(con, table_name, schema_name = "dbo") {
  query <- glue::glue("
    SELECT COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = '{table_name}'
      AND TABLE_SCHEMA = '{schema_name}'
    ORDER BY ORDINAL_POSITION
  ")
  DBI::dbGetQuery(con, query)
}

#' Sanitize Data for BCP
#'
#' Only handles Logical -> Integer conversion.
#' Date/Time/NA formatting is now handled by fwrite for performance.
#' @keywords internal
#' @importFrom data.table set
format_for_bcp <- function(dt) {
  # Fast in-place modification
  # We only scan for logicals. Everything else is handled by C-code in fwrite.
  cols <- names(dt)
  for (j in cols) {
    # Check type of first element (fast check)
    if (is.logical(dt[[j]])) {
      data.table::set(dt, j = j, value = as.integer(dt[[j]]))
    }
  }
  return(dt)
}
