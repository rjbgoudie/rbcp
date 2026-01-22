#' Bulk Insert Data into SQL Server using bcp
#'
#' This function provides a high-performance method for bulk inserting data into a
#' SQL Server table. It acts as a wrapper around the SQL Server command-line
#' utility `bcp`, handling data serialization, schema validation, and the
#' execution of the `bcp` process.
#'
#' @details
#' The function performs the following steps:
#' \enumerate{
#'   \item **Input Validation:** Checks if the input is a `data.frame`, `data.table`, or a path to a `.parquet` file.
#'   \item **Database Connection:** Connects to the SQL Server database using ODBC.
#'   \item **Overwrite Logic:** If `overwrite = TRUE`, it truncates the target table.
#'   \item **Schema Alignment:** It retrieves the schema of the target table and reorders the columns of the input data to match. It will stop if any columns from the SQL table are missing in the input data.
#'   \item **Data Staging:** The data is written to a temporary, UTF-8 encoded, delimited file. `data.table::fwrite` is used for high performance.
#'   \item **BCP Execution:** The `bcp` command-line utility is called to insert the data from the temporary file into the target SQL Server table.
#' }
#'
#' Authentication can be done using either Windows Authentication (`trusted_connection = TRUE`) or SQL Server Authentication (by providing `username` and `password`).
#'
#' @param input_data A `data.frame`, `data.table`, or a character string specifying the path to a `.parquet` file. The data to be inserted.
#' @param server The name of the SQL Server instance to connect to.
#' @param database The name of the database containing the target table.
#' @param table The name of the target table for the bulk insert operation.
#' @param schema The schema of the target table. Defaults to `"dbo"`.
#' @param overwrite A boolean value. If `TRUE`, the target table will be truncated before inserting the new data. If `FALSE` (the default), the new data will be appended.
#' @param username The username for SQL Server Authentication. Not required if `trusted_connection = TRUE`.
#' @param password The password for SQL Server Authentication. Not required if `trusted_connection = TRUE`.
#' @param trusted_connection A boolean value. If `TRUE` (the default), Windows Authentication will be used. If `FALSE`, SQL Server Authentication will be used (requires `username` and `password`).
#' @param bcp_path The path to the `bcp` executable. Defaults to `"bcp"`, assuming it is in the system's PATH.
#' @param batch_size The number of rows per batch for the `bcp` operation. Defaults to `1,000,000`.
#' @param tmpdir The directory where the temporary data file will be created. Defaults to `tempdir()`.
#' @param packet_size The network packet size (in bytes) for the `bcp` connection. Defaults to `4096`.
#'
#' @return The function is called for its side effects and does not return a value. It will stop with an error if the operation fails.
#'
#' @seealso
#' Microsoft's documentation for the `bcp` utility: \url{https://docs.microsoft.com/en-us/sql/tools/bcp-utility}
#'
#' @examples
#' \dontrun{
#' # Create a sample data frame
#' my_data <- data.frame(
#'   ID = 1:10,
#'   Name = letters[1:10],
#'   Value = rnorm(10),
#'   InsertDate = as.POSIXct(Sys.Date())
#' )
#'
#' # Perform a bulk insert using Windows Authentication
#' bcp_insert(
#'   input_data = my_data,
#'   server = "MY_SQL_SERVER",
#'   database = "MyDatabase",
#'   table = "MyTable",
#'   schema = "dbo",
#'   overwrite = TRUE
#' )
#'
#' # Perform a bulk insert using SQL Server Authentication
#' bcp_insert(
#'   input_data = my_data,
#'   server = "MY_SQL_SERVER",
#'   database = "MyDatabase",
#'   table = "MyTable",
#'   schema = "dbo",
#'   overwrite = FALSE,
#'   username = "my_user",
#'   password = "my_password",
#'   trusted_connection = FALSE
#' )
#'
#' # Insert from a Parquet file
#' arrow::write_parquet(my_data, "my_data.parquet")
#' bcp_insert(
#'   input_data = "my_data.parquet",
#'   server = "MY_SQL_SERVER",
#'   database = "MyDatabase",
#'   table = "MyTable"
#' )
#' }
#'
#' @importFrom arrow read_parquet
#' @importFrom checkmate assert_choice
#' @importFrom cli cli_alert_danger cli_alert_info cli_alert_success cli_alert_warning cli_code cli_h1 cli_progress_done cli_progress_step
#' @importFrom clock date_format
#' @importFrom data.table := as.data.table fwrite setcolorder
#' @importFrom DBI dbConnect dbDisconnect dbExecute
#' @importFrom glue glue
#' @importFrom odbc odbc
#' @importFrom parallel detectCores
#' @importFrom processx run
#' @export
bcp_insert <- function(input_data,
                       server,
                       database,
                       table,
                       schema = "dbo",
                       overwrite = FALSE,
                       username = NULL,
                       password = NULL,
                       trusted_connection = TRUE,
                       bcp_path = "bcp",
                       batch_size = 1000000L,
                       tmpdir = tempdir(),
                       packet_size = 4096L) {
  cli::cli_h1(glue::glue("BCP insert: {schema}.{table}"))

  # --- 1. Validation ---
  cli::cli_progress_step("Reading input data", msg_done = "Data loaded")
  checkmate::assert_choice(
    class(input_data)[1],
    c("data.frame", "data.table", "character")
  )

  if (is.character(input_data) &&
    grepl("\\.parquet$", input_data, ignore.case = TRUE)) {
    if (!file.exists(input_data)) {
      stop("Parquet file not found.")
    }
    dt <- data.table::as.data.table(arrow::read_parquet(input_data))
  } else {
    dt <- data.table::as.data.table(input_data)
  }
  cli::cli_progress_done()

  # --- 2. Database Connection & Schema ---
  cli::cli_progress_step("Connecting to SQL Server")
  con_str <- glue::glue(
    "Driver={{ODBC Driver 17 for SQL Server}};",
    "Server={server};Database={database};",
    ifelse(trusted_connection,
      "Trusted_Connection=yes;",
      glue::glue("Uid={username};Pwd={password};")
    )
  )

  con <- tryCatch(
    {
      DBI::dbConnect(odbc::odbc(), .connection_string = con_str)
    },
    error = function(e) {
      cli::cli_progress_done(result = "failed")
      stop("Connection failed: ", e$message)
    }
  )
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  cli::cli_progress_done()

  # --- 3. Overwrite / Append Logic ---
  if (overwrite) {
    cli::cli_alert_warning(
      glue::glue("Overwrite mode enabled. Truncating {schema}.{table}...")
    )
    tryCatch(
      {
        DBI::dbExecute(con, glue::glue("TRUNCATE TABLE {schema}.{table}"))
        cli::cli_alert_success("Table truncated.")
      },
      error = function(e) {
        stop("Failed to truncate table. Check permissions.")
      }
    )
  } else {
    cli::cli_alert_info(
      glue::glue(
        "Append mode enabled.",
        "Adding to existing data in {schema}.{table}."
      )
    )
  }

  # --- 4. Schema Alignment ---
  cli::cli_progress_step("Aligning schema")
  db_cols <- get_table_schema(con, table, schema)
  if (nrow(db_cols) == 0) {
    stop(glue::glue("Table {schema}.{table} not found."))
  }

  missing_cols <- setdiff(db_cols$COLUMN_NAME, names(dt))
  if (length(missing_cols) > 0) {
    stop("Missing columns: ", paste(missing_cols, collapse = ", "))
  }

  data.table::setcolorder(dt, db_cols$COLUMN_NAME)
  dt <- dt[, .SD, .SDcols = db_cols$COLUMN_NAME]
  cli::cli_progress_done()

  # --- 5. Write to File ---
  cli::cli_progress_step("Staging data for bcp (UTF-8)")
  dt <- format_for_bcp(dt)
  temp_file <- tempfile(fileext = ".dat", tmpdir = tmpdir)
  on.exit(unlink(temp_file), add = TRUE)
  cli::cli_progress_done()

  cli::cli_progress_step("Writing {temp_file} ")
  field_term <- "\x1f"
  row_term <- "\n"

  data.table::fwrite(dt,
    file = temp_file,
    sep = field_term,
    eol = row_term,
    col.names = FALSE,
    quote = FALSE,
    na = "",
    nThread = parallel::detectCores() - 1,
    buffMB = 512,
    showProgress = TRUE,
    verbose = TRUE
  )
  cli::cli_progress_done()

  # --- 6. Execute BCP ---
  cli::cli_progress_step("Inserting data to server using bcp")
  auth_flag <- if (trusted_connection) {
    "-T"
  } else {
    glue::glue("-U {username} -P {password}")
  }
  full_table_name <- glue::glue("{database}.{schema}.{table}")

  args <- c(
    full_table_name,
    "in", temp_file,
    if (trusted_connection) "-T" else c("-U", username, "-P", password),
    "-S", server,
    "-c",
    "-t", field_term,
    "-r", row_term,
    "-C", "65001",
    "-b", as.character(batch_size),
    "-a", packet_size
  )

  result <- tryCatch(
    {
      processx::run(bcp_path, unlist(args),
        error_on_status = FALSE,
        echo_cmd = FALSE,
        echo = TRUE
      )
    },
    error = function(e) {
      stop("Could not spawn BCP process.")
    }
  )

  if (result$status != 0) {
    cli::cli_progress_done(result = "failed")
    cli::cli_alert_danger("BCP Failed")
    err <- result$stderr
    if (err == "") err <- result$stdout
    cli::cli_code(err)
    stop("BCP non-zero exit.", call. = FALSE)
  } else {
    cli::cli_progress_done()
    cli::cli_alert_success(glue::glue("Inserted {nrow(dt)} rows into {schema}.{table}"))
  }
}
