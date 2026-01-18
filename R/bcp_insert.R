#' Bulk Insert Data into SQL Server
#'
#' @param input_data A data.frame, data.table, or path to a .parquet file.
#' @param server SQL Server instance name.
#' @param database Database name.
#' @param table Target table name.
#' @param schema Target schema name (default "dbo").
#' @param overwrite Boolean. If TRUE, table is TRUNCATED before insert. Default FALSE.
#' @param username SQL authentication username (optional).
#' @param password SQL authentication password (optional).
#' @param trusted_connection Boolean. Use Windows Authentication? Default TRUE.
#' @param bcp_path Path to bcp executable. Default assumes it is in system PATH.
#' @param batch_size Number of rows per batch.
#' @param tmpdir Directory for temporary files.
#' @param packet_size Network packet size in bytes.
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
  checkmate::assert_choice(class(input_data)[1], c("data.frame", "data.table", "character"))

  if (is.character(input_data) &&
    grepl("\\.parquet$", input_data, ignore.case = TRUE)) {
    if (!file.exists(input_data)) stop("Parquet file not found.")
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
    ifelse(trusted_connection, "Trusted_Connection=yes;", glue::glue("Uid={username};Pwd={password};"))
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
    cli::cli_alert_warning(glue::glue("Overwrite mode enabled. Truncating {schema}.{table}..."))
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
    cli::cli_alert_info(glue::glue("Append mode enabled. Adding to existing data in {schema}.{table}."))
  }

  # --- 4. Schema Alignment ---
  cli::cli_progress_step("Aligning schema")
  db_cols <- get_table_schema(con, table, schema)
  if (nrow(db_cols) == 0) stop(glue::glue("Table {schema}.{table} not found."))

  missing_cols <- setdiff(db_cols$COLUMN_NAME, names(dt))
  if (length(missing_cols) > 0) stop("Missing columns: ", paste(missing_cols, collapse = ", "))

  data.table::setcolorder(dt, db_cols$COLUMN_NAME)
  dt <- dt[, .SD, .SDcols = db_cols$COLUMN_NAME]
  cli::cli_progress_done()

  # --- 5. Write to File ---
  cli::cli_progress_step("Staging data for bcp (UTF-8)")
  dt <- format_for_bcp(dt)
  temp_file <- tempfile(fileext = ".dat", tmpdir = tmpdir)
  on.exit(unlink(temp_file), add = TRUE)

  field_term <- "\x1f"
  row_term <- "\n"
  cli::cli_progress_done()
  cli::cli_progress_step("Converting datetimes to strings")

  cols <- names(dt)[sapply(dt, inherits, "POSIXct")]
  # Apply transformation by reference (in-place)
  if (FALSE) {
    dt[, (cols) := lapply(.SD, clock::date_format, format = "%Y-%m-%d %H:%M:%S"), .SDcols = cols]
  } else if (FALSE) {
    dt[, (cols) := lapply(.SD, stringi::stri_datetime_format, format = "yyyy-MM-dd HH:mm:ss"), .SDcols = cols]
  } else if (TRUE) {
    format_fast_ODBC <- function(x) {
      u_x <- unique(x)
      # Format uniques only once
      u_fmt <- clock::date_format(u_x, format = "%Y-%m-%d %H:%M:%S")
      # Map back using fast integer matching
      u_fmt[match(x, u_x)]
    }
    dt[, (cols) := lapply(.SD, format_fast_ODBC), .SDcols = cols]
  }
  cli::cli_progress_done()

  cli::cli_progress_step("Writing {temp_file} ")
  data.table::fwrite(dt,
    file = temp_file, sep = field_term, eol = row_term,
    col.names = FALSE, quote = FALSE, na = "",
    # dateTimeAs = "write.csv",
    nThread = parallel::detectCores() - 1,
    buffMB = 512,
    showProgress = TRUE,
    verbose = TRUE
  )
  cli::cli_progress_done()

  # --- 6. Execute BCP ---
  cli::cli_progress_step("Running BCP")
  auth_flag <- if (trusted_connection) "-T" else glue::glue("-U {username} -P {password}")
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
    error = function(e) stop("Could not spawn BCP process.")
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
