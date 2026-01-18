library(testthat)
library(mockery)
library(data.table)

test_that("bcp_insert appends by default", {
  stub(bcp_insert, "DBI::dbConnect", mock(1))
  stub(bcp_insert, "DBI::dbGetQuery", mock(data.frame(COLUMN_NAME="a", DATA_TYPE="int")))
  stub(bcp_insert, "DBI::dbDisconnect", mock(TRUE))
  stub(bcp_insert, "processx::run", mock(list(status=0, stdout="", stderr="")))
  m_exec <- mock(TRUE)
  stub(bcp_insert, "DBI::dbExecute", m_exec)
  stub(bcp_insert, "data.table::fwrite", mock(TRUE))
  
  bcp_insert(data.table(a=1), "srv", "db", "tbl")
  
  # dbExecute should NOT be called for append (default)
  expect_called(m_exec, 0)
})

test_that("bcp_insert truncates when overwrite=TRUE", {
  stub(bcp_insert, "DBI::dbConnect", mock(1))
  stub(bcp_insert, "DBI::dbGetQuery", mock(data.frame(COLUMN_NAME="a", DATA_TYPE="int")))
  stub(bcp_insert, "DBI::dbDisconnect", mock(TRUE))
  stub(bcp_insert, "processx::run", mock(list(status=0, stdout="", stderr="")))
  stub(bcp_insert, "data.table::fwrite", mock(TRUE))
  
  m_exec <- mock(TRUE)
  stub(bcp_insert, "DBI::dbExecute", m_exec)
  
  bcp_insert(data.table(a=1), "srv", "db", "tbl", overwrite = TRUE)
  
  # dbExecute SHOULD be called for TRUNCATE
  expect_called(m_exec, 1)
  args <- mock_args(m_exec)[[1]]
  expect_true(grepl("TRUNCATE TABLE", args[[2]]))
})
