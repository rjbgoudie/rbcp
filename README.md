# rbcp

## Overview

`rbcp` is an R package that provides a high-performance wrapper for the SQL Server BCP (Bulk Copy Program) utility. It is designed to be a simple and efficient way to insert large datasets into SQL Server from R.
The package supports `data.frame`, `data.table`, and `.parquet` files as input, handles schema validation (since `bcp` needs the columns in the right order), and supports both Windows and SQL Server authentication.

## Installation

You can install the development version of `rbcp` from GitHub with:

```r
# install.packages("devtools")
devtools::install_github("rjbgoudie/rbcp")
```

## Usage

```r
# Create a sample data frame
my_data <- data.frame(
  ID = 1:10,
  Name = letters[1:10],
  Value = rnorm(10)
)

# Perform a bulk insert using Windows Authentication
bcp_insert(
  input_data = my_data,
  server = "MY_SQL_SERVER",
  database = "MyDatabase",
  table = "MyTable",
  schema = "dbo",
  overwrite = TRUE
)
```
