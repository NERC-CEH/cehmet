#' Import Campbell Scientific TOA5 Data
#'
#' Reads a TOA5-format data file from a Campbell Scientific logger and returns either
#' the metadata or the data as a data frame.
#'
#' @param filename Character. Path to the TOA5 file.
#' @param RetOpt Character. Either `"data"` (default) to return the data, or `"info"` to return metadata.
#'
#' @return A data frame of logger data if `RetOpt = "data"`, or a character vector of metadata if `RetOpt = "info"`.
#' @export
importCSdata <- function(filename, RetOpt = "data") {
  if (RetOpt == "info") {
    # bring in entire header of CSI TOA5 data file for metadata
    stn.info <- scan(file = filename, nlines = 4, what = character(), sep = "\r")
    return(stn.info)
  } else {
    # second line of header contains variable names
    header <- scan(
      file = filename, skip = 1, nlines = 1,
      what = character(), sep = ","
    )
    # bring in data
    stn.data <- read.table(
      file = filename, skip = 4, header = FALSE,
      na.strings = c("NAN"), sep = ","
    )
    names(stn.data) <- header
    # add column of R-formatted date/timestamps
    stn.data$TIMESTAMP <- as.POSIXlt(strptime(stn.data$TIMESTAMP, "%Y-%m-%d %H:%M:%S"))
    return(stn.data)
  }
}

#' Create Daily Output File Path
#'
#' Builds the full path for a daily output file based on date and logger identifiers.
#'
#' @param date_to_process POSIXlt. The date for which the file is being generated.
#' @param dir_out Character. Output directory.
#' @param station_code Character. Station code prefix (default: `"UK-AMo_BM_"`).
#' @param logger_id Character. Logger identifier (e.g., `"_L02"`).
#' @param file_id Character. File identifier (e.g., `"_F01"`).
#' @param ext Character. File extension (default: `".dat"`).
#'
#' @return A character string with the full path to the output file.
#' @export
get_pathname_daily <- function(date_to_process, dir_out,
                               station_code = "UK-AMo_BM_", logger_id, file_id, ext = ".dat") {
  # get the date parts of the output file name
  year <- date_to_process$year + 1900
  mon <- str_pad(date_to_process$mon + 1, 2, pad = "0")
  mday <- str_pad(date_to_process$mday, 2, pad = "0")

  # get the full path for writing output
  pathname <- path(dir_out, paste0(station_code, year, mon, mday, logger_id, file_id, ext))
  return(pathname)
}

#' Write Daily Subset of Logger Data
#'
#' Subsets a logger data file to a specific day and writes it to a new file in ICOS format.
#'
#' @param date_to_process POSIXlt. The date to extract.
#' @param dir_in Character. Input directory containing raw files.
#' @param fname_in Character. Filename of the input data.
#' @param pname_daily Character. Full path to the output daily file.
#'
#' @return Logical. `TRUE` if the file was written successfully, `FALSE` otherwise.
#' @export
write_daily_file <- function(date_to_process, dir_in, fname_in, pname_daily) {
  pname_in <- path(dir_in, fname_in)
  df <- importCSdata(pname_in)

  # remove duplicate rows - sometimes occur in the Campbell files
  df <- df[!duplicated(df$TIMESTAMP), ]

  # ICOS require the midnight value to be included as the final value of the day
  # i.e. not as the first value of the next day, as subsetting on yday would do.
  # So we need to lag the timestamps by -1 row and subset on this
  datect <- as.POSIXct(df$TIMESTAMP)
  # lag with n=1 and pad with NA (returns vector)
  datect <- shift(datect, n = 1, fill = NA, type = "lag")
  df$TIMESTAMP_lagged <- as.POSIXlt(datect)
  # subset using the lagged values
  df <- subset(
    df,
    TIMESTAMP_lagged$year == year(date_to_process) &
      TIMESTAMP_lagged$yday == yday(date_to_process)
  )
  df$TIMESTAMP_lagged <- NULL # and remove it
  # sort in time order - required because logger output is not always in order
  df <- df[order(df$TIMESTAMP), ]
  # change timestamp to a character variable with no punctuation
  df$TIMESTAMP <- str_remove_all(as.character(format(df$TIMESTAMP, format = "%Y-%m-%d %H:%M:%S")), "[-: ]")

  res <- try(write.csv(df,
    file = pname_daily, eol = "\r\n", na = "NAN",
    row.names = FALSE
  ))
  success <- is.null(res)
  return(success)
}

#' Upload Daily File to ICOS Server
#'
#' Uploads a daily file to the ICOS server using `curl`, with an MD5 hash for verification.
#'
#' @param pathname Character. Full path to the file to upload.
#' @param user Character. Username for authentication (default: from `cred` object).
#' @param password Character. Password for authentication (default: from `cred` object).
#'
#' @return Integer. Exit status from the system `curl` command (0 = success).
#' @export
upload_daily_file <- function(pathname,
                              user = cred$user, password = cred$password) {
  MD5 <- md5sum(pathname)

  # create the command from the parts
  print(paste("Uploading", path_file(pathname)))
  cmd <- paste0(
    "curl --upload-file ", pathname, " https://", user,
    password, "@data.icos-cp.eu/upload/etc/", MD5, "/", path_file(pathname)
  )
  # submit the command to the OS
  err <- system(cmd)
  return(err)
}

#' Remove Lines with invalid Timestamps
#'
#' Checks each line of a data file to ensure it starts with a valid timestamp.
#' Removes lines that do not match the expected format.
#'
#' @param pname_in Character. Path to the input file.
#' @param n_headers Integer. Number of header lines to preserve (default: 4).
#'
#' @return A character vector of lines (including headers) with only valid timestamped data.
#' @export
remove_time_errors <- function(pname_in, n_headers = 4) {
  # Define a regular expression for a valid timestamp format
  #                      i.e. YYYY-MM-DD hh:mm:ss
  timestamp_regex <- '^\\"\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}'

  # Read the header lines into a character vector
  v_header <- read_lines(pname_in, n_max = n_headers)
  # Read the rest of the file into a character vector, skipping the header lines
  v_lines <- read_lines(pname_in, skip = n_headers)

  # Check if every line begins with a valid timestamp
  v_valid_time <- str_starts(v_lines, timestamp_regex)

  # Print any lines that do not match the valid timestamp format
  if (any(!v_valid_time)) {
    print(paste0(pname_in, ": the following lines do not begin with a valid timestamp and will be removed:"))
    print(which(!v_valid_time))
    print(v_lines[!v_valid_time], sep = "\n")
  } else {
    print(paste0(pname_in, ": every line begins with a valid timestamp."))
  }
  # subset to only lines with a valid timestamp
  v_lines <- v_lines[v_valid_time]

  v_lines <- c(v_header, v_lines)
  return(v_lines)
}

#' Timecheck and Clean Multiple Logger Files
#'
#' Applies timestamp validation and cleaning to a set of input files using `remove_time_errors()`,
#' and writes the cleaned content to corresponding output files.
#'
#' @param v_fname_in Character vector. Filenames of the raw input data files.
#' @param v_fname_tc Character vector. Filenames for the cleaned (timechecked) output files.
#' @param dir_in Character. Directory containing the input files.
#'
#' @return A character vector of output filenames (`v_fname_tc`) that were written.
#' @details This function loops over each input file, checks for valid timestamps using
#' `remove_time_errors()`, and writes the cleaned lines to the corresponding output file.
#' It is designed for serial execution and is suitable for use in a `{targets}` pipeline.
#' @export
timecheck_all_files <- function(v_fname_in, v_fname_tc, dir_in) {
  for (i_file in seq_along(v_fname_in)) {
    pname_in <- fs::path(dir_in, v_fname_in[i_file])
    pname_tc <- fs::path(dir_in, v_fname_tc[i_file])
    v_lines <- remove_time_errors(pname_in)
    print(pname_tc)
    readr::write_lines(v_lines, file = pname_tc)
  }
  return(v_fname_tc)
}

#' Timecheck and Clean Multiple Logger Files
#'
#' Applies timestamp validation and cleaning to a set of input files using `remove_time_errors()`,
#' and writes the cleaned content to corresponding output files.
#'
#' @param v_fname_in Character vector. Filenames of the raw input data files.
#' @param v_fname_tc Character vector. Filenames for the cleaned (timechecked) output files.
#' @param dir_in Character. Directory containing the input files.
#'
#' @return A character vector of output filenames (`v_fname_tc`) that were written.
#'
#' @details This function loops over each input file, checks for valid timestamps using
#' `remove_time_errors()`, and writes the cleaned lines to the corresponding output file.
#' It is designed for serial execution and is suitable for use in a `{targets}` pipeline.
#'
#' @examples
#' \dontrun{
#' v_in <- c("logger1.dat", "logger2.dat")
#' v_out <- c("logger1_timechecked.dat", "logger2_timechecked.dat")
#' dir_in <- "data-raw/logs"
#' timecheck_all_files(v_in, v_out, dir_in)
#' }
#'
#' @export
timecheck_all_files <- function(v_fname_in, v_fname_tc, dir_in) {
  for (i_file in seq_along(v_fname_in)) {
    pname_in <- fs::path(dir_in, v_fname_in[i_file])
    pname_tc <- fs::path(dir_in, v_fname_tc[i_file])
    v_lines <- remove_time_errors(pname_in)
    readr::write_lines(v_lines, file = pname_tc)
  }
  return(v_fname_tc)
}

#' Process and Upload a Daily Logger File
#'
#' Generates a daily ICOS-format file from a timechecked logger file, optionally uploads it,
#' and returns the path to the output file.
#'
#' @param date_to_process POSIXlt. The date for which the file should be generated.
#' @param i_file Integer. Index of the file to process (used to select logger and file IDs).
#' @param dir_in Character. Directory containing the input (timechecked) files.
#' @param dir_out Character. Directory where the output file should be written.
#' @param station_code Character. Prefix for the station code (e.g., `"UK-AMo_BM_"`).
#' @param v_logger_id Character vector. Logger identifiers (e.g., `"_L02"`).
#' @param v_file_id Character vector. File identifiers (e.g., `"_F01"`).
#' @param v_fname_tc Character vector. Filenames of the timechecked input files.
#' @param ext Character. File extension for the output file (e.g., `".dat"`).
#' @param overwrite Logical. Whether to overwrite an existing file.
#' @param do_upload Logical. Whether to upload the file if it did not already exist.
#' @param force_upload Logical. Whether to force upload even if the file already existed.
#'
#' @return Character. The full path to the daily output file.
#'
#' @details
#' This function constructs the output file path using `get_pathname_daily()`, checks if the file
#' already exists, and if not (or if `overwrite = TRUE`), writes the daily file using
#' `write_daily_file()`. If `do_upload` is `TRUE` and the file is new, or if `force_upload` is `TRUE`,
#' the file is uploaded using `upload_daily_file()`. If the upload fails, the file is deleted.
#'
#' @examples
#' \dontrun{
#' process_daily_file(
#'   date_to_process = as.POSIXlt("2025-07-01"),
#'   i_file = 1,
#'   dir_in = "data-raw",
#'   dir_out = "data-out",
#'   station_code = "UK-AMo_BM_",
#'   v_logger_id = c("_L02"),
#'   v_file
process_daily_file <- function(date_to_process, i_file, dir_in, dir_out,
                               station_code, v_logger_id, v_file_id,
                               v_fname_tc, ext, overwrite, do_upload, force_upload) {
  pname_daily <- get_pathname_daily(
    date_to_process, dir_out,
    station_code, v_logger_id[i_file],
    v_file_id[i_file], ext
  )

  file_already_existed <- file.exists(pname_daily)
  message(path_file(pname_daily), " already existed: ", file_already_existed)

  if (!file_already_existed || overwrite) {
    message("Writing daily file: ", pname_daily)
    written <- write_daily_file(date_to_process, dir_in, v_fname_tc[i_file], pname_daily)
  }

  if ((!file_already_existed && do_upload) || force_upload) {
    err <- upload_daily_file(pname_daily)
    if (err != 0) {
      unlink(pname_daily)
    }
  } else {
    message("Not uploading daily file: ", pname_daily)
  }

  return(pname_daily)
}

#' Construct SPARQL Query for ICOS File Metadata
#'
#' Builds a SPARQL query string to retrieve persistent identifiers (PIDs) and filenames
#' for a given set of ICOS data files, filtered by file type and station.
#'
#' @param file_names Character vector. Filenames to query (e.g., `"UK-AMo_BM_20250701_L02_F01.dat"`).
#'
#' @return A character string containing the SPARQL query.
#'
#' @details
#' This function constructs a SPARQL query that filters for BioMeteorological (BM) data
#' files associated with the UK-AMo station. It replaces any file extensions with `.dat`
#' and embeds the filenames into a regex filter in the query.
#'
#' @export
make_query <- function(file_names = NA) {
  file_names <- gsub("\\.[^.]+$", ".dat", file_names)

  sparql_query <- paste0('
prefix cpmeta: <http://meta.icos-cp.eu/ontologies/cpmeta/>
prefix cpres: <http://meta.icos-cp.eu/resources/cpmeta/>
prefix esstation: <http://meta.icos-cp.eu/resources/stations/ES_>
prefix prov: <http://www.w3.org/ns/prov#>
prefix xsd: <http://www.w3.org/2001/XMLSchema#>

select ?pid ?fileName
where {
  {
    SELECT ?spec WHERE {
      VALUES (?ftype ?spec){
        ("DHP" cpres:digHemispherPics )
        ("EC" cpres:etcEddyFluxRawSeriesBin )
        ("EC" cpres:etcEddyFluxRawSeriesCsv )
        ("ST" cpres:etcStorageFluxRawSeriesBin )
        ("ST" cpres:etcStorageFluxRawSeriesCsv )
        ("BM" cpres:etcBioMeteoRawSeriesBin )
        ("BM" cpres:etcBioMeteoRawSeriesCsv )
        ("SAHEAT" cpres:etcSaheatFlagFile )
        ("CEP" cpres:ceptometerMeasurements )
      }
      FILTER((?ftype = "BM"))
    }
  }

  ?dobj cpmeta:hasObjectSpec ?spec .
  ?dobj cpmeta:wasAcquiredBy/prov:wasAssociatedWith esstation:UK-AMo .
  ?dobj cpmeta:wasAcquiredBy/prov:startedAtTime ?startTime .
  FILTER NOT EXISTS{[] cpmeta:isNextVersionOf ?dobj }
  BIND(substr(str(?dobj), strlen(str(?dobj)) - 23) AS ?pid)
  ?dobj cpmeta:hasName ?fileName .
  ?dobj cpmeta:hasSizeInBytes ?size .
  FILTER(regex(?fileName, "', paste(file_names, collapse = "|"), '"))
}
')

  return(sparql_query)
}

#' Retrieve Persistent Identifiers (PIDs) for ICOS Files
#'
#' Queries the ICOS metadata SPARQL endpoint to retrieve PIDs for a list of filenames.
#'
#' @param file_names Character vector. Filenames to query.
#'
#' @return A data frame with columns `filename` and `pid`.
#'
#' @details
#' This function sends a SPARQL query to the ICOS metadata endpoint using `make_query()`.
#' It parses the JSON response and joins the results with the input filenames.
#' If no PIDs are returned or the number of results doesn't match the input, warnings or errors are raised.
#' NB: PIDs are assigned after the post-upload QAQC. They are assigned by ICOS, once they recieve the uploaded data from the relevant Thematic Centre.
#'
#' @export
get_pid <- function(file_names = NA) {
  endpoint <- "https://meta.icos-cp.eu/sparql"
  response <- httr::GET(endpoint, query = list(query = make_query(file_names)))

  status_code <- httr::status_code(response)
  # cat("HTTP Status Code:", status_code, "\n") # 200 == successful
  if (status_code != 200) {
    stop("API status code suggests an error. Check the query?")
  }

  # content_type <- httr::headers(response)$`content-type`
  # cat("Content Type:", content_type, "\n")

  parsed_json <- fromJSON(httr::content(response, "text"), simplifyDataFrame = T)

  if (length(parsed_json$results$bindings$fileName$value) == 0) {
    stop("Did not recieve any PIDs")
  }

  if (length(parsed_json$results$bindings$fileName$value) != length(file_names)) {
    warning("Missing PIDs")
  }

  if (length(parsed_json$results$bindings$fileName$value) > 0) {
    return(dplyr::left_join(data.frame(filename = file_names), data.frame(filename = parsed_json$results$bindings$fileName$value, pid = parsed_json$results$bindings$pid$value), by = "filename"))
  }
}

#' Query if ICOS Files Exist on the Carbon Portal
#'
#' Verifies whether files with given filenames have corresponding PIDs and exist on the ICOS Carbon Portal.
#'
#' @param filenames Character vector. Filenames to check.
#'
#' @return Logical vector. `TRUE` if the file exists on the portal, `FALSE` otherwise.
#'
#' @details
#' This function uses `get_pid()` to retrieve PIDs for the given filenames, constructs the
#' corresponding Carbon Portal URLs, and checks if those URLs exist using `url_exists()`.
#' It also handles the case where a PID is `NA` but the URL appears to exist.
#'
#' @export
query_CP <- function(filenames = NA) {
  v_url <- paste0("https://meta.icos-cp.eu/objects/", get_pid(filenames)$pid)
  result <- url_exists(v_url)
  result <- ifelse(v_url == "https://meta.icos-cp.eu/objects/NA", F, result)
  if (length(result == 1)) {
    return(unname(result))
  } else {
    return(result)
  }
}

#' Check if URLs Exist
#'
#' This function takes a vector of URLs and checks whether each one is reachable.
#' It returns a logical vector indicating the existence of each URL.
#'
#' @param urls A character vector of URLs to check.
#' @return A logical vector of the same length as `urls`, where each element is
#' `TRUE` if the corresponding URL is reachable (i.e., no error occurred during the GET request),
#' and `FALSE` otherwise.
#' @importFrom httr GET
#' @examples
#' url_exists(c("https://www.google.com", "https://nonexistent.example.com"))
url_exists <- function(urls) {
  results <- sapply(urls, function(url) {
    response <- tryCatch(
      GET(url),
      error = function(e) e
    )

    if (inherits(response, "error")) {
      # An error occurred, indicating that the URL does not exist or couldn't be reached
      return(FALSE)
    } else {
      # The URL exists and the response was successful
      return(TRUE)
    }
  })
}

#' Write Carbon Portal Response to CSV
#'
#' This function writes a data frame (typically a response from the ICOS Carbon Portal)
#' to a CSV file. The filename includes the date range from the `query_dates` vector,
#' formatted as `dd-mm-yyyy`.
#'
#' @param response A data frame containing the response data to be written.
#' @param query_dates A vector of dates (as Date objects) representing the query period.
#' These are used to generate the filename.
#'
#' @return Writes a CSV file to the working directory. The function does not return a value.
#' @examples
#' \dontrun{
#' write_CP_response(response_data, as.Date(c("2025-06-26", "2025-07-02")))
#' }
write_CP_response <- function(response, query_dates){
  fname <- sprintf(
    "./data/CP_responses/CP_response_%s_to_%s.csv",
    format(min(query_dates), "%d-%m-%Y"),
    format(max(query_dates), "%d-%m-%Y"))

  readr::write_csv(response,
                   file = fname)

  return(fname)

}
