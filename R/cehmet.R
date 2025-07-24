"%!in%" <- Negate("%in%")
"%!like%" <- Negate("%like%")

# define function to check is each line of file starts with a valid timestamp
# of the form YYYY-MM-DD hh:mm:ss
remove_time_errors <- function(
  pname_in,
  n_headers = 4,
  dryrun = FALSE,
  pname_out = NULL
) {
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
    print(paste0(
      pname_in,
      ": the following lines do not begin with a valid timestamp and will be removed:"
    ))
    print(which(!v_valid_time))
    print(v_lines[!v_valid_time], sep = "\n")
  } else {
    print(paste0(pname_in, ": every line begins with a valid timestamp."))
  }
  # subset to only lines with a valid timestamp
  v_lines <- v_lines[v_valid_time]

  # write_lines(v_header, file = pname_in, append = FALSE)
  # write_lines(v_lines,  file = pname_in, append = TRUE)
  # length(v_lines)
  v_lines <- c(v_header, v_lines)

  if (is.null(pname_out)) {
    pname_out <- ifelse(
      grepl("_timechecked\\.[^.]+$", pname_in),
      pname_in,
      sub("(\\.[^.]+)$", "_timechecked\\1", pname_in)
    )
  }

  if (!dryrun) {
    write_lines(v_lines, file = pname_out)
  }
  return(pname_out)
}

# define function to read TOA5 data
import_campbell_data <- function(fname) {
  # second line of header contains variable names
  header <- scan(
    file = fname,
    skip = 1,
    nlines = 1,
    what = character(),
    sep = ","
  )
  # read in data
  dt <- fread(
    file = fname,
    skip = 4,
    header = FALSE,
    na.strings = c("NAN"),
    sep = ","
  )
  names(dt) <- header

  if (class(dt$TIMESTAMP)[1] != "POSIXct") {
    stop(paste("Non-POSIX timestamp in", fname))
  }

  # remove duplicate rows - sometimes occur in the Campbell files
  dt <- dt[!duplicated(dt$TIMESTAMP), ]
  # rename timestamp variable
  setnames(dt, "TIMESTAMP", "date")
  # if variable RECORD exists, remove it
  if ("RECORD" %in% colnames(dt)) {
    dt$RECORD <- NULL
  }
  return(dt)
}


# remove standard deviations and any unneeded columns
remove_excess_cols <- function(dt, remove_std = FALSE) {
  # remove any standard deviations columns (ending with "_Std") - no need for these
  if (remove_std) {
    dt[, grep("_Std$", colnames(dt)) := NULL]
  }
  dt[, grep("_IU", colnames(dt)) := NULL]
  # remove any columns marked for deletion - usually duplicates after merging
  dt[, grep("_delete_me", colnames(dt)) := NULL]

  # check for duplicate cols
  if (any(duplicated(names(dt)))) {
    stop("Duplicate column names found in data frame.")
  }
  return(dt)
}

# standardise names
standardise_names <- function(dt) {
  names(dt) <- make.names(names(dt))
  # remove "_Avg" suffix - inconsistent and unnecessary
  names(dt) <- str_replace(names(dt), "_Avg", "")

  # ad hoc renaming and changes
  names(dt) <- str_replace(names(dt), "_HMP", "")
  names(dt) <- str_replace(names(dt), "A_tot_NRT_Tot", "A_tot_NRT_12_1_1_Tot")
  names(dt) <- str_replace(names(dt), "P_TB_Tot", "P_13_1_2_Tot")

  # check for duplicate cols
  if (any(duplicated(names(dt)))) {
    stop("Duplicate column names found in data frame.")
  }
  return(dt)
}

# dt <- calc_energy_balance(dt)
calc_energy_balance <- function(dt) {
  # energy balance
  dt[, RN_5_1_1 := SW_IN_5_1_1 + LW_IN_5_1_1 - SW_OUT_5_1_1 - LW_OUT_5_1_1]
  return(dt)
}

# dt <- remove_soil_heat_flux_cal(dt)
remove_soil_heat_flux_cal <- function(dt) {
  # soil heat flux
  # the program does not seem to remove all cases
  dt[G_ISCAL_8_1_1 != 0, G_8_1_1 := NA]
  dt[G_ISCAL_9_1_1 != 0, G_9_1_1 := NA]
  dt[G_ISCAL_10_1_1 != 0, G_10_1_1 := NA]
  dt[G_ISCAL_11_1_1 != 0, G_11_1_1 := NA]

  # we do not want to keep the soil heat flux sensitivity factor
  names_sf <- names(dt)[str_starts(names(dt), "G_SF")]
  dt[, eval(names_sf) := NULL]
  # used to exclude 8 & 9 from the average because of calibration problems:
  # dt$G <- rowMeans(data.frame(dt$G_4_1_1, dt$G_10_1_1, dt$G_11_1_1), na.rm = TRUE)
  return(dt)
}

adhoc_changes <- function(dt) {
  # mainmet WTD in cm not m, but pos not neg? or just needs a -ve offset for installation depth?
  dt$WTD_4_1_1 <- dt$WTD_4_1_1 * 0.01 - 0.6 # guessing installation depth of -0.6 m

  # mainmet SWC in frac not %
  dt$SWC_4_1_1 <- dt$SWC_4_1_1 * 100
  dt$SWC_4_2_1 <- dt$SWC_4_2_1 * 100
  dt$SWC_4_3_1 <- dt$SWC_4_3_1 * 100
  dt$SWC_4_4_1 <- dt$SWC_4_4_1 * 100

  # windspeed and direction from WindSonic @10 m
  dt$WS_6_1_1 <- dt$WS_6_1_1_WVc.1.
  dt$WD_6_1_1 <- dt$WS_6_1_1_WVc.2.

  # prior to 19 Feb 2022, there was a program error whereby the WindSonic gave wrong data
  # so we use instead windspeed and direction from EC sonic @3 m
  ##* WIP: only works if we merge with EC data
  # dt$WS_6_1_1[dt$date < "2022-02-19"] <- dt$wind_speed[dt$date < "2022-02-19"]
  # dt$WD_6_1_1[dt$date < "2022-02-19"] <- dt$wind_dir[dt$date < "2022-02-19"]

  dt$WS_6_1_1_WVc.1. <- NULL
  dt$WS_6_1_1_WVc.2. <- NULL
  dt$WS_6_1_1_WVc.3. <- NULL

  return(dt)
}


spatial_average_icos_vars <- function(dt) {
  df_range <- read_excel(here("data/variable_ranges.xlsx"), sheet = "rules")
  df_range <- subset(df_range, !is.na(min))
  v_icos_names <- unique(df_range$symbol)
  # we do not want to keep the soil heat flux sensitivity factor
  v_icos_names <- v_icos_names[!str_starts(v_icos_names, "G_SF")]

  for (var_name in v_icos_names) {
    print(var_name)
    v_matching_names <-
      names(dt)[
        str_starts(names(dt), paste0(var_name, "_")) &
          names(dt) %!like% "_IU_" &
          names(dt) %!like% "_SF" &
          names(dt) %!like% "_ISCAL_" &
          names(dt) %!like% "_Max" &
          # names(dt) %!like% "_Tot" &
          names(dt) %!like% "_Std"
      ]
    print(v_matching_names)

    # calculate each of the ICOS vars as the mean of all replicates
    # possible limit soil vars to one depth?
    dt <- within(
      dt,
      assign(
        var_name,
        rowMeans(dt[, ..v_matching_names, drop = FALSE], na.rm = TRUE)
      )
    )
    # print(summary(dt[, v_matching_names, drop = FALSE]))
  }
  return(dt)
}


###### functions below not called I think #####

combine_with_existing <- function(dt, fname_to_append_to, write_csv = TRUE) {
  # read the annual or complete file
  # dt_to_append_to <- fread(fname_to_append_to)
  dt_to_append_to <- import_campbell_data(fname_to_append_to)
  dt_to_append_to <- standardise_names(dt_to_append_to)
  # combine the two data.tables
  dt <- rbindlist(list(dt, dt_to_append_to), use.names = TRUE)
  # remove duplicate times
  dt <- dt[!duplicated(dt[, "time"], fromLast = TRUE), ]
  setorder(dt, time)
  # write the data to a csv file
  if (write_csv) {
    fwrite(dt, fname_to_append_to, dateTimeAs = "write.csv")
  }
  return(dt)
}

append_new_data <- function(
  dt_files_to_read,
  last_date_to_process = as.POSIXlt(Sys.Date() - 1)
) {
  for (i in seq_len(nrow(dt_files_to_read))) {
    # get the file names for reading and writing
    fname_in <- fs::path(paste(
      dt_files_to_read[i, "pname_data"],
      dt_files_to_read[i, "site_id"],
      "current",
      dt_files_to_read[i, "fname"],
      sep = "/"
    ))
    this_year <- year(Sys.Date())
    fname_annual <- fs::path(paste(
      dt_files_to_read[i, "pname_data"],
      dt_files_to_read[i, "site_id"],
      "annual",
      this_year,
      dt_files_to_read[i, "fname"],
      # paste0(fs::path_ext_remove(dt_files_to_read[i, "fname"]), "_", this_year, ".dat"),
      sep = "/"
    ))
    fname_complete <- fs::path(paste(
      dt_files_to_read[i, "pname_data"],
      dt_files_to_read[i, "site_id"],
      "complete",
      dt_files_to_read[i, "fname"],
      sep = "/"
    ))
    # create the directories if they don't exist
    fs::dir_create(fs::path_dir(fname_annual))
    fs::dir_create(fs::path_dir(fname_complete))

    # read the data
    dt <- import_campbell_data(fname_in)
    # dt <- subset_data(dt, last_date_to_process, n_days) # do we want to do this here?
    dt <- standardise_names(dt)
    # append the data
    combine_with_existing(dt, fname_annual = NULL, fname_complete = NULL)
    combine_with_existing(
      dt,
      fname_to_append_to = fname_annual,
      write_csv = FALSE
    )
    combine_with_existing(
      dt,
      fname_to_append_to = fname_complete,
      write_csv = FALSE
    )
  }
}
