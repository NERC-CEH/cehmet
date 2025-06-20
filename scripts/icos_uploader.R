# R script to:
#   read ICOS logger BM files,
#   subset to day of interest,
#   write daily files, and
#   upload daily files to ICOS server, with MD5 hash

# Peter Levy
# Centre for Ecology & Hydrology, Edinburgh, U.K.
# plevy@ceh.ac.uk

rm(list = ls())
library(fs)
library(readr)
library(stringr)
library(tools)
library(yaml)
library(data.table)
# Use UTC time throughout
Sys.setenv(TZ = "UTC")
source("/gws/nopw/j04/ceh_generic/amo_met/R/amo_met.R")
# user & password credentials stored in file readable by user only
cred <- yaml.load_file("/gws/nopw/j04/ceh_generic/amo_met/credentials.yml")

# define constants
do_upload <- TRUE
force_upload <- FALSE
overwrite <- FALSE
station_code <- "UK-AMo_BM_" # actually includes "BM" for bio-met data
ext <- ".dat"

# usually constant
# dir_in  <- "/gws/nopw/j04/ceh_generic/amo_met/server_mirror/complete"
dir_in <- paste0(
  "/gws/nopw/j04/ceh_generic/amo_met/server_mirror/complete/",
  format(Sys.Date(), "%Y")
) # Now looking at the current year folder
dir_out <- "/gws/nopw/j04/dare_uk/public/UK-AMo/daily"

# process n_days prior to ending_n_days_ago, usually yesterday (= 0)
n_days <- 14 # number of days to process
ending_n_days_ago <- 0 # 0 = yesterday

first_date_to_process <- as.POSIXlt(Sys.Date() - ending_n_days_ago - n_days)
v_date_to_process <- as.POSIXlt(seq(
  from = first_date_to_process,
  length.out = n_days,
  by = "days"
))
length(v_date_to_process)
v_date_to_process

# make a vector of logger-file ids and loop over these
v_logger_id <- c("_L02", "_L03", "_L03", "_L04", "_L04", "_L04", "_L04", "_L05")
v_file_id <- c("_F01", "_F01", "_F02", "_F01", "_F02", "_F04", "_F05", "_F01")
v_fname_in <- paste0("UK-AMo_BM", v_logger_id, v_file_id, ".dat")
v_fname_tc <- paste0(path_ext_remove(v_fname_in), "_timechecked.dat")

for (i_file in 1:length(v_fname_in)) {
  pname_in <- path(dir_in, v_fname_in[i_file])
  pname_tc <- path(dir_in, v_fname_tc[i_file])
  v_lines <- remove_time_errors(pname_in)
  write_lines(v_lines, file = pname_tc)
}

for (i_day in 1:length(v_date_to_process)) {
  date_to_process <- v_date_to_process[i_day]
  for (i_file in 1:length(v_fname_in)) {
    pname_daily <- get_pathname_daily(
      date_to_process,
      dir_out,
      station_code,
      v_logger_id[i_file],
      v_file_id[i_file],
      ext
    )

    file_already_existed <- file.exists(pname_daily)
    print(paste(
      path_file(pname_daily),
      "already existed:",
      file_already_existed
    ))

    # write output only if file does not already exist or we want to overwrite
    if (!file_already_existed | overwrite == TRUE) {
      print(paste("Writing daily file:", pname_daily))
      written <- write_daily_file(
        date_to_process,
        dir_in,
        v_fname_tc[i_file],
        pname_daily
      )
    }
    # upload to the ICOS server only if file does not already exist and we want
    # to upload, OR we want to force uploading if it has failed previously

    # if ((!file_already_existed & do_upload == TRUE) | force_upload == TRUE) {
    #   err <- upload_daily_file(pname_daily) # have 3 goes - before I can get this sorted.
    #   if(err != 0){
    #     err <- upload_daily_file(pname_daily)
    #   }
    #   if(err != 0){
    #     err <- upload_daily_file(pname_daily)
    #   }
    #   if(err != 0){ # if it is still failing then remove the daily file - so it can be caught again the next day.
    #     unlink(pname_daily)
    #   }

    if ((!file_already_existed & do_upload == TRUE) | force_upload == TRUE) {
      err <- upload_daily_file(pname_daily) # have 3 goes - before I can get this sorted.
      if (err != 0) {
        # if it fails then remove the daily file - so it can be caught again the next day(s).
        unlink(pname_daily)
      }
    } else {
      print(paste("Not uploading daily file:", pname_daily))
    }
  } # i_file
} # i_day

# exit gracefully
quit("no")
