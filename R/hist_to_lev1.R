get_logger_filenames <- function(
    fname_pattern = "*Metmast_MainMet_30min*",
    dir_in = here("data-raw/UK-AMO/archive")) {
  v_fname <- fs::dir_ls(dir_in, glob = fname_pattern)
  return(v_fname)
}

process_logger_files <- function(
  v_fname = NULL,
  fname_pattern = NULL,
  dir_in = NULL,
  check_time = TRUE,
  avg.time = "30 min", start.date = "2022-01-01 00:00:00") {

  # if not supplied, get the file names
  if (is.null(v_fname)) v_fname <- get_logger_filenames(fname_pattern, dir_in)

  if (check_time) v_fname <- lapply(v_fname, remove_time_errors)
  l_dt <- lapply(v_fname, import_campbell_data)
  l_dt <- lapply(l_dt, remove_excess_cols, remove_std = TRUE)
  l_dt <- lapply(l_dt, standardise_names)
  dt <- rbindlist(l_dt, use.names = TRUE, fill = TRUE)
  # remove duplicate times
  dt <- dt[!duplicated(dt[, "date"], fromLast = TRUE), ]
  setorder(dt, date)
  # subset to after start.date
  dt <- dt[date >= start.date]

  dt <- as.data.table(timeAverage(dt, avg.time = avg.time, start.date = start.date))
  return(dt)
}

subset_by_year <- function(
    l_lev, dir_out = "/gws/nopw/j04/eddystore/public",
    site_id = "UK-AMO", level = "lev0") {


  if ("DATECT" %!in% names(l_lev$dt))
    stop("Date variable named DATECT not present when subsetting by year")
  first_year <- l_lev$dt[, min(year(DATECT))]
  last_year  <- l_lev$dt[, max(year(DATECT))]
  v_year <- first_year:last_year

  # define a function to write out one year
  write_out <- function(i_year) {
    # the data
    dir_create(dir_out, site_id, level)
    fname <- path(dir_out, site_id, level, paste0(site_id, "_BM_dt_", i_year, ".csv"))
    fwrite(l_lev$dt[year(DATECT) == i_year], file = fname, dateTimeAs = "write.csv")
    # the qc codes
    fname <- path(dir_out, site_id, level, paste0(site_id, "_BM_qc_", i_year, ".csv"))
    fwrite(l_lev$dt_qc[year(DATECT) == i_year], file = fname, dateTimeAs = "write.csv")
    # the lot
    fname <- path(dir_out, site_id, level, paste0(site_id, "_BM_", i_year, ".rds"))
    l_lev$dt    <- l_lev$dt[year(DATECT) == i_year]
    l_lev$dt_qc <- l_lev$dt_qc[year(DATECT) == i_year]
    saveRDS(l_lev, file = fname)
    return(fname)
  }
  v_fname <- sapply(v_year, write_out)
  return(v_fname)
}

augment_logger <- function(dt) {
  dt <- calc_energy_balance(dt)
  dt <- remove_soil_heat_flux_cal(dt)
  dt <- adhoc_changes(dt)
  dt <- spatial_average_icos_vars(dt)
  return(dt)
}

remove_out_of_range <- function(dt) {
  # validate data with simple range check
  dt[TS            <    -10| TS            >   30, TS            := NA]
  dt[TS_4_1_1      <    -10| TS_4_1_1      >   30, TS_4_1_1      := NA]
  dt[SWC           <    30 | SWC           >  100, SWC           := NA]
  dt[SWC_4_1_1     <    30 | SWC_4_1_1     >  100, SWC_4_1_1     := NA]
  dt[SWC_4_2_1     <    30 | SWC_4_2_1     >  100, SWC_4_2_1     := NA]
  dt[SWC_4_3_1     <    30 | SWC_4_3_1     >  100, SWC_4_3_1     := NA]
  dt[SWC_4_4_1     <    30 | SWC_4_4_1     >  100, SWC_4_4_1     := NA]
  dt[G             <   -20 | G             >   20, G             := NA]
  dt[G_4_1_1       <   -20 | G_4_1_1       >   20, G_4_1_1       := NA]
  dt[G_4_1_2       <   -20 | G_4_1_2       >   20, G_4_1_2       := NA]
  dt[TA_4_1_1      <   -20 | TA_4_1_1      >   35, TA_4_1_1      := NA]
  dt[PA_4_1_1      <    92 | PA_4_1_1      >  120, PA_4_1_1      := NA]
  dt[RH_4_1_1      <    30                       , RH_4_1_1      := NA]
  dt[RH_4_1_1      >   100 & RH_4_1_1      <  120, RH_4_1_1      := 100]
  dt[RH_4_1_1      >=  120                       , RH_4_1_1      := NA]
  dt[SW_IN         <     0 | SW_IN         > 1200, SW_IN         := NA]
  dt[SW_OUT        <     0 | SW_OUT        >  200, SW_OUT        := NA]
  dt[LW_IN         <     0 | LW_IN         > 1200, LW_IN         := NA]
  dt[LW_OUT        <     0 | LW_OUT        > 1000, LW_OUT        := NA]
  dt[PPFD_IN_4_1_1 <     0 | PPFD_IN_4_1_1 > 2200, PPFD_IN_4_1_1 := NA]
  dt[P_12_1_1      <     0 | P_12_1_1      >   30, P_12_1_1      := NA]
  dt[WS_6_1_1      <     0 | WS_6_1_1      >   30, WS_6_1_1      := NA]
  dt[WD_6_1_1      <     0 | WD_6_1_1      >  360, WD_6_1_1      := NA]
  dt[D_SNOW        <     0 | D_SNOW        >  500, D_SNOW        := NA]

  return(dt)
}

logger_to_lev0 <- function(dt, v_names_for_db,
 fname_out = NULL) {

  setnames(dt, "date", "DATECT", skip_absent=TRUE)

  # check all variables are present
  # sum(v_names_for_db %in% names(dt))
  # sum(v_names_for_db %!in% names(dt))
  # v_names_for_db[v_names_for_db %in% names(dt)]
  # v_names_for_db[v_names_for_db %!in% names(dt)]
  # str_split_fixed(v_names_for_db, "_", n = 2)

  # make dt of variables for Main Met data set
  dt <- dt[, ..v_names_for_db]

  # make dt of qc values
  dt_qc <- copy(dt)
  # set qc to 1 if missing, 0 otherwise
  dt_qc[, v_names_for_db[-1] := lapply(.SD,
    function(x) as.numeric(is.na(x))), .SDcols = v_names_for_db[-1]]

  # create list of data tables containing raw data and qc codes
  l_lev0 <- list(dt = dt, dt_qc = dt_qc)

  # add in any missing date-timestamps
  # names(l_lev0$dt)
  # str(l_lev0$dt)
  # dim(l_lev0$dt); dim(l_lev0$dt_qc)
  l_lev0$dt       <- pad_data(l_lev0$dt)
  l_lev0$dt_qc    <- pad_data(l_lev0$dt_qc)
  # dim(l_lev0$dt); dim(l_lev0$dt_qc)

  # write to file
  if (!is.null(fname_out)) saveRDS(l_lev0, file = fname_out)
  return(l_lev0)
}

# l_lev <- subset_by_date(l_lev0)

log_lev0_to_lev1 <- function(l_lev, v_names_for_db, plot_graph = FALSE) {
##* WIP: I got thid far. This function needs to be gone through.

  # before range check
  # sum(is.na(l_lev$dt))

  # set out-of-range values to missing
  l_lev$dt <- remove_out_of_range(l_lev$dt)
  l_lev$dt_qc <- copy(l_lev$dt)
  # set qc to 1 if missing, 0 otherwise
  l_lev$dt_qc[, v_names_for_db[-1] := lapply(.SD,
    function(x) as.numeric(is.na(x))), .SDcols = v_names_for_db[-1]]

  # after range check, before gap-filling; both the below lines should give the same
  # sum(is.na(l_lev$dt))
  # sum(l_lev$dt_qc[, -1])

  # fill in the gaps by imputation
  # plot_graph = TRUE # TRUE # FALSE
  # set night-time values to zero
  l_lev <- impute(y = "SW_IN", l_met = l_lev, method = "nightzero", plot_graph = plot_graph)
  l_lev <- impute(y = "SW_OUT", l_met = l_lev, method = "nightzero", plot_graph = plot_graph)
  l_lev <- impute(y = "PPFD_IN_4_1_1", l_met = l_lev, method = "nightzero", plot_graph = plot_graph)
  l_lev <- impute(y = "PPFD_OUT", l_met = l_lev, method = "nightzero", plot_graph = plot_graph)
  l_lev <- impute(y = "PPFD_DIF", l_met = l_lev, method = "nightzero", plot_graph = plot_graph)
  l_lev <- impute(y = "RG_4_1_0", l_met = l_lev, method = "nightzero", plot_graph = plot_graph)

  # impute by smoothing regression with time
  l_lev <- impute(y = "TS", l_met = l_lev, method = "time", k = 20, plot_graph = plot_graph)
  l_lev <- impute(y = "SWC", l_met = l_lev, method = "time", k = 20, plot_graph = plot_graph)
  l_lev <- impute(y = "G", l_met = l_lev, method = "time", k = 20, plot_graph = plot_graph)
  l_lev <- impute(y = "WTD", l_met = l_lev, method = "time", k = 20, plot_graph = plot_graph)
  ##l_lev <- impute(y = "WTD_4_1_1", l_met = l_lev, method = "time", k = 20, plot_graph = plot_graph)
  l_lev <- impute(y = "PA_4_1_1", l_met = l_lev, method = "time", k = 20, plot_graph = plot_graph)
  l_lev <- impute(y = "SW_IN", l_met = l_lev, method = "time", k = 20,  qc_tokeep = c(0, 4), plot_graph = plot_graph)
  l_lev <- impute(y = "SW_OUT", l_met = l_lev, method = "time", k = 20, qc_tokeep = c(0, 4), plot_graph = plot_graph)
  l_lev <- impute(y = "LW_IN", l_met = l_lev, method = "time", k = 20, plot_graph = plot_graph)
  l_lev <- impute(y = "LW_OUT", l_met = l_lev, method = "time", k = 20, plot_graph = plot_graph)
  l_lev <- impute(y = "PPFD_IN_4_1_1", l_met = l_lev, method = "time", k = 20, qc_tokeep = c(0, 4), plot_graph = plot_graph)
  l_lev <- impute(y = "PPFD_OUT", l_met = l_lev, method = "time", k = 20, qc_tokeep = c(0, 4), plot_graph = plot_graph)
  l_lev <- impute(y = "PPFD_DIF", l_met = l_lev, method = "time", k = 20, qc_tokeep = c(0, 4), plot_graph = plot_graph)
  # not approp for rain
  #l_lev <- impute(y = "P_12_1_1", l_met = l_lev, method = "time", k = 20, plot_graph = plot_graph)
  l_lev <- impute(y = "WS_6_1_1", l_met = l_lev, method = "time", k = 20, plot_graph = plot_graph)
  l_lev <- impute(y = "WD_6_1_1", l_met = l_lev, method = "time", k = 20, plot_graph = plot_graph)
  # snow depth is usually zero - data are often just noise
  l_lev <- impute(y = "D_SNOW", l_met = l_lev, method = "zero", plot_graph = plot_graph)
  l_lev <- impute(y = "RN_5_1_1", l_met = l_lev, method = "time", k = 20, plot_graph = plot_graph)
  l_lev <- impute(y = "TS_4_1_1", l_met = l_lev, method = "time", k = 20, plot_graph = plot_graph)
  l_lev <- impute(y = "TS_4_2_1", l_met = l_lev, method = "time", k = 20, plot_graph = plot_graph)
  l_lev <- impute(y = "TS_4_3_1", l_met = l_lev, method = "time", k = 20, plot_graph = plot_graph)
  l_lev <- impute(y = "TS_4_4_1", l_met = l_lev, method = "time", k = 20, plot_graph = plot_graph)
  l_lev <- impute(y = "SWC_4_1_1", l_met = l_lev, method = "time", k = 20, plot_graph = plot_graph)
  l_lev <- impute(y = "SWC_4_2_1", l_met = l_lev, method = "time", k = 20, plot_graph = plot_graph)
  l_lev <- impute(y = "SWC_4_3_1", l_met = l_lev, method = "time", k = 20, plot_graph = plot_graph)
  l_lev <- impute(y = "SWC_4_4_1", l_met = l_lev, method = "time", k = 20, plot_graph = plot_graph)
  l_lev <- impute(y = "G_4_1_1", l_met = l_lev, method = "time", k = 20, plot_graph = plot_graph)
  l_lev <- impute(y = "G_4_1_2", l_met = l_lev, method = "time", k = 20, plot_graph = plot_graph)
  l_lev <- impute(y = "TA_4_1_1", l_met = l_lev, method = "time", k = 20, plot_graph = plot_graph)
  l_lev <- impute(y = "RH_4_1_1", l_met = l_lev, method = "time", k = 20, plot_graph = plot_graph)
  l_lev <- impute(y = "RG_4_1_0", l_met = l_lev, method = "time", k = 20, qc_tokeep = c(0, 4), plot_graph = plot_graph)
  l_lev <- impute(y = "NDVI_649IN_5_1_1", l_met = l_lev, method = "time", k = 20, plot_graph = plot_graph)
  l_lev <- impute(y = "NDVI_649OUT_5_1_1", l_met = l_lev, method = "time", k = 20, plot_graph = plot_graph)
  l_lev <- impute(y = "NDVI_797IN_5_1_1", l_met = l_lev, method = "time", k = 20, plot_graph = plot_graph)
  l_lev <- impute(y = "NDVI_797OUT_5_1_1", l_met = l_lev, method = "time", k = 20, plot_graph = plot_graph)
  l_lev <- impute(y = "LWS_4_1_1", l_met = l_lev, method = "time", k = 20, plot_graph = plot_graph)
  l_lev <- impute(y = "LWS_4_1_2", l_met = l_lev, method = "time", k = 20, plot_graph = plot_graph)

  # # after gap-filling
  # summary(l_lev$dt)
  # sum(is.na(l_lev$dt))

  # add name of validator - later replaced by username when manually checked
  l_lev$dt_qc$validator <- "auto"
  # some values in 2021 produce NAs in qc
  # do we need this? PL 2025-02-08
  # l_lev$dt_qc[(is.na(l_lev$dt_qc))] <- 1

  return(l_lev)
}

# l_lev <- subset_by_date(l_lev0)

subset_by_date <- function(
    l_lev, start_date = "2025-01-01", end_date = "2025-01-31") {
  start_date <- as.POSIXct(start_date)
  end_date   <- as.POSIXct(end_date)
  if ("DATECT" %!in% names(l_lev$dt))
    stop("Date variable named DATECT not present when subsetting by date")
    l_lev$dt    <- l_lev$dt[DATECT    >= start_date & DATECT <= end_date]
    l_lev$dt_qc <- l_lev$dt_qc[DATECT >= start_date & DATECT <= end_date]
  return(l_lev)
}

join_met <- function(l_met1, l_met2) {
    l_met2$dt    <- power_full_join(l_met1$dt, l_met2$dt,
      by = "DATECT", conflict = coalesce_yx)
    l_met2$dt_qc <- power_full_join(l_met1$dt_qc, l_met2$dt_qc,
      by = "DATECT", conflict = coalesce_yx)
  return(l_met2)
}
