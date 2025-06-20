ceda_to_lev0 <- function(v_fname, v_names_for_db, df_names, write_csv = FALSE) {
  l_dt <- lapply(v_fname, fread, skip = 202)

  # remove extra cols from 2018 on
  l_dt[[23]] <- l_dt[[23]][, 1:55]
  l_dt[[24]] <- l_dt[[24]][, 1:55]
  l_dt[[25]] <- l_dt[[25]][, 1:55]
  l_dt[[26]] <- l_dt[[26]][, 1:55]
  l_dt[[27]] <- l_dt[[27]][, 1:55]
  dt <- rbindlist(l_dt)

  # map the ceda names to those we want in mainmet
  # this depends on the mapping in df_names
  # all the unwanted flags and gf data remain as "Vnn"
  names(dt) <- df_names$mainmet_name
  v_mainmet_names_in_ceda <- names(dt)[names(dt) %in% df_names$mainmet_name]

  # keep only those we want in mainmet - discard all the flags and gf data
  dt <- dt[, ..v_mainmet_names_in_ceda]
  # convert data type of date column
  dt$DATECT <- as.POSIXct(dt$DATECT, format = "%d/%m/%Y %H:%M")

  # add the variables missing from CEDA but wanted in MAINMET
  names(dt) %in% v_names_for_db
  # list MAINMET variables present
  v_names_for_db[v_names_for_db %in% names(dt)]
  # list MAINMET variables that are missing
  v_missing <- v_names_for_db[v_names_for_db %!in% names(dt)]
  # add these to the data table
  dt[, c(v_missing) := NA]
  # ensure df has all variables, and ordered
  dt <- dt[, ..v_names_for_db]

  # if all missing, these appear as logical; convert to numeric
  dt <- dt[, lapply(.SD, as.numeric)]
  # the above converts date columns to numeric; convert back to POSIXct
  dt[, DATECT := as.POSIXct(dt$DATECT, origin = "1970-01-01")]

  # some ad hoc unit conversions
  dt$PA_4_1_1 <- dt$PA_4_1_1 / 10 # hPa to kPa
  dt$D_SNOW <- dt$D_SNOW / 10 # mm to cm

  # fill in missing timestamps
  dt <- as.data.table(pad_data(dt, v_dates = NULL))
  dt[is.na(dt)] <- NA # seems to be needed in case of NaN

  # write out the CEDA data in mainmet format before any QC
  ##* WIP Should we add a df_qc with all zeroes?
  if (write_csv) {
    fwrite(dt, file = here("data", "UK-Amo_BM_lev0.csv"))
  }
  return(dt)
}

plot_lev0_vs_era5 <- function(
  dt,
  df_era5,
  v_names = v_names_for_db[-1],
  avg.time = "1 week"
) {
  setnames(dt, "DATECT", "date")
  setnames(df_era5, "DATECT", "date")
  if (!is.na(avg.time)) {
    dt <- as.data.table(timeAverage(dt, avg.time = avg.time, fill = TRUE))
    df_era5 <- as.data.table(timeAverage(
      df_era5,
      avg.time = avg.time,
      fill = TRUE
    ))
  }
  df_era5 <- df_era5[date > min(dt$date) & date < max(dt$date)]

  for (var_name in v_names) {
    p <- ggplot(dt, aes(date, get(var_name))) + geom_point(colour = "red")
    p <- p +
      geom_line(data = df_era5, aes(date, get(var_name)), colour = "black")
    p <- p + labs(title = paste(var_name, ": red = raw, black = era5"))
    fname <- paste0("plot_", var_name, "_vs_era5.png")
    pname <- here("output", "ceda_vs_era5", gsub(" ", "_", avg.time))
    fs::dir_create(pname)
    # remove spaces from variable name
    ggsave(p, filename = fs::path(pname, fname))
  }
  return(fname)
}

convert_lev0_to_lev1 <- function(dt, df_era5, write_csv = FALSE) {
  # validate data with simple range check
  dt[TS < -10 | TS > 30, TS := NA]
  dt[TS_4_1_1 < -10 | TS_4_1_1 > 30, TS_4_1_1 := NA]
  dt[SWC < 30 | SWC > 100, SWC := NA]
  dt[SWC_4_1_1 < 30 | SWC_4_1_1 > 100, SWC_4_1_1 := NA]
  dt[G < -20 | G > 20, G := NA]
  dt[G_4_1_1 < -20 | G_4_1_1 > 20, G_4_1_1 := NA]
  dt[G_4_1_2 < -20 | G_4_1_2 > 20, G_4_1_2 := NA]
  dt[TA_4_1_1 < -20 | TA_4_1_1 > 35, TA_4_1_1 := NA]
  dt[PA_4_1_1 < 92 | PA_4_1_1 > 120, PA_4_1_1 := NA]
  dt[RH_4_1_1 < 30, RH_4_1_1 := NA]
  dt[RH_4_1_1 > 100 & RH_4_1_1 < 120, RH_4_1_1 := 100]
  dt[RH_4_1_1 >= 120, RH_4_1_1 := NA]
  dt[SW_IN < 0 | SW_IN > 1200, SW_IN := NA]
  dt[SW_OUT < 0 | SW_OUT > 200, SW_OUT := NA]
  dt[LW_IN < 0 | LW_IN > 1200, LW_IN := NA]
  dt[LW_OUT < 0 | LW_OUT > 1000, LW_OUT := NA]
  dt[PPFD_IN_4_1_1 < 0 | PPFD_IN_4_1_1 > 2200, PPFD_IN_4_1_1 := NA]
  dt[P_12_1_1 < 0 | P_12_1_1 > 30, P_12_1_1 := NA]
  dt[WS_6_1_1 < 0 | WS_6_1_1 > 30, WS_6_1_1 := NA]
  dt[WD_6_1_1 < 0 | WD_6_1_1 > 360, WD_6_1_1 := NA]
  dt[D_SNOW < 0 | D_SNOW > 500, D_SNOW := NA]

  ##* WIP ultimately we want to keep this as a data table
  df <- as.data.frame(dt)
  # initialise QC code data frame
  df_qc <- df
  # convert missing values to 0 = raw & 1 = missing;
  # "-1" here means skip first column - the date
  df_qc[, -1] <- as.numeric(is.na(df[, -1]))

  l_lev1 <- list(df = df, df_qc = df_qc)

  # add name of validator as a new variable - the above range checks correspond to
  # validation that happens automatically, hence = "auto"
  # later replaced by username in metqc app after manual check
  l_lev1$df_qc$validator <- "auto"

  var_name = "TS_4_1_1"
  p <- ggplot(l_lev1$df, aes(DATECT, get(var_name)))
  p <- p + geom_line()
  p
  plot_with_qc(y = var_name, l_lev1)
  summary(l_lev1$df[, var_name])
  sum(is.na(l_lev1$df[, var_name]))
  #l_lev1$df[which(is.na(l_lev1$df[, var_name])), ]

  # list of data frames containing gap-filled data and qc codes
  # ERA5 works well
  plot_graph = TRUE # FALSE # TRUE
  # get the point measurements complete first
  # ERA5 works ok, but straight replacement better
  l_lev1 <- impute(
    y = "P_12_1_1",
    l_met = l_lev1,
    method = "era5",
    fit = FALSE,
    df_era5 = as.data.frame(df_era5),
    plot_graph = plot_graph
  )
  l_lev1 <- impute(
    y = "WD_6_1_1",
    l_met = l_lev1,
    method = "era5",
    fit = FALSE,
    df_era5 = df_era5,
    plot_graph = plot_graph
  )
  l_lev1 <- impute(
    y = "SW_IN",
    l_met = l_lev1,
    method = "era5",
    fit = FALSE,
    df_era5 = df_era5,
    plot_graph = plot_graph
  )
  l_lev1 <- impute(
    y = "SW_IN",
    l_met = l_lev1,
    method = "regn",
    qc_tokeep = c(0, 7),
    x = "TS_4_1_1",
    plot_graph = plot_graph
  )
  l_lev1 <- impute(
    y = "SW_OUT",
    l_met = l_lev1,
    method = "era5",
    fit = TRUE,
    df_era5 = df_era5,
    plot_graph = plot_graph
  )
  l_lev1 <- impute(
    y = "LW_IN",
    l_met = l_lev1,
    method = "era5",
    fit = FALSE,
    df_era5 = df_era5,
    plot_graph = plot_graph
  )
  l_lev1 <- impute(
    y = "LW_OUT",
    l_met = l_lev1,
    method = "era5",
    fit = FALSE,
    df_era5 = df_era5,
    plot_graph = plot_graph
  )
  l_lev1 <- impute(
    y = "RG_4_1_0",
    l_met = l_lev1,
    method = "era5",
    fit = FALSE,
    df_era5 = df_era5,
    plot_graph = plot_graph
  )

  # use ERA5 regression
  l_lev1 <- impute(
    y = "TA_4_1_1",
    l_met = l_lev1,
    method = "era5",
    df_era5 = df_era5,
    plot_graph = plot_graph
  )
  l_lev1 <- impute(
    y = "RH_4_1_1",
    l_met = l_lev1,
    method = "era5",
    df_era5 = df_era5,
    plot_graph = plot_graph
  )
  l_lev1 <- impute(
    y = "TS_4_1_1",
    l_met = l_lev1,
    method = "era5",
    df_era5 = df_era5,
    plot_graph = plot_graph
  )
  l_lev1 <- impute(
    y = "PA_4_1_1",
    l_met = l_lev1,
    method = "era5",
    df_era5 = df_era5,
    plot_graph = plot_graph
  )
  l_lev1 <- impute(
    y = "PPFD_DIF",
    l_met = l_lev1,
    method = "era5",
    df_era5 = df_era5,
    plot_graph = plot_graph
  )
  l_lev1 <- impute(
    y = "WS_6_1_1",
    l_met = l_lev1,
    method = "era5",
    df_era5 = df_era5,
    plot_graph = plot_graph
  )
  l_lev1 <- impute(
    y = "NDVI_649IN_5_1_1",
    l_met = l_lev1,
    method = "era5",
    df_era5 = df_era5,
    plot_graph = plot_graph
  )
  l_lev1 <- impute(
    y = "NDVI_649OUT_5_1_1",
    l_met = l_lev1,
    method = "era5",
    df_era5 = df_era5,
    plot_graph = plot_graph
  )
  l_lev1 <- impute(
    y = "NDVI_797IN_5_1_1",
    l_met = l_lev1,
    method = "era5",
    df_era5 = df_era5,
    plot_graph = plot_graph
  )
  l_lev1 <- impute(
    y = "NDVI_797OUT_5_1_1",
    l_met = l_lev1,
    method = "era5",
    df_era5 = df_era5,
    plot_graph = plot_graph
  )
  # For RN, CEDA data dodgy (just wrong col? never <0) - check;
  # better to replace pre-icos data with era5; for now, replace all with qc_tokeep = "" (so even replace raw data)
  # or recalc as balance of in-out
  l_lev1 <- impute(
    y = "RN_5_1_1",
    l_met = l_lev1,
    qc_tokeep = "",
    method = "era5",
    df_era5 = df_era5,
    fit = FALSE,
    plot_graph = plot_graph
  )

  # use other covariate regression
  l_lev1 <- impute(
    y = "G_4_1_1",
    l_met = l_lev1,
    method = "time",
    plot_graph = plot_graph
  )
  l_lev1 <- impute(
    y = "G_4_1_2",
    l_met = l_lev1,
    method = "regn",
    x = "G_4_1_1",
    plot_graph = plot_graph
  )
  l_lev1 <- impute(
    y = "PPFD_IN_4_1_1",
    l_met = l_lev1,
    method = "regn",
    x = "SW_IN",
    plot_graph = plot_graph
  )
  l_lev1 <- impute(
    y = "PPFD_OUT",
    l_met = l_lev1,
    method = "regn",
    x = "PPFD_IN_4_1_1",
    plot_graph = plot_graph
  )
  l_lev1 <- impute(
    y = "PPFD_DIF",
    l_met = l_lev1,
    method = "regn",
    x = "PPFD_IN_4_1_1",
    plot_graph = plot_graph
  )
  # fill in remaining gaps
  l_lev1 <- impute(
    y = "PPFD_IN_4_1_1",
    l_met = l_lev1,
    method = "regn",
    qc_tokeep = c(0, 3),
    x = "TS_4_1_1",
    plot_graph = plot_graph
  )
  l_lev1 <- impute(
    y = "PPFD_IN_4_1_1",
    l_met = l_lev1,
    method = "time",
    qc_tokeep = c(0, 3),
    plot_graph = plot_graph
  )

  # soil temperature profile: use top TS in covariate regression
  l_lev1 <- impute(
    y = "TS_4_2_1",
    l_met = l_lev1,
    method = "regn",
    x = "TS_4_1_1",
    plot_graph = plot_graph
  )
  l_lev1 <- impute(
    y = "TS_4_3_1",
    l_met = l_lev1,
    method = "regn",
    x = "TS_4_1_1",
    plot_graph = plot_graph
  )
  l_lev1 <- impute(
    y = "TS_4_4_1",
    l_met = l_lev1,
    method = "regn",
    x = "TS_4_1_1",
    plot_graph = plot_graph
  )

  # soil water profile: use top SWC in covariate regression
  l_lev1 <- impute(
    y = "SWC_4_1_1",
    l_met = l_lev1,
    method = "era5",
    df_era5 = df_era5,
    plot_graph = plot_graph
  )
  l_lev1 <- impute(
    y = "SWC_4_2_1",
    l_met = l_lev1,
    method = "regn",
    x = "SWC_4_1_1",
    plot_graph = plot_graph
  )
  l_lev1 <- impute(
    y = "SWC_4_3_1",
    l_met = l_lev1,
    method = "regn",
    x = "SWC_4_1_1",
    plot_graph = plot_graph
  )
  l_lev1 <- impute(
    y = "SWC_4_4_1",
    l_met = l_lev1,
    method = "regn",
    x = "SWC_4_1_1",
    plot_graph = plot_graph
  )

  l_lev1 <- impute(
    y = "SW_IN",
    l_met = l_lev1,
    method = "regn",
    x = "TS_4_1_1",
    plot_graph = plot_graph
  )

  # doesnt really work with ERA5
  l_lev1 <- impute(
    y = "WTD_4_1_1",
    l_met = l_lev1,
    method = "era5",
    df_era5 = df_era5,
    plot_graph = plot_graph
  )

  # for spatial means:
  # do regression with point variables present in CEDA
  # then do regression with ERA5 data
  l_lev1 <- impute(
    y = "TS",
    l_met = l_lev1,
    method = "regn",
    x = "TS_4_1_1",
    plot_graph = plot_graph
  )
  l_lev1 <- impute(
    y = "G",
    l_met = l_lev1,
    method = "regn",
    fit = FALSE,
    x = "G_4_1_1",
    plot_graph = plot_graph
  )
  l_lev1 <- impute(
    y = "SWC",
    l_met = l_lev1,
    method = "regn",
    fit = TRUE,
    x = "SWC_4_1_1",
    plot_graph = plot_graph
  )
  l_lev1 <- impute(
    y = "WTD",
    l_met = l_lev1,
    method = "regn",
    fit = FALSE,
    x = "WTD_4_1_1",
    plot_graph = plot_graph
  )

  # snow is difficult: no CEDA data, recent data dodgy, ERA5 looks too high
  # best available is replace with ERA5
  l_lev1 <- impute(
    y = "D_SNOW",
    l_met = l_lev1,
    qc_tokeep = "",
    method = "era5",
    df_era5 = df_era5,
    fit = FALSE,
    plot_graph = plot_graph
  )

  saveRDS(l_lev1, file = here("data", "UK-Amo_mainmet_lev1.rds"))
  # fwrite(l_lev1$df,    file = here("data", "UK-Amo_mainmet_val.csv"))
  # fwrite(l_lev1$df_qc, file = here("data", "UK-Amo_mainmet_val_qc.csv"))
  dim(l_lev1$df_qc)

  return(dt)
}
