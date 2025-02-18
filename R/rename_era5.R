rename_era5 <- function(fname_era5, df_names = df_names_era5, write_csv = FALSE) {
  df_era5 <- as.data.frame(fread(fname_era5))
  v_mainmet_name <- c("DATECT", df_names$mainmet_name)  # should be mainmet_name really?

  df_era5 <- drop_units(df_era5)
  df_era5$date <- df_era5$time
  df_era5$time <- NULL

  # depends on the mapping of ERA5 names to mainmet in the excel file
  df_mainmet_era5 <- df_era5[, df_names$era5_name]
  names(df_mainmet_era5) <- df_names$mainmet_name
  df_mainmet_era5$date <- df_era5$date
  df_era5 <- df_mainmet_era5

  ##* WIP: need to improve the switching around of date name
  df_era5$DATECT <- df_era5$date
  df_era5$date <- NULL

  # ensure df has all variables, and ordered
  df_era5 <- df_era5[, v_mainmet_name]
  return(df_era5)
}

make_hhourly_and_pad_era5 <- function(df_era5) {
    # remove duplicates
    df_era5 <- df_era5[!duplicated(df_era5[, "DATECT"], fromLast = TRUE), ]

    # fill in any missing timestamps
    df_era5 <- pad_data(df_era5, by = "60 min", v_dates = NULL)
    dim(df_era5)
    min(df_era5$DATECT); max(df_era5$DATECT)
    tail(df_era5$DATECT)

    # make half-hourly from hourly

    setnames(df_era5, "DATECT", "date")
    df_era5 <- timeAverage(df_era5, avg.time = "30 min", fill = TRUE)
    setnames(df_era5, "date", "DATECT")
    # write to file
    saveRDS(df_era5, file = here("data", "df_era5.rds"))
    return(df_era5)
}