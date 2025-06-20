# convert the CEDA data to level 1 data
library(targets)
library(data.table)
# Use UTC time throughout
Sys.setenv(TZ = "UTC")

# Packages that your targets need for their tasks.
v_packages <- c(
  "here",
  "fs",
  "units",
  "data.table",
  "stringr",
  "ecmwfr",
  "stars",
  "humidity",
  "ggplot2",
  "ggforce",
  "readxl",
  "openair",
  "mgcv"
)
# Set target options:
tar_option_set(
  packages = v_packages
)
tar_source()

# constants
dir_out <- "/gws/nopw/j04/eddystore/public"

list(
  # file names to monitor for changes
  tar_target(
    fname_names_db,
    here("data", "v_names_for_db.txt"),
    format = "file"
  ),
  tar_target(fname_names, here("data", "ERA5_to_ICOS.xlsx"), format = "file"),
  tar_target(
    v_fname,
    dir_ls(here("data-raw/UK-AMO/amo_ceda")),
    format = "file"
  ),
  tar_target(
    fname_era5,
    "/gws/nopw/j04/eddystore/era5met/data-raw/era5/N55.75_W03.25/monthly/dt.csv",
    format = "file"
  ),

  tar_target(
    v_names_for_db,
    read.table(fname_names_db, stringsAsFactors = FALSE)$V1
  ),
  tar_target(df_names, read_excel(fname_names, sheet = "ceda_to_mainmet")),
  tar_target(df_names_era5, read_excel(fname_names, sheet = "mainmet_to_era5")),

  # tar_target(df_era5, as.data.frame(fread(fname_era5))),
  tar_target(df_era5, rename_era5(fname_era5, df_names_era5)),

  # only read up to end of 2021, when ICOS starts
  tar_target(
    dt_lev0,
    ceda_to_lev0(
      v_fname = v_fname[1:27],
      v_names_for_db,
      df_names,
      write_csv = TRUE
    )
  ),
  tar_target(
    fname_plot,
    plot_lev0_vs_era5(
      dt = dt_lev0,
      df_era5,
      v_names = v_names_for_db[-1],
      avg.time = "1 hour"
    )
  ),
  tar_target(
    l_lev0,
    logger_to_lev0(
      dt_lev0,
      v_names_for_db,
      fname_out = "/gws/nopw/j04/eddystore/public/UK-AMO/UK-AMO_BM_pre2022.csv"
    )
  ),
  tar_target(
    l_lev0_sub,
    subset_by_date(
      l_lev0,
      start_date = "1995-01-01",
      end_date = "2021-12-31 23:59:59"
    )
  ),
  tar_target(
    v_fname_lev0,
    subset_by_year(
      l_lev0_sub,
      dir_out = dir_out,
      site_id = "UK-AMO",
      level = "lev0"
    )
  ),
  tar_target(
    l_lev1,
    log_lev0_to_lev1(l_lev0_sub, v_names_for_db, plot_graph = FALSE)
  ),
  tar_target(
    v_fname_lev1,
    subset_by_year(
      l_lev1,
      dir_out = dir_out,
      site_id = "UK-AMO",
      level = "lev1"
    )
  )
)
