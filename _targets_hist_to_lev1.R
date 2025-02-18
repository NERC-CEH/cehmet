# convert the historical logger data (for 2022-2024) to level 1 data
library(targets)
library(future)
library(data.table)
future::plan("multicore")

# Use UTC time throughout
Sys.setenv(TZ = "UTC")
"%!in%" <- Negate("%in%")
"%!like%" <- Negate("%like%")

# Packages that your targets need for their tasks.
v_packages <- c(
  "here", "fs", "units", "data.table", "stringr", "ecmwfr", "powerjoin",
  "stars", "humidity", "ggplot2", "ggforce", "readr", "readxl", "openair", "mgcv"
)

# Set target options:
tar_option_set(
  packages = v_packages
)
tar_source()

# constants
dir_out <- "/gws/nopw/j04/eddystore/public"

list(
  # variable names to monitor for changes
  tar_target(fname_names_db,
    here("data", "v_names_for_db.txt"),
    format = "file"
  ),
  tar_target(fname_l_lev1_pre2022, here("_targets_ceda_to_lev1/objects/l_lev1"),
    format = "file"),
  tar_target(fname_names, here("data", "ERA5_to_ICOS.xlsx"), format = "file"),
  tar_target(fname_era5,
    "/gws/nopw/j04/eddystore/era5met/data-raw/era5/N55.75_W03.25/monthly/dt.csv",
    format = "file"
  ),
  tar_target(
    v_names_for_db,
    read.table(fname_names_db, stringsAsFactors = FALSE)$V1
  ),
  tar_target(df_names, read_excel(fname_names, sheet = "ceda_to_mainmet")),
  tar_target(df_names_era5, read_excel(fname_names, sheet = "mainmet_to_era5")),

  tar_target(df_era5, rename_era5(fname_era5, df_names_era5)),

  # read all files from Oct 2021, when ICOS starts
  tar_target(
    dt_mm,
    process_logger_files(fname_pattern = "*Metmast_MainMet_30min*",
      dir_in = here("data-raw/UK-AMO/archive"))
  ),
  tar_target(
    dt_L04_F01,
    process_logger_files(fname_pattern = "*_L04_F01*",
      dir_in = here("data-raw/UK-AMO/archive"))
  ),
  tar_target(
    dt_L04_F02,
    process_logger_files(fname_pattern = "*_L04_F02*",
      dir_in = here("data-raw/UK-AMO/archive"))
  ),
  tar_target(
    dt_L04_F03,
    process_logger_files(fname_pattern = "*_L04_F03*",
      dir_in = here("data-raw/UK-AMO/archive"))
  ),
  tar_target(
    dt_L04_F04,
    process_logger_files(fname_pattern = "*_L04_F04*",
      dir_in = here("data-raw/UK-AMO/archive"))
  ),
  tar_target(
    dt_L04_F05,
    process_logger_files(fname_pattern = "*_L04_F05*",
      dir_in = here("data-raw/UK-AMO/archive"))
  ),
  tar_target(
    dt_L02_F01,
    process_logger_files(fname_pattern = "*_L02_F01*",
      dir_in = here("data-raw/UK-AMO/archive"))
  ),
  tar_target(
    dt_L03_F01,
    process_logger_files(fname_pattern = "*_L03_F01*",
      dir_in = here("data-raw/UK-AMO/archive"))
  ),
  tar_target(
    dt_L03_F02,
    process_logger_files(fname_pattern = "*_L03_F02*",
      dir_in = here("data-raw/UK-AMO/archive"))
  ),
  tar_target(
    dt_L05_F01,
    process_logger_files(fname_pattern = "*_L05_F01*",
      dir_in = here("data-raw/UK-AMO/archive"))
  ),
  tar_target(
    l_dt,
    list(
      dt_mm, dt_L04_F01, dt_L04_F02, dt_L04_F03, dt_L04_F04, dt_L04_F05,
      dt_L02_F01, dt_L03_F01, dt_L03_F02, dt_L05_F01
    )
  ),
  tar_target(
    dt_log0,
    power_left_join(l_dt, by = "date", conflict = coalesce_xy)
  ),
  tar_target(
    dt_log,
    augment_logger(dt_log0)
  ),
  tar_target(
    l_lev0,
    logger_to_lev0(dt_log, v_names_for_db,
      fname_out = "/gws/nopw/j04/eddystore/public/UK-AMO/UK-AMO_BM_post2022.rds")
  ),
  tar_target(
    v_fname_lev0,
    subset_by_year(l_lev0, dir_out = dir_out,
      site_id = "UK-AMO", level = "lev0")
  ),
  tar_target(
    l_lev0_sub,
    subset_by_date(l_lev0, start_date = "2022-01-01", end_date = "2025-01-31")
  ),
  tar_target(
    l_lev1_post2022,
    log_lev0_to_lev1(l_lev0_sub, v_names_for_db, plot_graph = FALSE)
  ),
  tar_target(
    v_fname_lev1,
    subset_by_year(l_lev1_post2022, dir_out = dir_out,
      site_id = "UK-AMO", level = "lev1")
  ),
  tar_target(
    l_lev1_pre2022,
    readRDS(fname_l_lev1_pre2022)
  ),
  tar_target(
    l_lev1,
    join_met(l_lev1_pre2022, l_lev1_post2022)
  )
)

# 310 secs from 2024-07-01
# 888 secs from 2022-01-01

