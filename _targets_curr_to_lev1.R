# convert the historical logger data (for 2022-2024) to level 1 data
library(here)
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
  "here",
  "fs",
  "units",
  "data.table",
  "stringr",
  "ecmwfr",
  "powerjoin",
  "stars",
  "humidity",
  "ggplot2",
  "ggforce",
  "readr",
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
dir_in <- here("data-raw/UK-AMO/current")
dir_out <- "/gws/nopw/j04/eddystore/public"
this_year <- as.character(year(Sys.Date()))

list(
  # variable names to monitor for changes
  tar_target(
    fname_names_db,
    here("data", "v_names_for_db.txt"),
    format = "file"
  ),
  tar_target(
    fname_l_lev1_prev,
    here("_targets_hist_to_lev1/objects/l_lev1"),
    format = "file"
  ),

  tar_target(
    v_names_for_db,
    read.table(fname_names_db, stringsAsFactors = FALSE)$V1
  ),

  # read all current logger files; track changes in these files via targets so
  # they are re-run if they change.
  tar_target(
    v_fname_mm,
    get_logger_filenames(
      fname_pattern = "*Metmast_MainMet_30min*",
      dir_in = dir_in
    ),
    format = "file"
  ),
  tar_target(
    dt_mm,
    process_logger_files(
      v_fname = v_fname_mm,
      start.date = paste0(this_year, "-01-01 00:00:00")
    )
  ),
  tar_target(
    v_fname_L04_F01,
    get_logger_filenames(fname_pattern = "*_L04_F01*", dir_in = dir_in),
    format = "file"
  ),
  tar_target(
    dt_L04_F01,
    process_logger_files(
      v_fname = v_fname_L04_F01,
      start.date = paste0(this_year, "-01-01 00:00:00")
    )
  ),
  tar_target(
    v_fname_L04_F02,
    get_logger_filenames(fname_pattern = "*_L04_F02*", dir_in = dir_in),
    format = "file"
  ),
  tar_target(
    dt_L04_F02,
    process_logger_files(
      v_fname = v_fname_L04_F02,
      start.date = paste0(this_year, "-01-01 00:00:00")
    )
  ),
  tar_target(
    v_fname_L04_F04,
    get_logger_filenames(fname_pattern = "*_L04_F04*", dir_in = dir_in),
    format = "file"
  ),
  tar_target(
    dt_L04_F04,
    process_logger_files(
      v_fname = v_fname_L04_F04,
      start.date = paste0(this_year, "-01-01 00:00:00")
    )
  ),
  tar_target(
    v_fname_L04_F05,
    get_logger_filenames(fname_pattern = "*_L04_F05*", dir_in = dir_in),
    format = "file"
  ),
  tar_target(
    dt_L04_F05,
    process_logger_files(
      v_fname = v_fname_L04_F05,
      start.date = paste0(this_year, "-01-01 00:00:00")
    )
  ),
  tar_target(
    v_fname_L02_F01,
    get_logger_filenames(fname_pattern = "*_L02_F01*", dir_in = dir_in),
    format = "file"
  ),
  tar_target(
    dt_L02_F01,
    process_logger_files(
      v_fname = v_fname_L02_F01,
      start.date = paste0(this_year, "-01-01 00:00:00")
    )
  ),
  tar_target(
    v_fname_L03_F01,
    get_logger_filenames(fname_pattern = "*_L03_F01*", dir_in = dir_in),
    format = "file"
  ),
  tar_target(
    dt_L03_F01,
    process_logger_files(
      v_fname = v_fname_L03_F01,
      start.date = paste0(this_year, "-01-01 00:00:00")
    )
  ),
  tar_target(
    v_fname_L03_F02,
    get_logger_filenames(fname_pattern = "*_L03_F02*", dir_in = dir_in),
    format = "file"
  ),
  tar_target(
    dt_L03_F02,
    process_logger_files(
      v_fname = v_fname_L03_F02,
      start.date = paste0(this_year, "-01-01 00:00:00")
    )
  ),
  tar_target(
    v_fname_L05_F01,
    get_logger_filenames(fname_pattern = "*_L05_F01*", dir_in = dir_in),
    format = "file"
  ),
  tar_target(
    dt_L05_F01,
    process_logger_files(
      v_fname = v_fname_L05_F01,
      start.date = paste0(this_year, "-01-01 00:00:00")
    )
  ),
  tar_target(
    l_dt,
    list(
      dt_mm,
      dt_L04_F01,
      dt_L04_F02,
      dt_L04_F04,
      dt_L04_F05,
      dt_L02_F01,
      dt_L03_F01,
      dt_L03_F02,
      dt_L05_F01
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
    logger_to_lev0(
      dt_log,
      v_names_for_db,
      fname_out = "/gws/nopw/j04/eddystore/public/UK-AMO/UK-AMO_BM_2025.rds"
    )
  ),
  tar_target(
    v_fname_lev0,
    subset_by_year(l_lev0, dir_out = dir_out)
  ),
  tar_target(
    l_lev0_sub,
    subset_by_date(l_lev0, start_date = "2025-01-01", end_date = "2026-01-01")
  ),
  tar_target(
    l_lev1_curr,
    log_lev0_to_lev1(l_lev0_sub, v_names_for_db, plot_graph = FALSE)
  ),
  # tar_target(
  #   v_fname_lev1,
  #   subset_by_year(l_lev1_post2022, dir_out = dir_out, site_id = "UK-AMO", level = "lev1")
  # ),
  tar_target(
    l_lev1_prev,
    readRDS(fname_l_lev1_prev)
  ),
  tar_target(
    l_lev1,
    join_met(l_lev1_prev, l_lev1_curr)
  ),
  tar_target(
    dummy,
    saveRDS(l_lev1, file = paste0(dir_out, "/UK-AMO/lev1/UK-AMO_BM.rds"))
  ),
  tar_target(
    v_fname_lev1,
    subset_by_year(
      l_lev1_curr,
      dir_out = dir_out,
      site_id = "UK-AMO",
      level = "lev1"
    )
  )
)

# 310 secs from 2024-07-01
# 888 secs from 2022-01-01
