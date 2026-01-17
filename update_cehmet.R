here::i_am("update_cehmet.R")
library(here)
library(fs)
library(targets)

Sys.setenv(TAR_PROJECT = "curr_to_lev1")
tar_outdated()
system.time(tar_make(callr_function = NULL))
