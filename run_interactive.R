here::i_am("run_interactive.R")
sink("run_interactive_log.txt", append = FALSE, split = TRUE)
library(here)
library(fs)
library(targets)
library(data.table)
tar_source()
# tar_config_set(script = "_targets_ceda_to_lev1.R", store = "_targets_ceda_to_lev1", project = "ceda_to_lev1")
# tar_config_set(script = "_targets_hist_to_lev1.R", store = "_targets_hist_to_lev1", project = "hist_to_lev1")
# tar_config_set(script = "_targets_curr_to_lev1.R", store = "_targets_curr_to_lev1", project = "curr_to_lev1")

Sys.setenv(TAR_PROJECT = "ceda_to_lev1")
Sys.setenv(TAR_PROJECT = "hist_to_lev1")
Sys.setenv(TAR_PROJECT = "curr_to_lev1")
tar_config_get("script")
tar_config_get("store")
tar_outdated()
system.time(tar_make())
system.time(tar_make_future(workers = 10L))

source("_targets_curr_to_lev1.R")
lapply(v_packages, require, character.only = TRUE)
tar_load_everything(strict = FALSE)

tar_manifest()
tar_visnetwork()

detect_gaps(l_lev0$dt)
head(l_lev0$dt$DATECT)
tail(l_lev0$dt$DATECT)
detect_gaps(dt_mm, date_field = "date")
head(dt_mm$date)
tail(dt_mm$date)

test_plot <- function(l_lev1) {
  p <- ggplot(l_lev1$dt, aes(DATECT, TS, colour = as.factor(l_lev1$dt_qc$TS)))
  p <- p + geom_point()
  return(p)
}
test_plot(l_lev1)

sink()