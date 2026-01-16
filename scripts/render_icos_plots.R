library(rmarkdown)
render(
  "/gws/ssde/j25a/eddystore/cehmet/scripts/plot_UK-AMo_met_JC.Rmd",
  #  output_file = "/gws/nopw/j04/dare_uk/public/plevy/UK-AMo/plot_UK-AMo_year.html",
  output_file = "/gws/nopw/j04/ukem/public/UK-AMo/plot_UK-AMo_year.html",
  params = list(
    n_days = 365,
    #dir_out_mainmet = "/gws/nopw/j04/dare_uk/public/plevy/UK-AMo",
    dir_out_mainmet = "/gws/nopw/j04/ukem/public/UK-AMo",
    validate_mainmet = TRUE
  )
)
render(
  "/gws/ssde/j25a/eddystore/cehmet/scripts/plot_UK-AMo_met_JC.Rmd",
  #  output_file = "/gws/nopw/j04/dare_uk/public/plevy/UK-AMo/plot_UK-AMo_month.html",
  output_file = "/gws/nopw/j04/ukem/public/UK-AMo/plot_UK-AMo_month.html",
  params = list(
    n_days = 31,
    #dir_out_mainmet = "/gws/nopw/j04/dare_uk/public/plevy/UK-AMo",
    dir_out_mainmet = "/gws/nopw/j04/ukem/public/UK-AMo",
    validate_mainmet = FALSE
  )
)
render(
  "/gws/ssde/j25a/eddystore/cehmet/scripts/plot_UK-AMo_met_JC.Rmd",
  #  output_file = "/gws/nopw/j04/dare_uk/public/plevy/UK-AMo/plot_UK-AMo_week.html",
  output_file = "/gws/nopw/j04/ukem/public/UK-AMo/plot_UK-AMo_week.html",
  params = list(
    n_days = 7,
    #dir_out_mainmet = "/gws/nopw/j04/dare_uk/public/plevy/UK-AMo",
    dir_out_mainmet = "/gws/nopw/j04/ukem/public/UK-AMo",
    validate_mainmet = TRUE
  )
)
