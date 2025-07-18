# library(sinew)
# makeOxygen(pad_data)
"%!in%" <- Negate("%in%")

#' @title pad_data
#' @description Adds in any gaps in a data frame representing a time series
#' @param df A data frame
#' @param by Time interval of series, Default: '30 min'
#' @param date_field Column name for POSIX date/time variable in df, Default: 'DATECT'
#' @param v_dates A vector of POSIX date/times, potentially from another df, to match with it. Default: 'df$DATECT'
#' @return OUTPUT_DESCRIPTION
#' @details DETAILS
#' @examples
#' \dontrun{
#' if(interactive()){
#'  #EXAMPLE1
#'  df <- pad_data(df)
#'  }
#' }
#' @rdname pad_data
#' @export
pad_data <- function(dt, by = "30 min", date_field = "DATECT", v_dates = NULL) {
  ##* WIP converting back and forth between df & dt is daft and has been removed - rerun to check effects elsewhere
  # df <- as.data.frame(df)
  setDT(dt)
  if (is.null(v_dates)) {
    v_dates <- dt[, ..date_field][[1]]
  }
  first <- min(v_dates, na.rm = TRUE)
  last <- max(v_dates, na.rm = TRUE)
  # make a dt with complete time series with interval "by"
  dt_date <- data.table(DATECT = seq.POSIXt(first, last, by = by))
  dt <- dt[dt_date, on = .(DATECT = DATECT)]
  # df <- as.data.frame(dt) ##* WIP this may have effects in the ERA5 processing - rerun that to check
  return(dt)
}

#' @title detect_gaps
#' @description Detects any gaps in a data frame representing a time series
#' @param df A data frame
#' @param by Time interval of series, Default: '30 min'
#' @param date_field Column name for POSIX date/time variable in df, Default: 'DATECT'
#' @return OUTPUT_DESCRIPTION
#' @details DETAILS
#' @examples
#' \dontrun{
#' if(interactive()){
#'  #EXAMPLE1
#'  gaps <- detect_gaps(l_logr$df)
#'  }
#' }
#' @rdname detect_gaps
#' @export
detect_gaps <- function(dt, expected_interval = 30, date_field = "DATECT") {
  setDT(dt)
  v_dates <- dt[, ..date_field][[1]]
  dt[, date_curr := v_dates]
  dt[, date_prev := shift(date_curr, 1)]
  dt[, date_int := difftime(date_curr, date_prev, units = "mins")]
  dt$date_int[1] <- expected_interval
  v_longer <- sum(dt$date_int > expected_interval)
  v_shorter <- sum(dt$date_int < expected_interval)
  v_gaps <- which(dt$date_int != 30)
  return(list(
    v_longer = v_longer,
    v_shorter = v_shorter,
    v_gaps = v_gaps
  ))
}

#' @title impute
#' @description Impute missing values using various methods
#' @param y Response variable with missing values to be replaced
#'   (variable name as a "quoted string")
#' @param x Covariate to be used (name of a variable in the same data frame as
#'   a "quoted string")
#' @param l_met List of two data tables containing data and qc codes. Default: l_met
#' @param method Method to use for imputing missing values, Default: "era5"
#' @param qc_toleave Which QC codes to leave unaltered when selecting values to impute, Default: 0 (raw data)
#' @param selection Denotes the points selected interactivelt by user in app Default: TRUE (= all)
#' @param date_field Name of the date field or variable in the data frame. Default: DATECT
#' @param k Number of knots to use when imputing using a GAM in the "time" method. Higher values give more flexibility = more wiggliness. Default: 40
#' @param fit Whether to fit a linear model or directly replace missing y with
#'   x values when using either "regn" or "era5" methods, Default: TRUE
#' @param x The covariate with which to fit a linear model in the "regn" method. Default: NULL
#' @param df_era5 The name of the data frame containing the corresponding ERA5 data. Default: df_era5
#' @param lat Latitude of the site for calculating day/night-time in "nightzero" method. Default: 55.792 (= Auchencorth)
#' @param lon Longitude of the site for calculating day/night-time in "nightzero" method. Default: -3.243 (= Auchencorth)
#' @param plot_graph Whether to produce a ggplot graphic - can be slow for large data sets. Default: TRUE
#' @return List of two data frames containing data and qc codes with imputed values.
#' @details DETAILS
#' @examples
#' \dontrun{
#' if(interactive()) {
#'  #EXAMPLE1
#' l_met <- list(dt = dt, dt_qc = dt_qc)
#' l_met <- impute(y = "SW_IN", x = "PPFD_IN",  l_met)
#'  }
#' }
#' @rdname impute
#' @export
##* WIP PL 11/07/2025 there is something going very wrong here!
impute <- function(
  y,
  l_met = l_met,
  method = "era5",
  qc_toleave = 0,
  selection = TRUE,
  date_field = "DATECT",
  k = 40,
  fit = TRUE,
  n_min = 10,
  x = NULL,
  df_era5 = NULL,
  lat = 55.792,
  lon = -3.243,
  plot_graph = TRUE
) {
  df_method <- data.frame(
    method = c(
      "missing",
      "time",
      "regn",
      "nightzero",
      "noneg",
      "zero",
      "era5"
    ),
    qc = c(1, 2, 3, 4, 5, 6, 7) # 0 = raw, 1 = missing
  )
  # saveRDS(df_method, file = here("data", "df_method.rds"))
  method <- match.arg(method, df_method$method)
  # get the qc code for the selected method
  qc <- df_method$qc[match(method, df_method$method)]

  # allow data frame or data table objects & naming conventions
  # force former to a data table
  if (exists("dt", where = l_met)) {
    dt <- l_met$dt
  }
  if (exists("df", where = l_met)) {
    dt <- setDT(l_met$df)
  }
  if (exists("dt_qc", where = l_met)) {
    dt_qc <- l_met$dt_qc
  }
  if (exists("df_qc", where = l_met)) {
    dt_qc <- setDT(l_met$df_qc)
  }

  # if there are no data or no missing data
  # or less than 2 x k data points for gam fitting, just return the input
  if (
    all(is.na(dt[, ..y])) ||
      all(!is.na(dt[, ..y])) ||
      (method == "time" && sum(!is.na(dt[, ..y])) < 2 * k)
  ) {
    print(paste(
      "No data, no missing data, or too few data for time (gam) regression",
      y
    ))
    return(l_met)
  } else {
    # how many non-missing data are there?
    n_data <- sum(!is.na(dt[, ..y][[1]]))
    # these methods don't work with very few/no data
    if (n_data <= n_min && (method == "time" || method == "regn")) {
      return(list(dt = dt, dt_qc = dt_qc))
    }

    # indices of values to change
    # qc_toleave defaults to 0, so leaves raw data but allows missing values to be filled in. Can be used so that one imputation method does not over-ride another.
    # selection optionally adds those selected in the metdb app ggiraph plots
    # selected = TRUE  = missing, imputed (AND selected)
    # selected = FALSE: don't think this would be used

    dt$selected <- dt_qc[, get(y) %!in% qc_toleave & selection]

    if (method == "noneg") {
      dt[selected == TRUE, selected := get(y) < 0]
    }
    if (method == "nightzero") {
      dt$date <- dt[, ..date_field] # needs to be called "date" for openair functions
      dt <- cutData(dt, type = "daylight", latitude = lat, longitude = lon)
      dt[selected == TRUE, selected := daylight == "nighttime"]
      dt$daylight <- NULL
      # if date is not the original variable name, delete it - we don't want an extra column
      if (date_field != "date") dt$date <- NULL
    }

    # calculate replacement values depending on the method
    # if a constant zero
    if (method == "nightzero" | method == "noneg" | method == "zero") {
      dt[selected == TRUE, eval(y) := 0]
    } else if (method == "time") {
      if (k > n_data / 4) {
        k <- as.integer(n_data / 4)
      }
      dt[, hour := hour(get(date_field)) + minute(get(date_field)) / 60]
      dt[, yday := as.POSIXlt(get(date_field))$yday + hour / 24]

      m <- gam(
        get(y) ~
          s(yday, k = k, bs = "cr") +
            s(hour, k = -1, bs = "cc"),
        na.action = na.exclude,
        data = dt
      )
      dt[, pred := predict(m, newdata = dt)]
      dt[selected == TRUE, eval(y) := pred]
      dt[, pred := NULL]
      dt[, yday := NULL]
      dt[, hour := NULL]
    } else if (method == "regn" || method == "era5") {
      if (method == "era5") {
        v_x <- dt_era5[, ..y] # use ERA5 data
      } else {
        v_x <- dt[, ..x] # use x variable in the CEDA data
      }
      if (fit) {
        dtt <- data.frame(y = dt[, ..y], x = v_x)
        # exclude indices selected i.e. do not fit to those we are replacing
        dtt$y[selected] <- NA
        m <- lm(y ~ x, data = dtt, na.action = na.exclude)
        v_pred <- predict(m, newdata = dtt)
      } else {
        # or just replace y with x
        v_pred <- v_x
      }
      dt[selected == TRUE, eval(y) := v_pred[selected]]
    }

    # add code for each replaced value in the qc dt
    dt_qc[which(dt$selected), eval(y) := qc]

    if (plot_graph) {
      dtt <- data.table(
        date = dt[, ..date_field][[1]],
        y = dt[, ..y][[1]],
        qc = dt_qc[, ..y][[1]]
      )
      p <- ggplot(dtt, aes(date, y))
      p <- p + geom_point(aes(y = y, colour = factor(qc)), size = 1) + ylab(y)
      fname <- paste0("plot_", y, "_", method, ".png")
      ggsave(p, filename = here("output", fname))
    }
    dt[, selected := NULL]
    return(list(dt = dt, dt_qc = dt_qc))
  }
}

plot_with_qc <- function(y, l_met = l_met, date_field = "DATECT") {
  dt <- l_met$dt
  dt_qc <- l_met$dt_qc
  dtt <- data.frame(date = dt[, date_field], y = dt[, y], qc = dt_qc[, y])
  p <- ggplot(dtt, aes(date, y))
  p <- p + geom_point(aes(y = y, colour = factor(qc)), size = 1) + ylab(y)
  return(p)
}
