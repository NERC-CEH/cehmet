# --------------------------------------------------------------------------------------------------------  
# Logger_Data_Update.r

# AUTHORS:  
# Dr James Cash, UK Centre for Ecology & Hydrology

# DESCRIPTION
# Code to update the logger data stored in JASMIN

# TO DO LIST:

# failed to make yearly files for "/gws/nopw/j04/ceh_generic/amo_met/server_mirror/current/UK-AMo_BM_L05_F01.dat"  

#### Clear workspace ####
# rm(list=ls())

# --------------------------------------------------------------------------------------------------------               
# FUNCTION: LoggerDataUpdate 

#LoggerDataUpdate <- function(RawFileDir = "/gws/nopw/j04/ceh_generic/amo_met/server_mirror/current", CompFileDir <- paste0("/gws/nopw/j04/ceh_generic/amo_met/server_mirror/complete") {
  
library(dplyr)

# Use UTC time throughout
Sys.setenv(TZ = "UTC")

# what year is it?
current_year <- format(Sys.Date(), "%Y")

# Grab directories holding data
RawFileDir <- paste0("/gws/nopw/j04/ceh_generic/amo_met/server_mirror/current")
CompFileDir <- paste0("/gws/nopw/j04/ceh_generic/amo_met/server_mirror/complete")

# List all files in the RawFileDir
raw_files <- list.files(RawFileDir, full.names = TRUE)

# --------------------------------------------------------------------------------------------------------
# Loop through each file in the RawFileDir

for (raw_file in raw_files) {
    # Extract the file name
    file_name <- basename(raw_file)
    
    # Construct the corresponding file path for the current year in CompFileDir
    comp_file <- file.path(CompFileDir,current_year,file_name)
    
    # Read the raw data
    raw_data <- do.call(rbind,lapply(raw_file,read.table, sep = ",", na.strings = c("NA", "NAN", "NaN"), skip = 4, header = FALSE))
    colnames(raw_data) <- t(read.table(file = raw_file, sep = ",", skip = 1, nrows = 1))
    raw_data <- raw_data[!duplicated(raw_data$TIMESTAMP),]
    raw_data$DateTime <- as.POSIXct(strptime(as.character(paste(raw_data$TIMESTAMP)),"%Y-%m-%d %H:%M:%S"), format = "%Y-%m-%d %H:M:S", tz = "GMT")
    
    # find out the time interval
    time_interval <- raw_data$DateTime[11] - raw_data$DateTime[10]
    
    # make sure the time series is continuous
    ContTimeStamp <- data.frame("DateTime" = seq(from = min(raw_data$DateTime, na.rm = T), to = max(raw_data$DateTime, na.rm = T), by = time_interval))
    raw_data <- merge(ContTimeStamp,raw_data, by = "DateTime", all.x = TRUE, all.y=TRUE)
    raw_data <- raw_data[!duplicated(raw_data$DateTime),]

    # Grab headers for saving later - note they're all different lengths so we need to grab individually
    # Headers <- read.table(file = raw_file, sep = ",", skip = 0, nrows = 4)
    Header1 <- read.table(file = raw_file, sep = ",", skip = 0, nrows = 1)
    Header2 <- read.table(file = raw_file, sep = ",", skip = 1, nrows = 1)
    Header3 <- read.table(file = raw_file, sep = ",", skip = 2, nrows = 1)
    Header4 <- read.table(file = raw_file, sep = ",", skip = 3, nrows = 1)

    # Check if the file exists in CompFileDir
    if (file.exists(comp_file)) {
        
        # Read the complete data       
        comp_data <- do.call(rbind,lapply(comp_file,read.table, sep = ",", na.strings = c("NA", "NAN", "NaN"), skip = 4, header = FALSE))
        colnames(comp_data) <- t(read.table(file = raw_file, sep = ",", skip = 1, nrows = 1))
        comp_data <- comp_data[!duplicated(comp_data$TIMESTAMP),]
        comp_data <- comp_data[!is.na(comp_data$TIMESTAMP),]
        comp_data$DateTime <- as.POSIXct(strptime(as.character(paste(comp_data$TIMESTAMP)),"%Y-%m-%d %H:%M:%S"), format = "%Y-%m-%d %H:M:S", tz = "GMT")

        #  # Read the complete data for a tricky file      
        # comp_data <- do.call(rbind,lapply(comp_file,read.table, sep = ",", na.strings = c("NA", "NAN", "NaN"), skip = 4, header = FALSE))
        # colnames(comp_data) <- t(read.table(file = raw_file, sep = ",", skip = 1, nrows = 1))
        # comp_data <- comp_data[!duplicated(comp_data$TIMESTAMP),]
        # comp_data <- comp_data[!is.na(comp_data$TIMESTAMP),]
        # valid_rows <- grepl("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}", comp_data$TIMESTAMP)
        # comp_data <- comp_data[valid_rows, ]
        # comp_data$TIMESTAMP <- substr(comp_data$TIMESTAMP, 1, 19)
        # comp_data$DateTime <- as.POSIXct(strptime(as.character(comp_data$TIMESTAMP), "%Y-%m-%d %H:%M:%S"), format = "%Y-%m-%d %H:%M:%S", tz = "GMT")

        # # Remove all data before 2022
        # comp_data <- comp_data[comp_data$DateTime >= as.POSIXct("2022-01-01 00:00:00", tz = "GMT"), ]

        # find out the time interval
        time_interval <- comp_data$DateTime[11] - comp_data$DateTime[10]
        
        # make sure the time series is continuous
        ContTimeStamp <- data.frame("DateTime" = seq(from = min(comp_data$DateTime, na.rm = T), to = max(comp_data$DateTime, na.rm = T), by = time_interval))
        comp_data <- merge(ContTimeStamp,comp_data, by = "DateTime", all.x = TRUE, all.y=TRUE)
        comp_data <- comp_data[!duplicated(comp_data$DateTime),]
        
        # Append the raw data to the complete data
        combined_data <- plyr::rbind.fill(comp_data, raw_data)
        combined_data$DateTime <- as.POSIXct(strptime(as.character(paste(combined_data$TIMESTAMP)), "%Y-%m-%d %H:%M:%S"), format = "%Y-%m-%d %H:M:S", tz = "GMT")

        # find out the time interval
        time_interval <- combined_data$DateTime[11] - combined_data$DateTime[10]

        # make sure the time series is continuous
        ContTimeStamp <- data.frame("DateTime" = seq(from = min(combined_data$DateTime, na.rm = T), to = max(combined_data$DateTime, na.rm = T), by = time_interval))
        combined_data <- merge(ContTimeStamp,combined_data, by = "DateTime", all.x = TRUE, all.y=TRUE)
        combined_data <- combined_data[!duplicated(combined_data$DateTime),]
        #combined_data$DateTime <- NULL

    } else {
        # If the file does not exist, the combined data is just the raw data
        combined_data <- raw_data
    }
    

    # --------------------------------------------------------------------------------------------------------
    # Write the combined data back to the complete file in specific year folder

    for (year in 2022:as.numeric(current_year)){
        #year <- as.character(2022+i)

        # if its a new year then we need to create a new folder
        if (!dir.exists(paste(CompFileDir,year,sep="/"))) {
        dir.create(paste(CompFileDir,year,sep="/"))
        Sys.chmod(paste(CompFileDir,year,sep="/"), mode = "0775", use_umask = FALSE) # set permissions so that anyone in the gws can write to this folder
        }

        if (year == current_year) {

        save_data <- combined_data[format(combined_data$DateTime, "%Y") == year,]
        save_data$DateTime <- NULL

        # First we need to write the headers line by line because some files have different numbers of column headers for some reason
        #write.table(Headers, file = comp_file, sep = ",", col.names =F, row.names = F, append = F, na="")
        write.table(Header1, file = paste(CompFileDir,year,file_name,sep="/"), sep = ",", col.names =F, row.names = F, append = F, na="")
        write.table(Header2, file = paste(CompFileDir,year,file_name,sep="/"), sep = ",", col.names =F, row.names = F, append = T, na="")
        write.table(Header3, file = paste(CompFileDir,year,file_name,sep="/"), sep = ",", col.names =F, row.names = F, append = T, na="")
        write.table(Header4, file = paste(CompFileDir,year,file_name,sep="/"), sep = ",", col.names =F, row.names = F, append = T, na="")
        write.table(save_data, file = paste(CompFileDir,year,file_name,sep="/"), sep = ",", col.names =F, row.names = F, append = T, na="")

        }

    }   # end of writing loop
}       # end of for loop
#}      # end of function