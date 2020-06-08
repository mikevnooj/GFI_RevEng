# This script collects initial data for GFI validation

library(tidyverse)

con <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "REPSQLP01VW", 
                      Database = "TransitAuthority_IndyGo_Reporting", 
                      Port = 1433)

Vehicle_Message_History_raw_sample <- tbl(con, dbplyr::in_schema("avl", "Vehicle_Message_History")) %>%
  filter(Time > "2019-11-30 03:00:00", Time < "2020-01-02 03:00:00", 
         Route_Name == "90", Route == 90,
         Vehicle_ID > 1950, Vehicle_ID < 2000 | Vehicle_ID == 1899) %>%
  collect() # consider adding criteria where either Stop_Id != 0 | Board > 0 | Alight > 0

Vehicle_Avl_History_raw_sample <- tbl(con, dbplyr::in_schema("avl", "Vehicle_Avl_History")) %>%
  filter(Time > "2019-11-30 03:00:00", Time < "2020-01-02 03:00:00") %>%
  collect() 

### --- Data Cleaning --- ###

# Join lat/lon 

Vehicle_Message_History_raw_sample <- Vehicle_Message_History_raw_sample  %>%
  left_join(Vehicle_Avl_History_raw_sample, by = "Avl_History_Id")

# format dates in VMH

Vehicle_Message_History_raw_sample$Date <- as.Date(str_sub(Vehicle_Message_History_raw_sample$Time.x, 1, 10))

Vehicle_Message_History_raw_sample$Clock_Time <- str_sub(Vehicle_Message_History_raw_sample$Time.x, 12, 19)

Vehicle_Message_History_raw_sample$DateTest <- ifelse(Vehicle_Message_History_raw_sample$Clock_Time < 
                                                        "03:00:00", 1, 0)

# now change Transit Day based on DateTest

Vehicle_Message_History_raw_sample$Transit_Day_Unix <- ifelse(Vehicle_Message_History_raw_sample$DateTest == 1,
                                                              lubridate::as_date(Vehicle_Message_History_raw_sample$Date - 1),
                                                              Vehicle_Message_History_raw_sample$Date)

# add two dates together

Vehicle_Message_History_raw_sample$Epoch_Date <- as.Date("1970-01-01")

Vehicle_Message_History_raw_sample$Transit_Day <- Vehicle_Message_History_raw_sample$Epoch_Date + lubridate::days(Vehicle_Message_History_raw_sample$Transit_Day_Unix)

# filter for December

Vehicle_Message_History_raw_sample <- Vehicle_Message_History_raw_sample %>%
  filter(Transit_Day >= "2019-12-01", Transit_Day < "2020-01-01")

# now get clock hour

Vehicle_Message_History_raw_sample$Clock_Hour <- str_sub(Vehicle_Message_History_raw_sample$Clock_Time, 1, 2)

# get seconds since midnight

Vehicle_Message_History_raw_sample$seconds_between_dates <- difftime(Vehicle_Message_History_raw_sample$Date,
                                                                     Vehicle_Message_History_raw_sample$Transit_Day,
                                                                     units = "secs")

# maybe just need to use GPS_Time format...

Vehicle_Message_History_raw_sample$seconds_since_midnight_GPS_Time <- difftime(Vehicle_Message_History_raw_sample$Time.x,
                                                                               Vehicle_Message_History_raw_sample$Date, 
                                                                               units = "secs")

# add those two seconds to get seconds since (true) midnight

Vehicle_Message_History_raw_sample$seconds_since_midnight <- Vehicle_Message_History_raw_sample$seconds_since_midnight_GPS_Time +
  Vehicle_Message_History_raw_sample$seconds_between_dates

# clean the format up (dplyr no like POSIXlt)

Vehicle_Message_History_raw_sample$seconds_since_midnight_in_seconds <- as.numeric(as.character(Vehicle_Message_History_raw_sample$seconds_since_midnight))

# export

# write.csv(Vehicle_Message_History_raw_sample, 
#           file = "VMH_GPS_Sample.csv",
#           row.names = FALSE)

# # Filter by latitude to create Red Line set
# 
# Vehicle_Message_History_raw_sample <- Vehicle_Message_History_raw_sample %>%
#   filter(Latitude > 39.709468, Latitude < 39.877512)