# library(lubridate)
# library(fuzzyjoin)
# 
# 
# testbusVMH <- filter(VMH_MB, Vehicle_ID == "1999", Transit_Day == "2019-12-02") %>%
#   select(Vehicle_ID, Time.x, Trip, Stop_Name, Inbound_Outbound, Latitude, Longitude, everything()) %>%
#   arrange(Time.x) %>%
#   filter(Latitude < 39.709465)
# 
# ##### see if we can filter one bus
# testbusTD <- as_tibble(filter(Dec_TD_Used, Bus == "1999", Route == "90", Transit_Day == "2019-12-02")) %>%
#   mutate(Transit_Day = as.character(Transit_Day))
# 
# # create filter by vehicle_id, trip, transit_day
# # time_int_filter <- testbusVMH %>%
# #   mutate(Vehicle_ID = as.character(Vehicle_ID),
# #          Trip = as.character(Trip)) %>%
# #   group_by(Vehicle_ID, Trip, Transit_Day) %>%
# #   summarize(start = min(Time.x), end = max(Time.x))
# #   
# # 
# # time_int_filter$interval <- interval(time_int_filter$start, time_int_filter$end)
# # time_int_filter$interval_length <- int_length(time_int_filter$interval  )
# # 
# # time_int_filter$interval[1:length(time_int_filter),]
# # 
# # 
# # jointest <- left_join(testbusTD, time_int_filter, by = c("Transit_Day"="Transit_Day",
# #                                                           "Bus"="Vehicle_ID",
# #                                                           "Run"="Trip"))
# # 
# # keeprows <- jointest$Date.and.Time %within% jointest$interval 
# # 
# # jointest[keeprows,]
# # 
# # x<- jointest %>%
# #   select(Date.and.Time,
# #          Bus,
# #          Route,
# #          Run,
# #          Transit_Day,
# #          start,
# #          end,
# #          interval)
# # view(x)
# # 
# # testbusTD$NumericDateTime <- as.numeric(as.POSIXct(testbusTD$Date.and.Time,tz = "UTC"))
# # testbusVMH$NumericDateTime <- as.numeric(as.POSIXct(testbusVMH$Time.x, tz = "UTC"))
# # test_fuzzy <- difference_join(testbusTD, testbusVMH, by = c("NumericDateTime"="NumericDateTime"),mode = "inner",max_dist = 2*60) %>%
# #   select(Type,
# #          Date.and.Time,
# #          Time.x,
# #          NumericDateTime.x,
# #          NumericDateTime.y,
# #          Bus,
# #          Route.x,
# #          Route.y,
# #          Run,
# #          Transit_Day.x
# #          )
# 
# 
# # let's try a rolling join, join on bus, then within each bus we want transit day, then within each transit day we want times
# rolltestbusTD <- data.table(testbusTD)
# rolltestbusVMH <- data.table(testbusVMH)
# rolltestbusTD$join_time <- as.POSIXct(rolltestbusTD$Date.and.Time, tz = "UTC")
# rolltestbusVMH$join_time <- as.POSIXct(rolltestbusVMH$Time.x, tz = "UTC")
# rolltestbusVMH$Vehicle_ID <- as.character(rolltestbusVMH$Vehicle_ID)
# 
# 
# setkey(rolltestbusTD, Bus, Transit_Day, join_time)
# setkey(rolltestbusVMH, Vehicle_ID, Transit_Day, join_time)
# 
# 
# # match each TD with the previous reported GPS, within 1.5 minutes of the TD Time
# test_roll <- rolltestbusVMH[rolltestbusTD, roll = 60*1.5] %>%
#   select(VMH_Time = Time.x,
#          TD_Time = Date.and.Time,
#          join_time,
#          everything())
# 
# 
# 
# 
# test_roll$Time.x[1]
# test_roll$Date.and.Time[1]
# test_roll$join_time[1]
# test_roll$Latitude[1]
# 
# rolltestbusVMH %>% filter(Time.x == "2019-12-02 04:43:16") %>% .[2]
# 
# 


# This script collects initial data for GFI validation

library(tidyverse)

con <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "REPSQLP01VW", 
                      Database = "TransitAuthority_IndyGo_Reporting", 
                      Port = 1433)

Vehicle_Message_History_raw_sample <- tbl(con, dbplyr::in_schema("avl", "Vehicle_Message_History")) %>%
  filter(Time > "2019-12-31 03:00:00", Time < "2020-02-01 3:00:00", 
         Route == 90 | Route == 901 | Route == 902,
         Vehicle_ID > 1950, Vehicle_ID < 2000 | Vehicle_ID == 1899) %>%
  collect() # consider adding criteria where either Stop_Id != 0 | Board > 0 | Alight > 0

Vehicle_Avl_History_raw_sample <- tbl(con, dbplyr::in_schema("avl", "Vehicle_Avl_History")) %>%
  filter(Time > "2019-12-31 03:00:00", Time < "2020-02-01 03:00:00") %>%
  collect() 


### --- Data Cleaning --- ###

# Join lat/lon 

Vehicle_Message_History_raw_sample <- Vehicle_Message_History_raw_sample  %>%
  left_join(Vehicle_Avl_History_raw_sample, by = "Avl_History_Id")
fwrite(Vehicle_Message_History_raw_sample, "data//raw//VMH_Data//JanAll90.csv")

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








