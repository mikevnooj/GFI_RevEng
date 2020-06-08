#michael nugent, jr 
#data analyst, indygo
#march 2 2020

#here we will generalize the GFI and VMH comparison
#this method relies on the RouteSum, Monthly Summary, and Transaction Detail reports from GFI,
#as well as VMH data
library(tidyverse)
library(hablar)
library(data.table)
library(stringr)
library(lubridate)
library(timeDate)

ms_filepaths <- dir('data//raw//Multi_Month_Data//', full.names = T, pattern = "MONTHLY SUMMARY")

MS_RulesImport <- function(ms_filepath){
  
  ms_dirty <- ms_filepath %>%
    readLines()
  
  ms_first <- ms_dirty %>% 
    grep("Ridership is defined", .)+1
  
  ms_last <- ms_dirty %>%
    grep("excluded", .)-1
  
  ms_raw_rules <- data.frame(ms_dirty[ms_first:ms_last]) #read rules into dataframe
  colnames(ms_raw_rules) <- "rules" #rename the column, just to make it easier to read
  
  ms_clean_ridership_rules <- ms_raw_rules %>%
    separate_rows(names(.), sep = ",") %>%
    filter(!rules == "\"\"") %>% # spread the columns into two columns so the code matches description
    mutate(ind = rep(c(1,2),length.out = n())) %>%
    group_by(ind) %>%
    mutate(id = row_number()) %>%
    spread(ind, rules) %>%
    select(-id) %>%
    rename(
      Code = 1,
      Description = 2
    ) %>% # next two lines remove "" characters if we need them
    mutate(Code = substr(Code, 2, nchar(Code)-1)) %>%
    mutate(Description = substr(Description, 2, nchar(Description)-1));
  ms_clean_ridership_rules
}

#grab feb rules
rules <- lapply(ms_filepaths, MS_RulesImport)
rules


# get ridership from routesum
# read filepaths
rs_filepaths <- dir('data//raw//', full.names = T, pattern = "EVENT SUMMARY")

#it is numero dos that we are looking for

RS_RidershipImport <- function(rs_filepath){
  rs_ridership_first <- rs_filepath %>%
    readLines() %>%
    grep("Ridership By Route", .) %>%
    first()
  
  rs_ridership_last <- rs_filepath %>%
    readLines() %>%
    grep("TOTAL",.)-1
  
  rs_ridership_last <- rs_ridership_last[1]
  
  rs_dirty <- rs_filepath %>%
    read.table(skip=rs_ridership_first, header = TRUE, nrows = rs_ridership_last-rs_ridership_first, sep = ",")
  
  rs_clean <- rs_dirty %>%
    select(-starts_with("X"), -Token.Count, -Ticket.Count, -Preset.Preset1)
  ;
  rs_clean
}

ridership_routesum <- lapply(rs_filepaths,RS_RidershipImport)


#now we'll grab the transaction data
td_filepaths <- dir('data//raw//', full.names = T, pattern = "TRANSACTION DETAIL")
TD_TransactionImport <- function(td_filepath){
  td_first <- td_filepath %>%
    readLines() %>%
    grep("Type",.) %>%
    first()-1
  
  td_last <- td_filepath %>%
    readLines() %>%
    grep("Number of Tran",.)-2 %>%
    last()
  
  td_dirty <- td_filepath %>%
    read.table(skip=td_first, header = TRUE, nrows = td_last-td_first, sep =",")
  
  td_clean <- td_dirty %>%
    select(-Direction, -Fareset, -Stop, -Trip) %>%
    convert(chr(Type,
                Bus,
                Route,
                Run,
                Driver,
                Description))
  ; td_clean
}


#grab them feb farebox thingies
TD_list <- lapply(td_filepaths,TD_TransactionImport)
#let's do our transit day stuff now
#add a dt with TD data

#grab only the used transactions
route_90_TD_Used <- TD_Febdt[Route %like% "^90"][Description %in% rules$Code | 
                                         Type %like% "118"]

#now we'll add transit day
route_90_TD_Used[,Date.and.Time := mdy_hms(Date.and.Time)][,c("ClockTime","Date") := list(str_sub(Date.and.Time,12,19), str_sub(Date.and.Time,1,10))][
  , DateTest := ifelse(ClockTime<"03:00:00",1,0)]

#fix the transit day and then filter for the date range we would like to use
route_90_TD_Used <-route_90_TD_Used[, Transit_Day := ifelse(DateTest == 1,
                                         as_date(Date)-1,
                                         as_date(Date))][,Transit_Day := as_date("1970-01-01")+days(Transit_Day)][
                                           Transit_Day >= "2020-02-09" & Transit_Day < "2020-03-01"
                                           ]
#add jointime
route_90_TD_Used[, jointime := as.POSIXct(Date.and.Time, tz = "UTC")]
#clean it up a bit
route_90_TD_Used[,c("Driver",
                    "Value",
                    "Amount",
                    "Longitude",
                    "Latitude",
                    "ClockTime",
                    "Date",
                    "DateTest") := NULL]

#fix the names for later
setnames(route_90_TD_Used, "Bus", "Vehicle_ID")
#set the key for later
setkey(route_90_TD_Used, Vehicle_ID, Transit_Day, jointime)
#let's see where our data is
route_90_TD_Used[,.N, by = .(Transit_Day, Route)]
route_90_TD_Used[,.N, by = Vehicle_ID]
#uh okay. so we're missing all our 901/902
route_90_TD_Used[Route==90,.N, by = Vehicle_ID]
#we'll roll with it for now and see what we can come up with
#so the RouteSum and our own actual data should match about exactly, let's see if that's true
GenerateFareDiff <- function(td_list, rules, ridership){
  # i = 1
  x <- td_list
  y <- rules
  z <- ridership
  
  ridership_test <- x %>%
    filter(Description %in% y$Code | 
             Type %like% "118") %>%
    group_by(Route) %>%
    summarize(count = n()) %>%
    arrange(as.integer(Route)) %>%
    rename(Ridership_mine = count)
  
  fare_diff <- ridership_test %>%
    inner_join(z, by = "Route") %>%
    select(Route, Ridership, Ridership_mine) %>%
    mutate(Ridership_diff = Ridership_mine-Ridership) %>%
    mutate(percent_disagree = round((Ridership_diff/Ridership)*100,digits = 3));
  fare_diff
}  
#and let's do it
farediff <- GenerateFareDiff(TD_Feb, rules, ridership_routesum)

#grab our VMH data
con <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "REPSQLP01VW", 
                      Database = "TransitAuthority_IndyGo_Reporting", 
                      Port = 1433)

VMH_Raw <- tbl(con, dbplyr::in_schema("avl", "Vehicle_Message_History")) %>%
  filter(Time > "2019-10-26 03:00:00", Time < "2020-01-01 03:00:00", 
         Route_Name %like% "90%", Route %like% "90%") %>%
  collect()


Vehicle_Avl_History_raw<- tbl(con, dbplyr::in_schema("avl", "Vehicle_Avl_History")) %>%
  filter(Time > "2020-10-26 03:00:00", Time < "2020-01-01 03:00:00") %>%
  collect() 



#join lat/lon
VMH_Raw_joined <- VMH_Raw %>% left_join(Vehicle_Avl_History_raw, by = "Avl_History_Id")

VMH_Raw_joined %>% group_by(GPSStatus) %>% summarise(n())

#and turn it into a DT so we can roll with it
VMH_Rawdt <- data.table(VMH_Raw_joined)

#add clock time and date, then test date
VMH_Rawdt[, c("ClockTime","Date") := list(str_sub(Time.x, 12, 19),str_sub(Time.x, 1, 10))][, DateTest := ifelse(ClockTime<"03:00:00",1,0)][, Transit_Day := ifelse(DateTest ==1,
                                as_date(Date)-1,
                                as_date(Date))][,Transit_Day := as_date("1970-01-01")+days(Transit_Day)]

#add jointime to VMH
VMH_Rawdt[, jointime := as.POSIXct(Time.x, tz="UTC")]
VMH_Rawdt[,.N, by = Transit_Day]
#then clean it up
VMH_Rawdt[,c("Message_Type_Id",
           "OffRouteStatus",
           "CommStatus",
           "OperationalStatus",
           "Server_Time",
           "Route",
           "Inbound_Outbound",
           "Deviation",
           "Vehicle_Name",
           "Run_Id",
           "Run_Name",
           "Stop_Name",
           "Operator_Record_Id",
           "Route_Name",
           "Stop_Report",
           "Scheduled_Headway",
           "Target_Headway",
           "Alarm_State",
           "Confidence_Level",
           "Stop_Dwell_Time",
           "PTV_Health_Alert",
           "Avl_History_Id",
           "Stop_Id",
           "StationaryStatus",
           "StationaryDuration",
           "VehicleStatusID",
           "Veh_Type_Id",
           "Block_Farebox_Id",
           "OdometerValue",
           "MDTFlags",
           "Previous_Stop_Id",
           "Distance",
           "Door_Cycle_Count",
           "Departure_Time",
           "HeadwayStatus",
           "ActualHeadway",
           "Block_External_Id",
           "Time.y",
           "Speed",
           "Direction",
           "ClockTime",
           "Date",
           "DateTest"):=NULL][,Vehicle_ID := as.character(Vehicle_ID)]

VMH_Rawdt[,.N, by = .(GPSStatus,Transit_Day)]
setkey(VMH_Rawdt, Vehicle_ID, Transit_Day, jointime)

#give me that sweet sweet rolljoin
rolljointable <- VMH_Rawdt[route_90_TD_Used, on = c(Vehicle_ID = "Vehicle_ID", Transit_Day="Transit_Day", jointime = "jointime"),roll=T,allow=F,mult="last"] %>%
  select(VMH_Time = Time.x,
         TD_Time = Date.and.Time,
         jointime,
         Vehicle_ID,
         everything())
#grab maxtimes
maxtimes <- VMH_Rawdt %>%
  group_by(Vehicle_ID,
           Transit_Day)%>%
  summarize(maxtime=as.character(max(as.POSIXct(Time.x, tz="UTC"))))
#join to rolljoin
rolljointable <- rolljointable %>% left_join(maxtimes) %>%
  select(maxtime,everything()) %>%
  setDT()
#set boundaries
north_boundary <- 39.877512
south_boundary <- 39.709468

#filter the rolljoin for the local data points
Local_Ridership_90 <- rolljointable[Latitude>north_boundary | Latitude < south_boundary][, Month := ifelse(Latitude > north_boundary,
                                                                                                           "February 901", 
                                                                                                           "February 902")]

#grab the count as well
Local_Ridership_90_month_count <- Local_Ridership_90[,.N, by = Month]

#lets look at the quality of our data now
#first find GPS bad or na
Route_90_TD_Bad_GPS_Pct <- rolljointable[GPSStatus != 2 | is.na(GPSStatus), .(Bad_TD_GPS = .N/nrow(rolljointable)*100)]

Route_90_TD_Bad_GPS_Pct %>% view()
#not bad


#lets get bigdiff now
#first we'll create a timediff dealio, basically the difference in time between the match and the transaction
rolljointable[,timediffs := as.difftime(as.ITime(TD_Time)-as.ITime(VMH_Time),units = "secs")]
#find how many TD's report after the final VMH
Route_90_TD_After_VMH <- rolljointable[as.POSIXct(TD_Time, tz= "UTC") > as.POSIXct(maxtime, tz="UTC")]
#find how many TD's are between start and end but have a large gap, here we've used 1.5 minutes, in seconds
Route_90_TD_gap_data <- rolljointable[as.POSIXct(TD_Time, tz = "UTC") < as.POSIXct(maxtime,tz="UTC") & timediffs > 60*1.5]
#let's throw them all together
Route_90_TD_Questionable_pct <- rbind(Route_90_TD_After_VMH,
                                      Route_90_TD_gap_data,
                                      rolljointable[GPSStatus != 2 | is.na(GPSStatus)])[ ,.(Pct_Invalid_Data = paste(round(.N/nrow(rolljointable)*100,4),"%",sep=" "))]



#let's add service typeeeee
#set the holy days
holidays_sunday_service <- c("USNewYearsDay", "USMemorialDay",
                             "USIndependenceDay", "USLaborDay",
                             "USThanksgivingDay", "USChristmasDay")

holidays_saturday_service <- c("USMLKingsBirthday")

#set sat sun
holidays_sunday <- holiday(2000:2020, holidays_sunday_service)
holidays_saturday <- holiday(2000:2020, holidays_saturday_service)
#set service type column
rolljointable[,Service_Type := case_when(Transit_Day %in% as_date(holidays_saturday@Data) ~ "Saturday",
                                            Transit_Day %in% as_date(holidays_sunday@Data) ~ "Sunday",
                                            weekdays(Transit_Day) %in% c("Monday","Tuesday","Wednesday","Thursday","Friday")~"Weekday",
                                            TRUE ~ weekdays(Transit_Day))]
#add month column to the whole thing, what the hell why not
rolljointable[, Local_Route := case_when(Latitude > north_boundary ~ "901",
                                   Latitude < south_boundary ~ "902",
                                   TRUE ~ "90")]


#summarise by type of service
summary_by_type <- rolljointable[Local_Route != "90",.(average_daily_ridership = round(.N/length(unique(Transit_Day)),0),
                                      number_of_days = length(unique(Transit_Day)),
                                      total_ridership = .N)
                                   ,by = .(Service_Type, Local_Route)]
#and by date
summary_by_date <- rolljointable[Local_Route != "90",.(average_daily_ridership = round(.N/length(unique(Transit_Day)),0),
                                                      number_of_days = length(unique(Transit_Day)),
                                                      total_ridership = .N)
                                ,by = .(Transit_Day, Local_Route)]


#order aaaaaand write!
fwrite(summary_by_type[order(-Service_Type,Local_Route)], file = "data//processed//February_Local_Ridership_by_type.csv")
fwrite(summary_by_date[order(Transit_Day, Local_Route)], file = "data//processed//February_Local_Ridership_by_date.csv")
