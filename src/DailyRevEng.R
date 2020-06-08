# mike nugent, jr
# data analyst, indygo
# February 11, 2019

# this will match gps records with TD records from Oct 27 to Jan 18
library(tidyverse)
library(hablar)
library(data.table)
library(stringr)
library(lubridate)
library(timeDate)


ms_filepaths <- dir('data//raw//', full.names = T, pattern = "MONTHLY SUMMARY")

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

rules_list <- lapply(ms_filepaths, MS_RulesImport)
# turns out they're all identical, but whatever we'll keep them around for now




##### STEP 2 -> read in the ridership from routesum reports
rs_filepaths <- dir('data//raw//', full.names = T, pattern = "EVENT SUMMARY")
rs_names <- str_sub(str_match(rs_filepaths, pattern = "\\w{1,}\\s\\d{1,2}\\,"),start = 1,end = 3)


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
ridership_list <- lapply(rs_filepaths, RS_RidershipImport)
names(ridership_list) <- rs_names


##### STEP 3 _> read in the transaction details
td_filepaths <- dir('data//raw//', full.names = T, pattern = "TRANSACTION DETAIL")
td_names <- str_match(td_filepaths, pattern = "\\w{7,}\\s\\d+,") %>% str_sub(start = 1, end = -2)

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
td_list <- lapply(td_filepaths, TD_TransactionImport)
names(td_list) <- td_names
td_list_concat <- list("December" = rbindlist(list(td_list$`DECEMBER 1`,td_list$`DECEMBER 16`)),
                       "February" = td_list$`FEBRUARY 1`,
                       "January" = td_list$`JANUARY 1`,
                       "November" = rbindlist(list(td_list$`NOVEMBER 1`, td_list$`NOVEMBER 16`)),
                       "October" = td_list$`OCTOBER 27`)
#clean up some memory
rm(td_list)
##### STEP 4 Generate the Fare Diff Table and see wazzup
GenerateFareDiff <- function(i){
  # i = 1
  x <- td_list_concat[[i]]
  y <- rules_list[[i]]
  z <- ridership_list[[i]]
  
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
# generate and rename
summarylist <- lapply(list(1,2,3,4,5), GenerateFareDiff)
names(summarylist) <- names(td_list_concat)

###### ---- READ IN VMH DATA ---- #####
# 
# VMH_Folder_Path <- dir(path = "data//raw//VMH_Data")
# Oct_VMH <- fread("data//raw//VMH_Data//201910_VMH_90.csv")
# Nov_VMH <- fread("data//raw//VMH_Data//201911_VMH_90.csv")
# Dec_VMH <- fread("data//raw//VMH_Data//201912_VMH_90.csv")
# Jan_VMH <- fread("data//raw//VMH_Data//201201_VMH_90.csv")
# 
# all_VMH <- rbind(Oct_VMH,Nov_VMH,Dec_VMH,Jan_VMH)

con <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "REPSQLP01VW", 
                      Database = "TransitAuthority_IndyGo_Reporting", 
                      Port = 1433)

VMH_Raw <- tbl(con, dbplyr::in_schema("avl", "Vehicle_Message_History")) %>%
  filter(Time > "2019-10-27 03:00:00", Time < "2020-03-01 03:00:00", 
         Route_Name %like% "90%", Route %like% "90%") %>%
  collect()


Vehicle_Avl_History_raw<- tbl(con, dbplyr::in_schema("avl", "Vehicle_Avl_History")) %>%
  filter(Time > "2019-10-27 03:00:00", Time < "2020-03-01 03:00:00") %>%
  collect() 


#join lat/lon
VMH_Raw <- VMH_Raw %>% left_join(Vehicle_Avl_History_raw, by = "Avl_History_Id")

all_VMH <- data.table(VMH_Raw)

all_VMH[, c("ClockTime","Date") := list(str_sub(Time.x, 12, 19),str_sub(Time.x, 1, 10))][, DateTest := ifelse(ClockTime<"03:00:00",1,0)]
all_VMH[, Transit_Day := ifelse(DateTest ==1,
                                as_date(Date)-1,
                                as_date(Date))][,Transit_Day := as_date("1970-01-01")+days(Transit_Day)]

#add join time
all_VMH[, jointime := as.POSIXct(Time.x, tz="UTC")]
all_VMH[,c("Message_Type_Id",
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

setkey(all_VMH, Vehicle_ID, Transit_Day, jointime)



##### We'll fold this away for later just in case
# do our transit day cleanup
# Oct_VMH[, ClockTime := str_sub(Time.x, 12, 19)]
# Nov_VMH[, ClockTime := str_sub(Time.x, 12, 19)]
# Dec_VMH[, ClockTime := str_sub(Time.x, 12, 19)]
# Jan_VMH[, ClockTime := str_sub(Time.x, 12, 19)]
# 
# Oct_VMH[, Date := str_sub(Time.x, 1, 10)]
# Nov_VMH[, Date := str_sub(Time.x, 1, 10)]
# Dec_VMH[, Date := str_sub(Time.x, 1, 10)]
# Jan_VMH[, Date := str_sub(Time.x, 1, 10)]
# 
# Oct_VMH[, DateTest := ifelse(ClockTime < "03:00:00",1,0)]
# Nov_VMH[, DateTest := ifelse(ClockTime < "03:00:00",1,0)]
# Dec_VMH[, DateTest := ifelse(ClockTime < "03:00:00",1,0)]
# Jan_VMH[, DateTest := ifelse(ClockTime < "03:00:00",1,0)]
# 
# Oct_VMH[, Transit_Day := ifelse(DateTest == 1,
#                                 as_date(Date) - 1,
#                                 Date)]
# Nov_VMH[, Transit_Day := ifelse(DateTest == 1,
#                                 as_date(Date) - 1,
#                                 Date)]
# Dec_VMH[, Transit_Day := ifelse(DateTest == 1,
#                                 as_date(Date) - 1,
#                                 Date)]
# Jan_VMH[, Transit_Day := ifelse(DateTest == 1,
#                                 as_date(Date) - 1,
#                                 Date)]


# Oct_VMH <- all_VMH[Transit_Day >= "2019-10-27" & Transit_Day < "2019-11-01"]
# Nov_VMH <- all_VMH[Transit_Day >= "2019-11-01" & Transit_Day < "2019-12-01"]
# Dec_VMH <- all_VMH[Transit_Day >= "2019-12-01" & Transit_Day < "2020-01-01"]
# Jan_VMH <- all_VMH[Transit_Day >= "2020-01-01" & Transit_Day < "2020-01-19"]
# rm(all_VMH)
all_TD <- rbind(td_list_concat$October, td_list_concat$November, td_list_concat$December, td_list_concat$January, td_list_concat$February)
route_90_TD_Used <- all_TD[Route %like% "^90"][Description %in% rules_list[[1]]$Code | 
                                          Type %like% "118"]

#clean up route 90 dates and transit days
route_90_TD_Used[,Date.and.Time := mdy_hms(Date.and.Time)][,c("ClockTime","Date") := list(str_sub(Date.and.Time,12,19), str_sub(Date.and.Time,1,10))][
  , DateTest := ifelse(ClockTime<"03:00:00",1,0)]

#fix transit day then filter
route_90_TD_Used <- route_90_TD_Used[, Transit_Day := ifelse(DateTest == 1,
                                         as_date(Date)-1,
                                         as_date(Date))][,Transit_Day := as_date("1970-01-01")+days(Transit_Day)][
                                           Transit_Day >= "2019-10-27" & Transit_Day < "2020-03-01"
                                         ]





route_90_TD_Used[, jointime := as.POSIXct(Date.and.Time, tz = "UTC")]
setnames(route_90_TD_Used, "Bus", "Vehicle_ID")
setkey(route_90_TD_Used, Vehicle_ID, Transit_Day, jointime)

#clean them up before we join them
route_90_TD_Used[,c("Driver",
                    "Value",
                    "Amount",
                    "Longitude",
                    "Latitude",
                    "ClockTime",
                    "Date",
                    "DateTest") := NULL][Vehicle_ID==0,Vehicle_ID:=1970]

# 
# 
# #and now to clean up VMH
# all_VMH[,c("Message_Type_Id",
#            "OffRouteStatus",
#            "CommStatus",
#            "OperationalStatus",
#            "Server_Time",
#            "Route",
#            "Inbound_Outbound",
#            "Deviation",
#            "Vehicle_Name",
#            "Run_Id",
#            "Run_Name",
#            "Stop_Name",
#            "Operator_Record_Id",
#            "Route_Name",
#            "Stop_Report",
#            "Scheduled_Headway",
#            "Target_Headway",
#            "Alarm_State",
#            "Confidence_Level",
#            "Stop_Dwell_Time",
#            "PTV_Health_Alert",
#            "Avl_History_Id",
#            "Stop_Id",
#            "StationaryStatus",
#            "StationaryDuration",
#            "VehicleStatusID",
#            "Veh_Type_Id",
#            "Block_Farebox_Id",
#            "OdometerValue",
#            "MDTFlags",
#            "Previous_Stop_Id",
#            "Distance",
#            "Door_Cycle_Count",
#            "Departure_Time",
#            "HeadwayStatus",
#            "ActualHeadway",
#            "Block_External_Id",
#            "Time.y",
#            "Speed",
#            "Direction",
#            "ClockTime",
#            "Date",
#            "DateTest"):=NULL][,Vehicle_ID := as.character(Vehicle_ID)]

#give me that sweet rolljoin
rolljointable <- all_VMH[route_90_TD_Used, on = c(Vehicle_ID = "Vehicle_ID", Transit_Day="Transit_Day", jointime = "jointime"),roll=T,allow=F,mult="last"] %>%
  select(VMH_Time = Time.x,
         TD_Time = Date.and.Time,
         jointime,
         Vehicle_ID,
         everything())


#create our maxtimes table so we can validate the results
maxtimes <- all_VMH %>%
  group_by(Vehicle_ID,
           Transit_Day)%>%
  summarize(maxtime=as.character(max(as.POSIXct(Time.x, tz="UTC"))))
#join it to rolljoin
rolljointable <- rolljointable %>% left_join(maxtimes) %>%
  select(maxtime,everything()) %>%
  setDT()
#add our boundaries
north_boundary <- 39.877512
south_boundary <- 39.709468
#filter the rolljoin and adjust the month names
Local_Ridership_90 <- rolljointable[Latitude>north_boundary | Latitude < south_boundary][, Month := month(Transit_Day)][, Month := case_when(Month == 1 ~ "January",
                                                                                                                                            Month == 10 ~ "October",
                                                                                                                                            Month == 11 ~ "November",
                                                                                                                                            Month == 12 ~ "December",
                                                                                                                                            Month == 2 ~ "February")]



#grab local counts
Local_Ridership_90_month_count <- Local_Ridership_90[,.N, by = Month]

#get our bad gps pct
Route_90_TD_Bad_GPS_Pct <- rolljointable[GPSStatus != 2 | is.na(GPSStatus), .(Bad_TD_GPS = .N/nrow(rolljointable)*100)]



#now for timediffs
rolljointable[,timediffs := as.difftime(as.ITime(TD_Time)-as.ITime(VMH_Time),units = "secs")]
#find how many TD's report after the final VMH
Route_90_TD_After_VMH <- rolljointable[as.POSIXct(TD_Time, tz= "UTC") > as.POSIXct(maxtime, tz="UTC")]
#find how many TD's are between start and end but have a large gap, here we've used 1.5 minutes, in seconds
Route_90_TD_gap_data <- rolljointable[as.POSIXct(TD_Time, tz = "UTC") < as.POSIXct(maxtime,tz="UTC") & timediffs > 60*1.5]

Route_90_TD_Questionable_pct <- rbind(Route_90_TD_After_VMH,
                                      Route_90_TD_gap_data,
                                      rolljointable[GPSStatus != 2 | is.na(GPSStatus) | is.na(VMH_Time)])[ ,.(Pct_Invalid_Data = paste(round(.N/nrow(rolljointable)*100,4),"%",sep=" "))]


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
#add labels
rolljointable[, Local_Route := case_when(Latitude > north_boundary ~ "901",
                                         Latitude < south_boundary ~ "902",
                                         TRUE ~ "90")]



#pull just feb data
summary_by_date_and_local <- rolljointable[Local_Route != 90, .(ridership = .N), by = .(Transit_Day, Service_Type, Local_Route)]

feb_splits <- summary_by_date_and_local[,.(Total_Ridership = sum(ridership),
                                           Service_Days = .N,
                                           Average_Daily_Ridership = round(mean(ridership))),
                                                                  by = .(Service_Type, month.name[month(Transit_Day)],Local_Route)][month.name == "February",
                                                                                                                        .(Service_Type,
                                                                                                                          Total_Ridership,
                                                                                                                          Service_Days,
                                                                                                                          Average_Daily_Ridership,
                                                                                                                          Local_Route)][order(-Service_Type)]



feb_splits[Local_Route==901]
feb_splits[Local_Route==902]

#do this so we can sum easily
summary_by_date <- rolljointable[Local_Route != 90, .(ridership = .N), by = .(Transit_Day, Service_Type)]
#we'll split
summary_by_date[order(Transit_Day)] %>% view()

calendar_dates <- seq.Date(as.Date("2019-10-27"),
                           as.Date("2020-02-29"),
                           "day")
calendar_dates == summary_by_date[order(Transit_Day),Transit_Day]
# hehehehehehe

#okay let's get some summaries going and stuff
#first read in the Local Route 90 things
local_90_GFI_path <-dir('data//raw//', full.names = T, pattern = "Local")
local_90_GFI <- RS_RidershipImport(local_90_GFI_path)
local_90_GFI <- local_90_GFI[1:(nrow(local_90_GFI)-2),]

as_date(local_90_GFI$Date[1])

local_90_GFI <- local_90_GFI %>%
  mutate(Transit_Day = mdy(Date),
         Service_Type = case_when(Transit_Day %in% as_date(holidays_saturday@Data) ~ "Saturday",
                                  Transit_Day %in% as_date(holidays_sunday@Data) ~ "Sunday",
                                  weekdays(Transit_Day) %in% c("Monday","Tuesday","Wednesday","Thursday","Friday")~"Weekday",
                                  TRUE ~ weekdays(Transit_Day)),
         ridership = Ridership) %>%
  select(Transit_Day,
         Service_Type,
         ridership)

total_summary_by_date <- rbind(local_90_GFI,
                      summary_by_date)
calendar_dates <- seq.Date(as.Date("2019-09-01"),
                           as.Date("2020-02-29"),
                           "day")
calendar_dates == total_summary_by_date[order(Transit_Day),Transit_Day]
# hehehehehehe

#sick lets group and summarise that ish
total_summary_by_date_dt <- data.table(total_summary_by_date)[order(Transit_Day)]
SeptoDecSummary <- total_summary_by_date[Transit_Day <= "2019-12-31",
                                         .(Total_Ridership = sum(ridership),
                                           Service_Days = .N,
                                           Average_Daily_Ridership = round(mean(ridership))),
                                         by = Service_Type][order(-Service_Type)]
SeptoDecSummary
SepSummary <- total_summary_by_date_dt[,.(Total_Ridership = sum(ridership),
                            Service_Days = .N,
                            Average_Daily_Ridership = round(mean(ridership))),
                         by = .(Service_Type, month.name[month(Transit_Day)])][month.name == "September",
                                                                               .(Service_Type,
                                                                                 Total_Ridership,
                                                                                 Service_Days,
                                                                                 Average_Daily_Ridership)][order(-Service_Type)]
OctSummary <- total_summary_by_date_dt[,.(Total_Ridership = sum(ridership),
                                          Service_Days = .N,
                                          Average_Daily_Ridership = round(mean(ridership))),
                                       by = .(Service_Type, month.name[month(Transit_Day)])][month.name == "October",
                                                                                             .(Service_Type,
                                                                                               Total_Ridership,
                                                                                               Service_Days,
                                                                                               Average_Daily_Ridership)][order(-Service_Type)]
NovSummary <- total_summary_by_date_dt[,.(Total_Ridership = sum(ridership),
                                          Service_Days = .N,
                                          Average_Daily_Ridership = round(mean(ridership))),
                                       by = .(Service_Type, month.name[month(Transit_Day)])][month.name == "November",
                                                                                             .(Service_Type,
                                                                                               Total_Ridership,
                                                                                               Service_Days,
                                                                                               Average_Daily_Ridership)][order(-Service_Type)]
DecSummary <- total_summary_by_date_dt[,.(Total_Ridership = sum(ridership),
                                          Service_Days = .N,
                                          Average_Daily_Ridership = round(mean(ridership))),
                                       by = .(Service_Type, month.name[month(Transit_Day)])][month.name == "December",
                                                                                             .(Service_Type,
                                                                                               Total_Ridership,
                                                                                               Service_Days,
                                                                                               Average_Daily_Ridership)][order(-Service_Type)]
JanSummary <- total_summary_by_date_dt[,.(Total_Ridership = sum(ridership),
                                          Service_Days = .N,
                                          Average_Daily_Ridership = round(mean(ridership))),
                                       by = .(Service_Type, month.name[month(Transit_Day)])][month.name == "January",
                                                                                             .(Service_Type,
                                                                                               Total_Ridership,
                                                                                               Service_Days,
                                                                                               Average_Daily_Ridership)][order(-Service_Type)]
FebSummary <- total_summary_by_date_dt[,.(Total_Ridership = sum(ridership),
                                          Service_Days = .N,
                                          Average_Daily_Ridership = round(mean(ridership))),
                                       by = .(Service_Type, month.name[month(Transit_Day)])][month.name == "February",
                                                                                             .(Service_Type,
                                                                                               Total_Ridership,
                                                                                               Service_Days,
                                                                                               Average_Daily_Ridership)][order(-Service_Type)]
summary_list_to_write <- list(SeptoDecSummary
,SepSummary
,OctSummary
,NovSummary
,DecSummary
,JanSummary
,FebSummary)
summary_list_to_write

lapply(summary_list_to_write, function(x) write.table( data.frame(x), "data//processed//Local_90_Ridership_Tables.csv", append = T, sep = ","))
fwrite(total_summary_by_date_dt, "data//processed//Local_90_by_date.csv")
