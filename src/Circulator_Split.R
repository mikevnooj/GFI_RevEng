# mike nugent, jr
# data analyst, indygo
# April 14, 2020

#grab 901/902 for the circulators

library(tidyverse)
library(hablar)
library(data.table)
library(stringr)
library(lubridate)

ms_filepaths <- ms_filepaths <- dir('data//raw//Multi_Month_Data', full.names = T, pattern = "MONTHLY SUMMARY")


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

ms_rules <- MS_RulesImport(ms_filepaths[3])

#get ridership
rs_filepaths <- dir('data//raw//Circulator//', full.names = T, pattern = "EVENT SUMMARY")

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

ridership_routesum <- RS_RidershipImport(rs_filepaths)

td_filepaths <- dir('data//raw//Circulator//', full.names = T, pattern = "TRANSACTION DETAIL")

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

circulator_td <- TD_TransactionImport(td_filepaths) %>% setDT()

circulator_td_used <- circulator_td[Route %like% "^90"][Description %in% ms_rules$Code | 
                                                          Type %like% "118"]

circulator_td_used[,Date.and.Time := mdy_hms(Date.and.Time)][,c("ClockTime","Date") := list(str_sub(Date.and.Time,12,19), str_sub(Date.and.Time,1,10))][
  , DateTest := ifelse(ClockTime<"03:00:00",1,0)]

circulator_td_used <- circulator_td_used[, Transit_Day := ifelse(DateTest == 1,
                                                                 as_date(Date)-1,
                                                                 as_date(Date))][,Transit_Day := as_date("1970-01-01")+days(Transit_Day)][
                                                                   Transit_Day >= "2019-09-01" & Transit_Day < "2019-10-27"
                                                                   ]
circulator_td_used[, jointime := as.POSIXct(Date.and.Time, tz = "UTC")]

circulator_td_used[,c("Driver",
                      "Value",
                      "Amount",
                      "Longitude",
                      "Latitude",
                      "ClockTime",
                      "Date",
                      "DateTest") := NULL]
#fix the names for later
setnames(circulator_td_used, "Bus", "Vehicle_ID")

#set the key for later
setkey(circulator_td_used, Vehicle_ID, Transit_Day, jointime)

#look and then compare
circulator_td_used_by_day <- circulator_td_used[,.N,by=Transit_Day][order(Transit_Day)]

#read local 90 stuff
local_rs_filepaths <- dir('data//raw//', full.names = T, pattern = "Local Route")
local_rs <- RS_RidershipImport(local_rs_filepaths)
local_rs <- local_rs[1:(nrow(local_rs)-1),]
local_rs <- local_rs %>% select(Date,
                                Ridership)


local_rs$Transit_Day <- ymd(strptime(local_rs$Date, "%m/%d/%Y"))

bound <- cbind(local_rs,circulator_td_used_by_day)
bound$diff <- bound$N-bound$Ridership
bound$pct_diff <- round((bound$diff/bound$Ridership)*100,digits = 3)


sum(bound$diff)/sum(bound$Ridership)

#okay close enough let's go ahead
con <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "REPSQLP01VW", 
                      Database = "TransitAuthority_IndyGo_Reporting", 
                      Port = 1433)

VMH_Raw <- tbl(con, dbplyr::in_schema("avl", "Vehicle_Message_History")) %>%
  filter(Time > "2019-09-01 03:00:00", Time < "2019-10-27 03:00:00",
         Route_Name %like% "90%") %>%
  collect()


Vehicle_Avl_History_raw<- tbl(con, dbplyr::in_schema("avl", "Vehicle_Avl_History")) %>%
  filter(Time > "2019-09-01 03:00:00", Time < "2019-10-27 03:00:00") %>%
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


setkey(VMH_Rawdt, Vehicle_ID, Transit_Day, jointime)

rolljointable <- VMH_Rawdt[circulator_td_used, on = c(Vehicle_ID = "Vehicle_ID", Transit_Day="Transit_Day", jointime = "jointime"),roll=T,allow=F,mult="last"] %>%
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


#lets look at the quality of our data now
#first find GPS bad or na
Bad_GPS_Pct <- rolljointable[GPSStatus != 2 | is.na(GPSStatus), .(Bad_TD_GPS = .N/nrow(rolljointable)*100)]

Bad_GPS_Pct %>% view()
#not bad


#lets get bigdiff now
#first we'll create a timediff dealio, basically the difference in time between the match and the transaction
rolljointable[,timediffs := as.difftime(as.ITime(TD_Time)-as.ITime(VMH_Time),units = "secs")]
#find how many TD's report after the final VMH
TD_After_VMH <- rolljointable[as.POSIXct(TD_Time, tz= "UTC") > as.POSIXct(maxtime, tz="UTC")]
#find how many TD's are between start and end but have a large gap, here we've used 1.5 minutes, in seconds
TD_gap_data <- rolljointable[as.POSIXct(TD_Time, tz = "UTC") < as.POSIXct(maxtime,tz="UTC") & timediffs > 60*1.5]
#let's throw them all together
TD_Questionable_pct <- rbind(TD_After_VMH,
                              TD_gap_data,
                                      rolljointable[GPSStatus != 2 | is.na(GPSStatus)])[ ,.(Pct_Invalid_Data = paste(round(.N/nrow(rolljointable)*100,4),"%",sep=" "))]



rolljointable[,.N,by=month.name[month(Transit_Day)]]
sum(local_rs$Ridership)
comparo_circulator <- cbind(rolljointable[Latitude > north_boundary | Latitude < south_boundary,.N, Transit_Day][order(Transit_Day)],local_rs[1:56,])
comparo_circulator$diff <- comparo_circulator$Ridership - comparo_circulator$N

sum(comparo_circulator$diff)
sum(comparo_circulator$N)
rolljointable[Latitude > north_boundary | Latitude < south_boundary,.N, .(Vehicle_ID,Transit_Day)] %>% view()

rolljointable[Transit_Day == "2019-09-01",.N,Local_Route]
comparo_circulator[Transit_Day == "2019-09-01"]

#est 901
# ((32/(32+91))*804)+32
# (901rj/(901rj+902rj))*diff)+901rj)
# diff = trueridership - (901rj+902rj)
# except where diff neg

rolljointable[Latitude > north_boundary | Latitude < south_boundary,.N, by = Transit_Day][order(Transit_Day)]
comparo <- cbind(rolljointable[Latitude > north_boundary | Latitude < south_boundary,.N, by = Transit_Day][order(Transit_Day)],
                   local_rs[1:56,],
                   rolljointable[Local_Route == 901, .(rj901 = .N), Transit_Day][order(Transit_Day)][,2],
                   rolljointable[Local_Route == 902, .(rj902 = .N),Transit_Day][order(Transit_Day)][,2])
comparodt <- data.table(comparo)
setnames(comparodt,"N","ridershiprj")
comparodt[,.diff := case_when(Ridership > ridershiprj ~0,
                             TRUE ~1)]

comparodt[,factored901 := case_when((ridershiprj < Ridership) ~((rj901/(rj901+rj902))*(Ridership-(rj901+rj902)))+rj901,
                                    TRUE ~ridershiprj)]
comparodt[,factored901 := as.integer(((rj901/(rj901+rj902))*(Ridership-(rj901+rj902)))+rj901)]
comparodt[,factored902 := as.integer(((rj902/(rj901+rj902))*(Ridership-(rj901+rj902)))+rj902)]
comparodt[,final901 := ifelse(ridershiprj < Ridership, factored901, rj901)]
comparodt[,final902 := ifelse(ridershiprj < Ridership, factored902, rj902)]
comparodt[,Date := NULL]
comparodt[,Service_Typex := case_when(Transit_Day %in% as_date(holidays_saturday@Data) ~ "Saturday",
                                     Transit_Day %in% as_date(holidays_sunday@Data) ~ "Sunday",
                                     weekdays(Transit_Day) %in% c("Monday","Tuesday","Wednesday","Thursday","Friday")~"Weekday",
                                     TRUE ~ weekdays(Transit_Day))]
comparodt[,.("901" = sum(final901),
             "902" = sum(final902)), Service_Typex]


circulator_out <- melt(comparodt[,.("901" = sum(final901),
                  "902" = sum(final902)), Service_Typex],id.vars = c("Service_Typex"),variable.name = "Local_Routex",value.name = "Ridership")

circulator_out <- circulator_out[order(Local_Routex, -Service_Typex)]

NTD_Output[,sum(N),Local_Route]
circulator_out[,sum(Ridership),Local_Routex]
2177/12292
5738/32992


Final_Output <- cbind(NTD_Output,circulator_out)
Final_Output[, Ridership := N+Ridership][,c("Local_Routex",
                                            "N",
                                            "Service_Typex") := NULL]
Final_Output
fwrite(Final_Output,file="data//processed//NTD_Local_Counts.csv")
NTD_Output
Final_Output_Wide <- dcast(Final_Output, Local_Route ~ Service_Type)
setcolorder(Final_Output_Wide, c("Local_Route","Weekday","Saturday","Sunday"))
Final_Output_Wide
fwrite(Final_Output_Wide, file = "data//processed//NTD_Local_Counts.csv",append = TRUE)
