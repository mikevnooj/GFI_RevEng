#mike nugent, jr
#data analyst, indygo
#May 4, 2020


#get month and year
#libraries
library(tidyverse)
library(hablar)
library(stringr)
library(lubridate)
library(data.table)
library(timeDate)

#### INPUTS ####
start <- "2020/06/01" #date in format "YYYY/mm/dd"
end <- "2020/06/30" #date in format "YYYY/mm/dd"
month <- FALSE
daily <- TRUE


start_date <- as.POSIXct(start,tz = "UTC")
end_date <- as.POSIXct(end,tz = "UTC")
which_month <- month.name[as.integer(format(start_date,"%m"))]

#### RIDERSHIP IMPORT ####
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

rules <- MS_RulesImport(ms_filepaths[which(ms_filepaths %ilike% which_month)])


#### ROUTESUM IMPORT ####
#now grab routesum for comparison
#this will have to be able to take multiple months
rs_filepaths <- dir('data//raw//', full.names = T, pattern = "EVENT SUMMARY")


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

routesum_ridership <- RS_RidershipImport(rs_filepaths[which(rs_filepaths %ilike% which_month)])

#set as DT
setDT(routesum_ridership,key = "Route")


#### TD IMPORT ####
td_filepaths <- dir('data//raw//TD', full.names = TRUE, pattern = toupper(which_month))

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
    fread(skip = td_first, header = TRUE, nrows = td_last-td_first, sep = ",")
  
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

td_data <- TD_TransactionImport(td_filepaths)
#change to dt so we can rolljoin later
td_dt <- data.table(td_data)
setnames(td_dt,"Date and Time","Date.and.Time")

#### TD CLEANING ####
#find the records that count as ridership, according to the rules
td_90_ridership_records <- td_dt[Route %in% c("90","901","902")
                         ][Description %in% rules$Code | Type %like% "118"]


#add transit day
td_90_ridership_records <-  td_90_ridership_records[,Date.and.Time := mdy_hms(Date.and.Time) #format
                                                    ][, `:=` ("ClockTime" = str_sub(Date.and.Time,12,19) #get the times
                                                             ,"Date" = str_sub(Date.and.Time,1,10)
                                                             ) #end `:=`
                                                      ][, DateTest := ifelse(ClockTime<"03:00:00",1,0) #test the days
                                                        ][, Transit_Day := ifelse(DateTest == 1, #set the days
                                                                            as_date(Date)-1,
                                                                            as_date(Date))
                                                          ][,Transit_Day := as_date("1970-01-01")+days(Transit_Day)#convert the days
                                                            ][Transit_Day >= start_date & Transit_Day < end_date + 1] #filter for the input range

#add jointime
td_90_ridership_records[,jointime := fasttime::fastPOSIXct(Date.and.Time, tz = "UTC")]
#clean
td_90_ridership_records[,c("Driver"
   ,"Value"
   ,"Amount"
   ,"Longitude"
   ,"Latitude"
   ,"ClockTime"
   ,"Date"
   ,"DateTest") := NULL]
#change bus to vehicle_id now
setnames(td_90_ridership_records, "Bus", "Vehicle_ID")
#set the key
setkey(td_90_ridership_records, Vehicle_ID, Transit_Day, jointime)

#### Ridership Difference ####
# will use this as test
# how to handle multiple months and multiple routesum?

#get the summary ridership and join it to routesum
routesum_ridership[,c("Route",
                      "Ridership") #select two useful columns
                   ][td_90_ridership_records[,.(ridership_td = .N),Route #get ridership by Route
                                             ][order(as.numeric(Route))] #this is all joined to routesum
                     ][,ridership_diff := ridership_td-Ridership #do calcs on resulting DT
                       ][,percent_disagree := round((ridership_diff/Ridership)*100,digits=3)][]



#### VMH_Import ####
con <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "REPSQLP01VW", 
                             Database = "TransitAuthority_IndyGo_Reporting", 
                             Port = 1433)

#we'll need these for our query of the database
#VMH_StartTime <- start_date + (60*60*3)
# get start time in yyyymmdd with no spaces
VMH_StartTime <- str_remove_all(start_date,"-")
VMH_EndTime <- str_remove_all(end_date,"-")

#paste0 the query
VMH_Raw <- tbl(con,sql(paste0("select a.Time
,a.Route
,Boards
,Alights
,Trip
,Vehicle_ID
,Stop_Name
,Stop_Id
,Inbound_Outbound
,Departure_Time
,Latitude
,Longitude
,GPSStatus
from avl.Vehicle_Message_History a (nolock)
left join avl.Vehicle_Avl_History b
on a.Avl_History_Id = b.Avl_History_Id
where a.Route like '90%'
and a.Time > '",VMH_StartTime,"'
and a.Time < DATEADD(day,1,'",VMH_EndTime,"')"))) %>% collect()

#set the DT's and keys
setDT(VMH_Raw)

#### VMH CLEANING ####
# do transit day
VMH_Raw[, c("ClockTime","Date") := list(str_sub(Time, 12, 19),str_sub(Time, 1, 10))
        ][, DateTest := ifelse(ClockTime<"03:00:00",1,0)
          ][, Transit_Day := ifelse(DateTest ==1
                                    ,as_date(Date)-1
                                    ,as_date(Date))
            ][,Transit_Day := as_date("1970-01-01")+days(Transit_Day)
              ][, jointime := fasttime::fastPOSIXct(Time, tz="UTC")]


#clean up the columns to save space
VMH_Raw[,c("Route",
           "Inbound_Outbound",
           "Stop_Name",
           "Stop_Id",
           "Departure_Time",
           "ClockTime",
           "Date",
           "DateTest"):=NULL][,Vehicle_ID := as.character(Vehicle_ID)]

#set the key so we can roll
setkey(VMH_Raw, Vehicle_ID, Transit_Day, jointime)

#### ROLL JOIN ####
rolljointable <- VMH_Raw[td_90_ridership_records, on = c(Vehicle_ID = "Vehicle_ID", Transit_Day="Transit_Day", jointime = "jointime"),roll=T,rollends=c(T,T)] %>%
  select(VMH_Time = Time,
         TD_Time = Date.and.Time,
         jointime,
         Vehicle_ID,
         everything())



#### VALIDATE RESULTS ####
#set max times
rolljointable[VMH_Raw[,max(fasttime::fastPOSIXct(Time, tz = "UTC")),.(Vehicle_ID,Transit_Day)]
              , on = .(Vehicle_ID,Transit_Day)
              ,`:=` (maxtime = V1)]

#add timediffs
rolljointable[,timediffs := as.difftime(as.ITime(TD_Time)-as.ITime(VMH_Time),units = "secs")]

#find how many TD's report after the final VMH, meaning a transaction after location reporting stopped
TD_After_VMH <- rolljointable[as.POSIXct(TD_Time, tz= "UTC") > as.POSIXct(maxtime, tz="UTC")]

#find how many TD's are between start and end but have a large gap, here we've used 1.5 minutes, in seconds
TD_gap_data <- rolljointable[(as.POSIXct(TD_Time, tz = "UTC") < as.POSIXct(maxtime,tz="UTC") & timediffs > 60*1.5) | is.na(timediffs)]

#find how many TD's had bad GPS data
TD_Bad_GPS <- rolljointable[GPSStatus != 2 | is.na(GPSStatus)]

TD_Questionable_PCT <- rbind(TD_After_VMH,TD_gap_data,TD_Bad_GPS)[,.N/nrow(rolljointable)]
TD_Questionable_PCT

#### DO THE SPLIT ####
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

#add local route column
rolljointable[, Local_Route := case_when(Latitude > north_boundary ~ "901",
                                         Latitude < south_boundary ~ "902",
                                         TRUE ~ "90")]

#### SUMMARIZE ####

#group by Transit_Day
Local_by_Transit_Day <- dcast(rolljointable[Local_Route != 90,.(Ridership = .N),.(Local_Route, Transit_Day)]
      ,Transit_Day ~ Local_Route)

Local_by_Service_Type_Wide <- dcast(rolljointable[Local_Route != 90,.(Ridership = .N),.(Local_Route,Service_Type)]
      ,Local_Route ~ Service_Type)

Local_by_Service_Type_Long <- rolljointable[Local_Route != 90, .(Ridership = .N),.(Service_Type,Local_Route)][order(Local_Route)]

Local_by_Transit_Day
Local_by_Service_Type_Wide
Local_by_Service_Type_Long


fwrite(Local_by_Transit_Day,file = paste0("data//processed//",which_month,"_by_Day.csv"))
fwrite(Local_by_Service_Type_Wide,file = paste0("data//processed//",which_month,"_by_Type_Wide.csv"))
fwrite(Local_by_Service_Type_Long,file = paste0("data//processed//",which_month,"_by_Type_Long.csv"))


