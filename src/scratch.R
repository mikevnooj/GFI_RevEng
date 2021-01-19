#let's use nov actual generalized to analyze ridership

#get ridership records from td
rules_DT <- data.table(rules)

joined_td <- rules_DT[
  td_dt[Description %in% rules$Code | Type %like% "118"]
,on = c(Code= "Description")
][
  #change 118 to transfer
  Type %like% "118"
  ,Description := "Transfer"
][,Date.and.Time := mdy_hms(Date.and.Time) #format
][, `:=` ("ClockTime" = str_sub(Date.and.Time,12,19) #get the times
          ,"Date" = str_sub(Date.and.Time,1,10)
) #end `:=`
][, DateTest := ifelse(ClockTime<"03:00:00",1,0) #test the days
  ][, transit_day := ifelse(DateTest == 1, #set the days
                            as_date(Date)-1,
                            as_date(Date))
    ][,transit_day := as_date("1970-01-01")+days(transit_day)#convert the days
      ][transit_day >= start_date & transit_day < end_date + 1][
        #do service type
        
        
      ]



Period_Passes <- c("31 HALF"
                   ,"31 FULL"
                   ,"7 DAY FULL"
                   ,"ALL DAY FULL"
                   ,"7 DAY HALF"
                   ,"ALL DAY HALF"
                   ,"31 DAY S PASS"
                   ,"Comp Day Pass"
                   ,"Comp 31 day pass"
                   ,"LA1 Issue Day Pass"
                   ,"RA1 Issue Day Half"
)

Single_Trip_Passes <- c("Single Ride Full Fare"
                      ,"Single Ride Half Fare"
                      ,"LA2 Single Full"
                      ,"RA2 Single Half"
                      ,"RA3 UnderPayment"
                      ,"*1 COURTESY ISSUED"
)

Store_Ride_Card <- c("10 TRIP FULL","Comp 10 Trip Pass"
)

joined_td[Description %in% Store_Ride_Card,Type_Of_Pass := "Stored Ride"]
joined_td[Description %in% Period_Passes,Type_Of_Pass := "Period Pass"]
joined_td[Description %in% Single_Trip_Passes,Type_Of_Pass := "Single Ride"]
joined_td[Description == "Free",Type_Of_Pass := "Free Key"]

joined_td[!is.na(Type_Of_Pass),.(number_of_passes = .N),.(Type_Of_Pass,transit_day)] %>%
  ggplot(aes(fill = Type_Of_Pass, x = transit_day,y = number_of_passes)) +
  geom_bar(stat = "identity") +
  labs(title = "Pass Type Count By Day",caption = "unofficial data, subject to change; internal use only; \n this chart does not include transfers")

joined_td[!is.na(Type_Of_Pass),.(proportion_of_passes = .N),.(Type_Of_Pass,transit_day)] %>%
  ggplot(aes(fill = Type_Of_Pass, x = transit_day,y = proportion_of_passes)) +
  geom_bar(stat = "identity",position = "fill") +
  labs(title = "Pass Type Proportion By Day",caption = "unofficial data, subject to change; internal use only; \n this chart does not include transfers")

#okay we need pct of total by day
joined_td[!is.na(Type_Of_Pass),Daily_Sum := .N,transit_day
][
  
]

td_summary <- 1

joined_td[!is.na(Type_Of_Pass),.(number_of_passes = .N),.(Type_Of_Pass,transit_day)
][
  ,dcast(.SD,transit_day ~ Type_Of_Pass)
][
  ,pct
]

joined_td[,tabulate(Type_Of_Pass)/.N,.(transit_day,Type_Of_Pass)]




daily_sums <- joined_td[,.N,transit_day]
