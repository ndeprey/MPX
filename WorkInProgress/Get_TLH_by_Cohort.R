### Get TLH by Cohort ####

library(lubridate)
library(RMySQL)
library(plyr)
library(reshape2)
m <- dbDriver("MySQL")
default.platforms <- c('IPHONE', 'ANDROID')

### this only works if you're on the dev server
try(setwd("/home/developer/MPX/cronjobs/results"))

start.date <- Sys.Date() - 7
end.date <- Sys.Date() - 1

##
## Declare Functions ##
##

GetTLHbyCohort <- function(start.date, end.date, platforms=default.platforms, driver=m, group="stage4") {
  # start.date <- '2014-07-12'
  # end.date <- '2014-07-14'
  
  #Fetch the ratings data from input parameters and sort by user id and timestamp  
  con <- dbConnect(m, group = group)
  SQLstatement <- paste("SELECT ratings_user_id, 
                        ratings_rating, 
                        ratings_timestamp, 
                        ratings_origin, 
                        ratings_elapsed, 
                        ratings_cohort 
                        FROM infinite.user_ratings ", 
                        "WHERE ratings_platform IN ('", paste(default.platforms, collapse="', '"), "') ",
                        "AND DATE(ratings_timestamp) >= '", start.date, "' ",
                        "AND DATE(ratings_timestamp) <= '", end.date, "' ",
                        "ORDER BY ratings_user_id ASC, TIMESTAMP(ratings_timestamp) ASC", sep='')
  rs <- dbSendQuery(con, SQLstatement)
  df <- fetch(rs, n=-1)
  print(paste("the query has", nrow(df), "ratings"))
  
  cohorts <- unique(df$ratings_cohort[!is.na(df$ratings_cohort)])
  TLH <- numeric()
  users <- numeric()
  TLM.per.user <- numeric()

  for(i in 1:length(cohorts)){
    TLH[i] <- sum(df$ratings_elapsed[df$ratings_cohort==cohorts[i]],na.rm=TRUE)
    users[i] <- length(unique(df$ratings_user_id[df$ratings_cohort==cohorts[i]]))
    TLM.per.user[i] <- (TLH[i] / users [i]) / 60
  }
  
  TLH.byCohort <- data.frame(cohort=cohorts,TLH=TLH,users=users,TLM.per.user=TLM.per.user)
  
  return(TLH.byCohort)
}

trial.Cohorts <- GetTLHbyCohort(start.date=start.date,end.date=end.date)
