#### This function: 
######## starts with a SQL query, 
######## pulls down ratings ordered by user id and timestamp
######## determines the session start and end points
######## groups them into sessions assigning key metadata in a new dataframe
######## performs key analysis on skips and completes
######## and finally writes to a csv in the same directory

## Declare key variables and load libraries

library(lubridate)
library(RMySQL)
library(plyr)
library(reshape2)
m <- dbDriver("MySQL")
default.platforms <- c('IPHONE', 'ANDROID')

## IF YOU GET AN ERROR for too many connections open, use the following code
cons <- dbListConnections(MySQL())
for (con in cons){
  dbDisconnect(con) }

#For measuring speed of query
query.start.time <- Sys.time()

### this only works if you're on the dev server
try(setwd("/home/developer/MPX/cronjobs/results"))

start.date <- Sys.Date() - 4
end.date <- Sys.Date() - 1
print(paste("for start date of",start.date,"and end date of",end.date))

##
## Declare Functions ##
##

End_of_rainbow <- function(start.date, end.date, session_timeout=30, platforms=default.platforms, driver=m, group="stage4") {
  # start.date <- '2014-07-12'
  # end.date <- '2014-07-14'
  
  #Fetch the ratings data from input parameters and sort by user id and timestamp  
  con <- dbConnect(m, group = group)
  SQLstatement <- paste("SELECT ratings_user_id, 
                        ratings_rating, ratings_timestamp, 
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
  
  ## Convert timestamp strings to POSIXct
  df$ratings_timestamp <- as.POSIXct(df$ratings_timestamp)
  
  rainbow_origins <- c("STID","DONATE","SPONS","XAPPAD","XAPPPROMO")
  rainbow_users <- c()
  
  for(i in 1:nrow(df)){
    if(df$ratings_origin[i] %in% rainbow_origins &&
         df$ratings_rating[i] == "COMPLETED" &&
         df$ratings_origin[i+1] %in% rainbow_origins &&
         df$ratings_rating[i+1] == "COMPLETED" &&
         df$ratings_origin[i+2] %in% rainbow_origins &&
         df$ratings_rating[i+2] == "COMPLETED" &&
         df$ratings_origin[i+3] %in% rainbow_origins &&
         df$ratings_rating[i+3] == "COMPLETED" &&
         df$ratings_origin[i+4] %in% rainbow_origins &&
         df$ratings_rating[i+4] == "COMPLETED" &&
         df$ratings_origin[i+5] %in% rainbow_origins &&
         df$ratings_rating[i+5] == "COMPLETED"
         )
    {
      rainbow_users <- append(rainbow_users, df$ratings_user_id[i])
    }
    
  }
  
  rainbow_users <- unique(rainbow_users)
  print(paste("the number of rainbow users was",length(rainbow_users)))
  print(paste("the total number of all users for this period was ",length(unique(df$ratings_user_id))))
  return(rainbow_users)
}

rainbow_users <- End_of_rainbow(start.date,end.date)