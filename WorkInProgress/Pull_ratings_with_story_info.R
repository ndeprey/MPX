## Get Ratings with metadata

## Declare key variables and load libraries

library(lubridate)
library(RMySQL)
library(plyr)
library(reshape2)
m <- dbDriver("MySQL")
default.platforms <- c('IPHONE', 'ANDROID')
default.origins <- c('INVEST','SELECTS','BREAK','OPENDOOR','LEAD','CORE','NONFICTION','ARCHIVES','ASSIST')

## IF YOU GET AN ERROR for too many connections open, use the following code
cons <- dbListConnections(MySQL())
for (con in cons){
  dbDisconnect(con) }

#For measuring speed of query
query.start.time <- Sys.time()

### this only works if you're on the dev server
try(setwd("/home/developer/MPX/cronjobs/results"))

##
## Declare Functions ##
##

Get_ratings_story_info <- function(start.date, end.date, origins, session_timeout=30, platforms=default.platforms, driver=m, group="stage4") {
  #start.date <- '2014-08-31'
  #end.date <- '2014-08-31'
  # group = "stage4"
  #Fetch the ratings data from input parameters and sort by user id and timestamp  
  con <- dbConnect(m, group = group)
  SQLstatement <- paste("SELECT user_ratings.ratings_user_id, 
                        user_ratings.ratings_media_id, 
                        user_ratings.ratings_story_id,
                        user_ratings.ratings_rating, ratings_timestamp, 
                        user_ratings.ratings_origin, 
                        user_ratings.ratings_elapsed, 
                        user_ratings.ratings_cohort,
                        thing.thing_title, thing.thing_teaser ",
                        "FROM infinite.user_ratings ", 
                        "INNER JOIN cms.thing on user_ratings.ratings_story_id = thing.thing_id ",
                        "WHERE user_ratings.ratings_platform IN ('", paste(default.platforms, collapse="', '"), "') ",
                        "AND user_ratings.ratings_origin IN ('", paste(default.origins, collapse="', '"), "') ",
                        "AND DATE(user_ratings.ratings_timestamp) >= '", start.date, "' ",
                        "AND DATE(user_ratings.ratings_timestamp) <= '", end.date, "' ",
                        "ORDER BY user_ratings.ratings_user_id ASC, TIMESTAMP(user_ratings.ratings_timestamp) ASC", sep='')
  rs <- dbSendQuery(con, SQLstatement)
  df <- fetch(rs, n=-1)
  print(paste("the query has", nrow(df), "ratings"))
  
  ## Convert timestamp strings to POSIXct
  df$ratings_timestamp <- as.POSIXct(df$ratings_timestamp)
  
  return(df)
}

start.date <- Sys.Date() - 21
end.date <- Sys.Date() - 1
print(paste("for start date of",start.date,"and end date of",end.date))

default.origins <- c('INVEST','SELECTS','BREAK','OPENDOOR','ARCHIVES')

df <- Get_ratings_story_info(start.date = start.date, end.date = end.date, origins = default.origins)

write.csv(df,file=paste("ratings_story",end.date,".csv",sep=''))

query.end.time <- Sys.time()
print(paste("this query started at",query.start.time,"and finished at",query.end.time))
