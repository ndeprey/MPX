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

RATED_breakdown <- function(rated_threshold, platforms=default.platforms, driver=m, group="stage4") {
  # start.date <- '2014-07-12'
  # end.date <- '2014-07-14'
  
  #Fetch the ratings data from input parameters and sort by user id and timestamp  
  con <- dbConnect(m, group = group)
  SQLstatement <- paste("SELECT uar_user_id as ratings_user_id, uar_affiliation_id as show_id, uar_rating as average, uar_ts, thing_title 
                          from infinite.user_affiliation_rating 
                          join cms.thing on uar_affiliation_id = thing.thing_id
                          WHERE uar_user_id < 1000000000
                          AND uar_affiliation_id > 10
                          AND uar_affiliation_id NOT IN (60, 22, 46, 57, 13, 230697168, 230697201)
                          AND date(uar_ts) > '2014-07-28'
                          AND uar_user_id < 1000000000
                          limit 1000000000000"
  )
  
  rs <- dbSendQuery(con, SQLstatement)
  df <- fetch(rs, n=-1)
  print(paste("the query has", nrow(df), "ratings"))
  
  df$cuts <- cut(df$average,breaks=c(0,rated_threshold,1))
  
  users <- dcast(df, ratings_user_id ~ cuts, fun.aggregate = length)
  colnames(users) <- c("user_id","below_rated","rated","NA")
  
  return(users)
}

# u <- RATED_breakdown(0.6)
# ggplot(u, aes(rated)) + geom_histogram(colour="white")
