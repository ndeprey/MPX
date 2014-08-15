# default.platforms <- c('IPHONE')
default.platforms <- c('IPHONE', 'ANDROID')
library(lubridate)
library(RMySQL)
library(plyr)
library(reshape2)
library(ggplot2)

m <- dbDriver("MySQL")

## IF YOU GET AN ERROR for too many connections open, use the following code
## cons <- dbListConnections(MySQL())
## for (con in cons){
## dbDisconnect(con) }

Get_origin_content <- function(start.date, end.date, origin, platforms=default.platforms, driver=m, group="stage4") {
  # start.date <- '2014-07-12'
  # end.date <- '2014-07-14'
  
  #Fetch the ratings data from input parameters and sort by user id and timestamp  
  con <- dbConnect(m, group = group)
  SQLstatement <- paste("SELECT ratings_user_id, ratings_media_id, ratings_rating, ratings_timestamp, ratings_origin, ratings_elapsed FROM infinite.user_ratings ", 
                        "WHERE ratings_platform IN ('", paste(default.platforms, collapse="', '"), "') ",
                        "AND ratings_rating IN ('SKIP', 'COMPLETED', 'START','THUMBUP','SHARE','SRCHSTART','SRCHCOMPL')",
                        "AND ratings_origin = '", origin, "' ",
                        "AND DATE(ratings_timestamp) >= '", start.date, "' ",
                        "AND DATE(ratings_timestamp) <= '", end.date, "' ",
                        #"ORDER BY ratings_user_id ASC, TIMESTAMP(ratings_timestamp) ASC", 
                        sep='')
  rs <- dbSendQuery(con, SQLstatement)
  df <- fetch(rs, n=-1)
  print(paste("the query has", nrow(df), "ratings for the origin",origin))
  
  content <- dcast(df,ratings_media_id ~ ratings_rating, fun.aggregate=length)
  
  for(i in 1:nrow(content)){
    content$instant_skips[i] <- length(df$ratings_rating[
      df$ratings_rating == "SKIP" &
        df$ratings_elapsed <= 1 &
        df$ratings_origin == content$ratings_origin[i]]) 
    
    content$TOTAL[i] <- sum(content$SKIP[i], content$COMPLETED[i], content$START[i], 
                            content$THUMBUP[i], content$SHARE[i],content$SRCHCOMPL[i],content$SRCHSTART[i])
    
    content$skip_rate[i] <- content$SKIP[i] / content$TOTAL[i]
    content$instant_skip_rate[i] <- content$instant_skips[i] / content$TOTAL[i]
    content$completion_rate[i] <- content$COMPLETED[i] / content$TOTAL[i]
    content$share_rate[i] <- content$SHARE[i] / content$TOTAL[i]
    content$thumbup_rate[i] <- content$THUMBUP[i] / content$TOTAL[i]
  }
  
  return(content)
  
}

