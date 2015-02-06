

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

SkipRates_by_origin <- function(start.date, end.date, platforms=default.platforms, driver=m, group="stage4") {
  # start.date <- '2014-07-12'
  # end.date <- '2014-07-14'
  
  #Fetch the ratings data from input parameters and sort by user id and timestamp  
  con <- dbConnect(m, group = group)
  SQLstatement <- paste("SELECT ratings_user_id, ratings_media_id, ratings_rating, ratings_timestamp, ratings_origin, ratings_elapsed FROM infinite.user_ratings ", 
                        "WHERE ratings_platform IN ('", paste(default.platforms, collapse="', '"), "') ",
                        "AND ratings_rating IN ('SKIP', 'COMPLETED', 'START','THUMBUP','SHARE')",
                        "AND ratings_origin NOT IN ('INTRO','ASSIST','NONFICTION','HELLO','STID','SEARCH','XAPPAD','XAPPPROMO','SPONS','DONATE','EDTR','SHARED') ",
                        "AND DATE(ratings_timestamp) >= '", start.date, "' ",
                        "AND DATE(ratings_timestamp) <= '", end.date, "' ",
                        "AND ratings_user_id < 1000000000 ",
                        "ORDER BY ratings_user_id ASC, TIMESTAMP(ratings_timestamp) ASC", sep='')
  rs <- dbSendQuery(con, SQLstatement)
  df <- fetch(rs, n=-1)
  print(paste("the query has", nrow(df), "ratings"))
  
  pivot_agg <- dcast(df,ratings_origin ~ ratings_rating, fun.aggregate = length)
  #pivot_agg <- rbind(pivot_agg, c("Total", colSums(pivot_agg[,-1])))
  
  for( i in 1:nrow(pivot_agg)){
      pivot_agg$instant_skips[i] <- length(df$ratings_rating[
        df$ratings_rating == "SKIP" &
          df$ratings_elapsed <= 4 &
          df$ratings_origin == pivot_agg$ratings_origin[i]]) 
  }
  
  pivot_agg <- rbind(pivot_agg, c("all", colSums(pivot_agg[,-1])))
  
  #print(pivot_agg)
  pivot_agg <- transform(pivot_agg, COMPLETED = as.numeric(COMPLETED),
            SHARE = as.numeric(SHARE),
            SKIP = as.numeric(SKIP),
            START = as.numeric(START),
            THUMBUP = as.numeric(THUMBUP),
            instant_skips = as.numeric(instant_skips)
  )
                              
  #print(str(pivot_agg))
  
  for (i in 1:nrow(pivot_agg)){
      pivot_agg$TOTAL[i] <- sum(pivot_agg$SKIP[i], pivot_agg$COMPLETED[i], pivot_agg$START[i], pivot_agg$THUMBUP[i], pivot_agg$SHARE[i])
        
      pivot_agg$start_rate[i] <- pivot_agg$START[i] / pivot_agg$TOTAL[i]
      pivot_agg$skip_rate[i] <- pivot_agg$SKIP[i] / pivot_agg$TOTAL[i]
      pivot_agg$instant_skip_rate[i] <- pivot_agg$instant_skips[i] / pivot_agg$TOTAL[i]
      pivot_agg$ins_over_skips[i] <- pivot_agg$instant_skips[i] / pivot_agg$SKIP[i]
      pivot_agg$completion_rate[i] <- pivot_agg$COMPLETED[i] / pivot_agg$TOTAL[i]
      pivot_agg$share_rate[i] <- pivot_agg$SHARE[i] / pivot_agg$TOTAL[i]
      pivot_agg$thumbup_rate[i] <- pivot_agg$THUMBUP[i] / pivot_agg$TOTAL[i]
      
  }
  
  
  return(pivot_agg)
  assign("all_ratings",df, envir = .GlobalEnv)
}
