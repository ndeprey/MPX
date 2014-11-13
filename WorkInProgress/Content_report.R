### Get Content Stats ###
# default.platforms <- c('IPHONE')
default.platforms <- c('IPHONE', 'ANDROID')
library(lubridate)
library(RMySQL)
library(plyr)
library(reshape2)
library(ggplot2)

m <- dbDriver("MySQL")
## IF YOU GET AN ERROR for too many connections open, use the following code
cons <- dbListConnections(MySQL())
for (con in cons){
dbDisconnect(con) }

Get_content <- function(start.date, end.date, platforms=default.platforms, driver=m, group="stage4") {
  # start.date <- '2014-07-12'
  # end.date <- '2014-07-14'
  
  #Fetch the ratings data from input parameters and sort by user id and timestamp  
  con <- dbConnect(m, group = group)
  SQLstatement <- paste("SELECT ratings_story_id as seamus_id, ratings_rating, ratings_origin as origin, ratings_elapsed, thing_title as title, date(thing_publish_date) as pub_date
                        FROM infinite.user_ratings 
                        JOIN cms.thing on user_ratings.ratings_story_id = thing.thing_id ", 
                        "WHERE ratings_platform IN ('", paste(default.platforms, collapse="', '"), "') ",
                        "AND DATE(ratings_timestamp) >= '",start.date,"' ",
                        "AND DATE(ratings_timestamp) <= '",end.date,"' ",
                        "AND ratings_user_id < 1000000000 ",
                        "AND ratings_origin IN ('ARCHIVES','BREAK','ASSIST','CORE','LEAD','INVEST','OPENDOOR','SELECTS','MUSICINT') ",
                        "AND ratings_rating IN ('START','SKIP','THUMBUP','SHARE','COMPLETED') ",
                        "ORDER by seamus_id ASC",
                        sep='')
  rs <- dbSendQuery(con, SQLstatement)
  df <- fetch(rs, n=-1)
  print(paste("the query has", length(unique(df$seamus_id)), "unique stories and", nrow(df), "total ratings"))
  
  content <- dcast(df, seamus_id + title + pub_date ~ ratings_rating, fun.aggregate = length, value.var="title")
  print("done with dcast")
  
  pb <- txtProgressBar(max=nrow(content), style=2)
  for(i in 1:nrow(content)){
    setTxtProgressBar(pb, i)
    tdf <- df[df$seamus_id == content$seamus_id[i],]
    
    content$skips_10[i] <- length(tdf$ratings_rating[
      (tdf$ratings_rating == "SKIP") &
        (tdf$ratings_elapsed <= 10) &
        (tdf$seamus_id == content$seamus_id[i])])
    
    content$skips10_30[i] <- length(tdf$ratings_rating[
      (tdf$ratings_rating == "SKIP") &
        (tdf$ratings_elapsed > 10) &
        (tdf$ratings_elapsed < 30) &
        (tdf$seamus_id == content$seamus_id[i])])
    
    content$skips30_60[i] <- length(tdf$ratings_rating[
      (tdf$ratings_rating == "SKIP") &
        (tdf$ratings_elapsed >= 30) &
        (tdf$ratings_elapsed <= 60) &
        (tdf$seamus_id == content$seamus_id[i])]) 
  
    content$TOTAL[i] <- sum(content$SKIP[i], content$COMPLETED[i], content$START[i], 
                            content$THUMBUP[i], content$SHARE[i],content$SRCHCOMPL[i],content$SRCHSTART[i])
    
    content$skip_rate[i] <- content$SKIP[i] / content$TOTAL[i]
    content$completion_rate[i] <- content$COMPLETED[i] / content$TOTAL[i]
    content$share_rate[i] <- content$SHARE[i] / content$TOTAL[i]
    content$thumbup_rate[i] <- content$THUMBUP[i] / content$TOTAL[i]
    content$thumbup_share_rate[i] <- content$thumbup_rate[i] + content$share_rate[i]
  }
  close(pb)
  content <- content[order(content$TOTAL,decreasing=TRUE),]
  
  return(content)
}

# yesterday <- Get_content(Sys.Date()-1,Sys.Date()-1)
# write.csv(yesterday, file=paste("/home/developer/content_reports/npr_one_content_yesterday",Sys.Date()-1,".csv",sep=''))
# write.csv(yesterday, file="/home/developer/content_reports/npr_one_content_yesterday_current.csv")
# 
# last7 <- Get_content(Sys.Date()-7,Sys.Date()-1)
# write.csv(last7, file=paste("/home/developer/content_reports/npr_one_content_last_7_days_",Sys.Date()-1,".csv",sep=''))
# write.csv(last7, file="/home/developer/content_reports/npr_one_content_last_7_days_current.csv")
# 
# last_30 <- Get_content(Sys.Date()-30,Sys.Date()-1)
# write.csv(last7, file=paste("/home/developer/content_reports/npr_one_content_last_30_days_",Sys.Date()-1,".csv",sep=''))
# write.csv(last7, file="/home/developer/content_reports/npr_one_content_last_30_days_current.csv")
# 




