### Winback Performance ###

Get_Winback_Performance <- function(send_date){ 
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
  
  # # set send date #
  # send_date <- as.Date(strptime("2014-10-30",format="%Y-%m-%d"))
  
  winback <- read.csv("/Users/ndeprey/Downloads/Winback Audience 220141104.csv")
  all_uids <- winback$public_user_id
  quarantine <- read.csv("/Users/ndeprey/Downloads/Winback Audience Quarantine20141104.csv")
  
  control <- quarantine$public_user_id
  treatment <- setdiff(all_uids, control)
  
  Get_all_ratings <- function(start.date, platforms=default.platforms, driver=m, group="stage4") {
    # start.date <- '2014-07-12'
    # end.date <- '2014-07-14'
    
    #Fetch the ratings data from input parameters and sort by user id and timestamp  
    con <- dbConnect(m, group = group)
    SQLstatement <- paste("SELECT ratings_user_id, ratings_media_id, ratings_rating, ratings_timestamp, ratings_origin, ratings_elapsed FROM infinite.user_ratings ", 
                          "WHERE ratings_platform IN ('", paste(default.platforms, collapse="', '"), "') ",
                          "AND DATE(ratings_timestamp) >= '", start.date, "' ",
                          sep='')
    rs <- dbSendQuery(con, SQLstatement)
    df <- fetch(rs, n=-1)
    return(df)
  }
  
  r <- Get_all_ratings(send_date)
  
  r.quarantine <- r[r$ratings_user_id %in% control,]
  r.winback <- r[r$ratings_user_id %in% treatment,]
  
  print(paste("the percentage of users in the winback group returning to the app since send was",
              length(unique(r.winback$ratings_user_id)) / length(treatment)))
  
  print(paste("the percentage in the quarantine/control group returning to the app since send was",
              length(unique(r.quarantine$ratings_user_id)) / length(control)))
  
  print(paste("a difference of",
              round(length(treatment) * (length(unique(r.winback$ratings_user_id)) / length(treatment) - 
                length(unique(r.quarantine$ratings_user_id)) / length(control))),
              "users"
  ))
}
            
Get_Winback_Performance("2014-10-30")

