library(lubridate)
library(RMySQL)
library(plyr)

m <- dbDriver("MySQL")
# default.platforms <- c('IPHONE')
default.platforms <- c('IPHONE', 'ANDROID','CRMAPP')

Get_TLH_by_users <- function(start.date, end.date, file.path, platforms=default.platforms, driver=m, group="stage4"){
  testgroup <- read.csv(file.path)
  ## testusers <- testgroup$public_user_id
  
  con <- dbConnect(driver, group = group)
  SQLstatement <- paste("SELECT ratings_user_id, 
                        ratings_rating, 
                        ratings_timestamp, 
                        ratings_elapsed, 
                        ratings_platform, 
                        ratings_origin 
                        FROM infinite.user_ratings ",
                        "WHERE ratings_platform IN ('", paste(platforms, collapse="', '"), "') ",
                        "AND DATE(ratings_timestamp) >= '", start.date, "' ",
                        "AND DATE(ratings_timestamp) <= '", end.date, "' ",
                        sep="")
  
  rs <- dbSendQuery(con, SQLstatement)
  df <- fetch(rs, n=-1)
  print(paste("total users for this period was",length(unique(df$ratings_user_id))))
  
  totalTLH <- sum(df$ratings_elapsed)/3600
  print(paste("the total TLH for all users was",totalTLH))
  
  treatment_in_ratings <- intersect(testgroup$public_user_id,df$ratings_user_id)
  control_user_ids <- setdiff(df$ratings_user_id,testgroup$public_user_id)
  
  print(paste("the treatment group contains",length(testgroup$public_user_id),"users"))
  print(paste("the control group contains",length(control_user_ids),"users"))
  
  print(paste(length(treatment_in_ratings)," of the treatment users had app activity in the period"))
  
  pct_of_treatment_using <- length(treatment_in_ratings) / length(testgroup$public_user_id)
  
  print(paste("the portion of the treatment group using the app in this period was",pct_of_treatment_using*100,"%"))
  
  treatmentTLH <- sum(df$ratings_elapsed[df$ratings_user_id %in% testgroup$public_user_id])/60
  treatTLH_per_user <- treatmentTLH / length(treatment_in_ratings)
  print(paste("the total listening time per user of the treatment group was",treatTLH_per_user,"minutes"))
  
  controlTLH <- sum(df$ratings_elapsed[!(df$ratings_user_id %in% testgroup$public_user_id)])/60
  controlTLH_per_user <- controlTLH / length(control_user_ids)
  print(paste("the TLH per user of the control group was",controlTLH_per_user,"minutes"))

}

