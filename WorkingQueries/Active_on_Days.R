############################################
######## ActiveOnDays ######################
############################################

## For a given period, finds the total number of unique users. Then it calculates
## the number of days within the period that each user was active and keeps a count.
## The end result is a frequency table showing the number of users by how many days they were active.
## For example you will be able to conclude that in a 14 day period, we had 100,000 active users,
## 55% of which were only active on only 1 day in the period, 45% were active at least 2 days, 
## 25% were active at least 3 days, etc.

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

ActiveOnDays <- function(start.date, end.date, days.back=7, platforms=default.platforms, driver=m, group="stage4") {
  # start.date <- '2014-07-12'
  # end.date <- '2014-07-25'
  
  ## Create a vector of days
  dates <- seq(as.Date(start.date), as.Date(end.date), by="days")
  
  ## Find the total number of uniques for the period and print
  con <- dbConnect(m, group = group)
  SQLstatement <- paste("SELECT ratings_user_id FROM infinite.user_ratings ", 
                        "WHERE ratings_platform IN ('", paste(default.platforms, collapse="', '"), "') ",
                        "AND DATE(ratings_timestamp) >= '", start.date, "' ", 
                        "AND DATE(ratings_timestamp) <= '", end.date, "' ", sep='')
  
  rs <- dbSendQuery(con, SQLstatement)
  all.users <- fetch(rs, n=-1)
  all.uniques <- length(unique(all.users$ratings_user_id))
  print(paste("total of all uniques for this period was",all.uniques))
  
  ## Fetch the user IDs by day, de-dupe and save to a list
  users <- list()
  # uniques <- data.frame(date=dates)
  for(i in 1:length(dates)){
    
    con <- dbConnect(m, group = group)
    SQLstatement <- paste("SELECT ratings_user_id FROM infinite.user_ratings ", 
                          "WHERE ratings_platform IN ('", paste(default.platforms, collapse="', '"), "') ",
                          "AND DATE(ratings_timestamp) = '", dates[i], "' ", sep='')
                          ## "ORDER BY ratings_user_id ASC, TIMESTAMP(ratings_timestamp) ASC", sep='')
    rs <- dbSendQuery(con, SQLstatement)
    df <- fetch(rs, n=-1)
    users[[i]] <- unique(df$ratings_user_id)
    print(paste(dates[i], "has",length(users[[i]]),"unique users"))
    # uniques$count[i] <- length(users[[i]])
    dbDisconnect(con)
  }
  
  ## Unlist the user IDs list and create a frequency table (in 2 steps)
  ## Then divide the frequencies by the total uniques for the period.
  freq.table <- data.frame(table(unlist(users)))
  frequency <- data.frame(table(freq.table$Freq))
  frequency$of.total <- frequency$Freq / all.uniques
  
  ## Calculate how many were active on AT LEAST that many days
  for(i in 1:nrow(frequency)){
    frequency$at.least[i] <- sum(frequency$of.total[i:nrow(frequency)])
  }
  
  ## Rename a column
  frequency <- rename(frequency, c("Var1" = "Days.Active"))
  
  ## Calculate the number active throughout and print the result
  active.throughout <- length(Reduce(intersect,users))
  print(paste("active throughtout =", active.throughout))
  
  ## Assign the frequency table to the gloval environment
  return(frequency)
}
