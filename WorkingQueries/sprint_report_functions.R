library(lubridate)
library(RMySQL)
library(plyr)

##################################
###  INITIALIZE KEY VARIABLES  ###
##################################
m <- dbDriver("MySQL")
mysql.group <- "stage4-infinite"
# default.platforms <- c('IPHONE')
default.platforms <- c('IPHONE', 'ANDROID', 'WINDOWPH')
robo.ids <- c(11987982,
  13174734, 1186881532, 1087617437, 1138038188, 9906791, 7456245, 8943206, 
  12526723, 2019184, 100042, 11727645, 10853163, 11657281, 11420786)
zip.db <- read.csv('~/Box Sync/work/NPR One metrics/zip_to_timezone/zipcode/zipcode.csv', 
                   colClasses=c(zip='character'))

########################################
###  Directory of functions defined  ###
########################################
#' GetValidUserIds: List of user ids active, between dates if provided. Filters out known bots.
#' CountUsersWithActivity: Number of users active in a date range.
#' GetUserRatings: All ratings for one user.
#' GetAllUserRatings: All ratings in a time period for all users.
#' GetUserActiveDates: All dates that a user was active.
#' GetUsers: Gets user information from public_user.mpx_beta_testers table.
#' GetOrgs: Gets organization information from public_user.organization
#' GetTLH: Total listening hours across all users for a date range, by cohort if specified.
#' GetTlhByHourOfDay: Total listening hours for a date range broken out by hour of day.
#' GetUserDailyListening: Returns vector containing daily listing times for each user 
#'   for the date range. Users not identified.
#' GetUserTotalListening: Returns vector containing total listing times for each user 
#'   for the date range. Users not identified.
#' GetStories: Seamus ids for all stories rated during a date rrange.
#' GetRatingRatesForStory: data.frame with the ratings for the story and the rate 
#'   (fraction) for each of the ratings.
#' UserSessionDurations: Mean session length, mean session per user, and total time.
#' ActiveDaysPerWeek: Average number of days users were active per week.
#' 
#' UserHadActivityDuring: Given GetUserRatings returns TRUE if user was active in the 
#'   date range.
#' DaysSinceLastActiveDay: Days since the user was last active.
#' CountActions: Counts actions (ratings) given during a date range.
#' MeanNonZeroValues: Calculates the mean after dropping zeros.
#' ZipToTimezoneOffset: Offset for the timezone from Zulu (Greenwich).
#' AQH: Number of users listening during each quarter hour; 5 minute minimum during 
#'   each quarter hour.

##########################
###  DEFINE FUNCITONS  ###
##########################
GetValidUserIds <- function(start.date, end.date, platforms=default.platforms, driver=m, group=mysql.group) {
  #' Returns a list of all user_ids in ratings table. Filters out known bots.
  #' Limits to defined list of default.platforms.  Given a date range it limits
  #' the list to those users active during that date range.
  #' 
  #' Limiting to a date range greatly speeds up the query.
  con <- dbConnect(driver, group=group)
  SQLstatement <- paste("SELECT DISTINCT(ratings_user_id) FROM user_ratings ",
    "WHERE ratings_platform IN ('", 
    paste(platforms, collapse="', '"),
    "')", sep="")
  if(!missing(start.date)) {
    SQLstatement <- paste(SQLstatement, " AND DATE(ratings_timestamp) >= '", start.date, "'", sep="")
  }
  if(!missing(end.date)) {
    SQLstatement <- paste(SQLstatement, " AND DATE(ratings_timestamp) <= '", end.date, "'", sep="")
  }
  SQLstatement <- paste(SQLstatement, " ORDER BY ratings_user_id", sep='')
  rs <- dbSendQuery(con, SQLstatement)
  user.ids <- fetch(rs, n=-1)
  if(0==ncol(user.ids)) return(numeric(0))
  user.ids <- setdiff(user.ids[,1], robo.ids)  # remove robots
  dbDisconnect(con)
  return(user.ids)
}
# user.ids <- GetValidUserIds()
# user.ids <- GetValidUserIds(start.date='2014-03-31')
# user.ids <- GetValidUserIds(end.date='2014-04-13')
# user.ids <- GetValidUserIds(start.date='2014-03-31', end.date='2014-04-13', platforms='IPHONE')
# user.ids <- GetValidUserIds(start.date='2014-03-31', end.date='2014-04-13', platforms='ANDROID')
# user.ids <- GetValidUserIds(start.date='2014-05-04', end.date='2014-05-04', platforms='ANDROID') # should return numeric(0)
# user.ids

CountUsersWithActivity <- function(start.date, end.date, platforms=default.platforms, driver=m, group=mysql.group) {
  #' Given a date range returns the number of users with activity in that range.
  #' Uses GetValidUserIds and counts results.
  if(missing(start.date)) {
    if(missing(end.date)) {
      user.ids <- GetValidUserIds(platforms=platforms, 
                                  driver=driver, group=group)
    } else {
      user.ids <- GetValidUserIds(end.date=end.date, 
                                  platforms=platforms, 
                                  driver=driver, group=group)
    }
  } else {
    if(missing(end.date)) {
      user.ids <- GetValidUserIds(start.date=start.date, 
                                  platforms=platforms, 
                                  driver=driver, group=group)
    } else {
      user.ids <- GetValidUserIds(start.date=start.date, 
                                  end.date=end.date, 
                                  platforms=platforms, 
                                  driver=driver, group=group)
    }
  }
  return(length(user.ids))
}
# CountUsersWithActivity(start="2014-05-18", end="2014-05-24", platforms=c('IPHONE', 'ANDROID'))
# CountUsersWithActivity(start="2014-05-18", end="2014-05-24", platforms='IPHONE')
# CountUsersWithActivity(start="2014-05-18", end="2014-05-24", platforms='ANDROID')

GetUserRatings <- function(user.id, driver=m, group=mysql.group) {
  #' Given a user_id returns all the ratings (all fields) for that user.
  #' This is used in some other functions to analyze a single user's activity.
  con <- dbConnect(driver, group = group)
  rs <- dbSendQuery(con, paste("SELECT * FROM user_ratings where ratings_user_id = '",
                               user.id, "' AND ratings_elapsed < 24*60*60", sep=""))
  df <- fetch(rs, n=-1)
  df$ratings_timestamp <- ymd_hms(df$ratings_timestamp)
  dbDisconnect(con)
  return(df)
}
# user.ratings <- GetUserRatings(1764670) # Demian
# str(user.ratings)
# hist(user.ratings$ratings_timestamp, "weeks", freq=TRUE, main="Demian's Ratings", xlab="")

GetAllUserRatings <- function(start.date, end.date, platforms=default.platforms, driver=m, group=mysql.group) {
  #' Given a date range return all records from ratings for valid users
  con <- dbConnect(driver, group = group)
  SQLstatement <- paste("SELECT * FROM user_ratings ",
                        "WHERE ratings_platform IN ('", 
                        paste(platforms, collapse="', '"),
                        "')", sep="")
  if(!missing(start.date)) {
    SQLstatement <- paste(SQLstatement, " AND DATE(ratings_timestamp) >= '", start.date, "'", sep="")
  }
  if(!missing(end.date)) {
    SQLstatement <- paste(SQLstatement, " AND DATE(ratings_timestamp) <= '", end.date, "'", sep="")
  }
  SQLstatement <- paste(SQLstatement, " ORDER BY ratings_timestamp")
  rs <- dbSendQuery(con, SQLstatement)
  df <- fetch(rs, n=-1)
  dbDisconnect(con)
  # remove robots
  df <- df[!(df$ratings_user_id %in% robo.ids),]
  # format the timestamps
  df$ratings_timestamp <- ymd_hms(df$ratings_timestamp)
  # add dates
  df$ratings_date <- as.Date(df$ratings_timestamp)
  return(df)
}

GetUserActiveDates <- function(user.id, start.date, end.date, driver=m, group=mysql.group) {
  #' Given a user_id returns dates when that user was active.
  con <- dbConnect(driver, group = group)
  SQLstatement <- paste("SELECT DISTINCT(DATE(ratings_timestamp)) as dates ",
                        "FROM user_ratings ",
                        "WHERE ratings_user_id = '", user.id, "'", sep='')
  if(!missing(start.date)) {
    SQLstatement <- paste(SQLstatement, " AND DATE(ratings_timestamp) >= '", start.date, "'", sep="")
  }
  if(!missing(end.date)) {
    SQLstatement <- paste(SQLstatement, " AND DATE(ratings_timestamp) <= '", end.date, "'", sep="")
  }
  SQLstatement <- paste(SQLstatement, "ORDER BY dates")
  rs <- dbSendQuery(con, SQLstatement)
  df <- fetch(rs, n=-1)
  df$dates <- as.Date(df$dates)
  dbDisconnect(con)
  return(df$dates)
}
# GetUserActiveDates(1764670) # Demian

GetUsers <- function(driver=m, group=mysql.group) {
  con <- dbConnect(m, group = group)
  rs <- dbSendQuery(con, "SELECT * FROM public_user.mpx_beta_testers")
  users <- fetch(rs, n=-1)
  dbDisconnect(con)
  # remove robots
  users <- users[!(users$public_user_id %in% robo.ids),]
  return(users)
}
#' users <- GetUsers()

GetOrgs <- function(driver=m, group=mysql.group) {
  con <- dbConnect(m, group = group)
  rs <- dbSendQuery(con, "SELECT * FROM public_user.organization")
  orgs <- fetch(rs, n=-1)
  dbDisconnect(con)
  return(orgs)
}
#' orgs <- GetOrgs()

GetTLH <- function(start.date, 
                   end.date, 
                   cohort,
                   platforms=default.platforms, 
                   driver=m, 
                   group=mysql.group) {
  #' Returns TLH for all users in a date range.
  con <- dbConnect(driver, group = group)
  SQLstatement <- paste("SELECT SUM(ratings_elapsed) FROM user_ratings ", 
    "WHERE ratings_platform IN ('", paste(platforms, collapse="', '"), "') ",
    "AND DATE(ratings_timestamp) >= '", start.date, "' ",
    "AND DATE(ratings_timestamp) <= '", end.date, "' ",
    "AND ratings_user_id NOT IN (", paste(robo.ids, collapse=", "), ")", sep='')
  if(!missing(cohort)) SQLstatement <- paste(SQLstatement, " AND ratings_cohort IN ('",
                                             paste(cohort, collapse="', '"), 
                                             "')", sep="")
  rs <- dbSendQuery(con, SQLstatement)
  total.seconds <- as.vector(fetch(rs, n=-1)[1,1])
  dbDisconnect(con)
  return(total.seconds / 3600)
}
# GetTLH(start.date='2014-05-18', end.date='2014-05-24')

GetTlhByHourOfDay <- function(start.date, 
                              end.date, 
                              platforms=default.platforms, 
                              driver=m, 
                              group=mysql.group) {
  #' Returns TLH for a date range broken out by hour of day.
  cat("All times are local (including Daylight Saving) for the east coast.\n")
  
  con <- dbConnect(driver, group = group)
  SQLstatement <- paste("SELECT HOUR(ratings_timestamp) as hour_of_day, SUM(ratings_elapsed) / 3600 as TLH ",
                        "FROM user_ratings ",
                        "WHERE ratings_platform IN ('", paste(platforms, collapse="', '"), "') ",
                        " AND DATE(ratings_timestamp) >= '", start.date, "'", 
                        " AND DATE(ratings_timestamp) <= '", end.date, "'",
                        " AND ratings_user_id NOT IN (", paste(robo.ids, collapse=", "), ")",
                        " AND ratings_elapsed > 0",
                        " GROUP BY hour_of_day",
                        " ORDER BY hour_of_day", sep='')
  rs <- dbSendQuery(con, SQLstatement)
  df <- fetch(rs, n=-1)
  dbDisconnect(con)
  # fill in if missing hours
  out <- rep(0, 24)
  names(out) <- paste('h.', 0:23, sep='')
  for(i in 1:nrow(df)) out[df[i,1] + 1] <- df[i,2]
  return(out)
}
# tlh.by.hour <- GetTlhByHourOfDay(start.date='2014-01-01', end.date='2014-05-24')
# plot(0:23, tlh.by.hour, type='l')

GetUserDailyListening <- function(start.date, 
                                  end.date, 
                                  cohort,
                                  platforms=default.platforms, 
                                  driver=m, 
                                  group=mysql.group) {
  #' Returns, for the period specified, for each user-active-day, the number of minutes that
  #' user listened on that day.  The mean of this given mean user daily listening per active day.
  #' 
  #' Note that this is not the same as time using the app because we are
  #' limiting these times to SKIP, COMPLETED, and SHARE ratings.
  con <- dbConnect(driver, group = group)
  rs <- dbSendQuery(con, 
                    paste("SELECT SUM(ratings_elapsed) / 60 as daily_total_minutes FROM infinite.user_ratings ",
                          "WHERE DATE(ratings_timestamp) >= '", start.date, 
                          "' AND DATE(ratings_timestamp) <= '", end.date,
                          "' AND ratings_platform IN ('", paste(platforms, collapse="', '"), "')",
                          " AND ratings_rating IN ('SKIP','COMPLETED', 'SHARE') ",
                          " AND ratings_user_id NOT IN (", paste(robo.ids, collapse=", "), ")", 
                          ifelse(missing(cohort), "", paste(" AND ratings_cohort IN ('",
                                                            paste(cohort, collapse="', '"), 
                                                            "')", sep="")),
                          " GROUP BY ratings_user_id, DATE(ratings_timestamp)",
                          sep=""))
  df <- fetch(rs, n=-1)
  dbDisconnect(con)
  
  daily.total.minutes <- df[,"daily_total_minutes"]
  daily.total.minutes <- daily.total.minutes[!is.na(daily.total.minutes)]
  return(daily.total.minutes)
}
# round(MeanNonZeroValues(GetUserDailyListening(start.date="2014-03-31", end.date="2014-04-13")), 1)
# round(MeanNonZeroValues(udl <- GetUserDailyListening(start.date="2014-03-19", end.date="2014-04-18")), 1)
# MeanNonZeroValues(GetUserDailyListening(start.date="2014-06-01", end.date="2014-06-16", platforms='ANDROID'))

GetUserTotalListening <- function(start.date, 
                                  end.date, 
                                  cohort,
                                  platforms=default.platforms, 
                                  driver=m, 
                                  group=mysql.group) {
  #' Returns, for the period specified, for each user, the number of minutes that
  #' user listened during the period.
  #' 
  #' Note that this is not the same as time using the app because we are
  #' limiting these times to SKIP, COMPLETED, and SHARE ratings.
  con <- dbConnect(driver, group = group)
  rs <- dbSendQuery(con, 
                    paste("SELECT SUM(ratings_elapsed) / 60 as total_minutes FROM infinite.user_ratings ",
                          "WHERE DATE(ratings_timestamp) >= '", start.date, 
                          "' AND DATE(ratings_timestamp) <= '", end.date,
                          "' AND ratings_platform IN ('", paste(platforms, collapse="', '"), "')",
                          " AND ratings_rating IN ('SKIP','COMPLETED', 'SHARE') ",
                          " AND ratings_user_id NOT IN (", paste(robo.ids, collapse=", "), ")", 
                          ifelse(missing(cohort), "", paste(" AND ratings_cohort IN ('",
                                                            paste(cohort, collapse="', '"), 
                                                            "')", sep="")),
                          " GROUP BY ratings_user_id",
                          sep=""))
  df <- fetch(rs, n=-1)
  dbDisconnect(con)
  
  total.minutes <- df[,"total_minutes"]
  total.minutes <- total.minutes[!is.na(total.minutes)]
  return(total.minutes)
}

GetStories <- function(start.date, 
                       end.date, 
                       platforms=default.platforms, 
                       driver=m, 
                       group=mysql.group) {
  #' Given two dates return all the Seamus story ids for stories
  #' rated between those dates.
  con <- dbConnect(driver, group = group)
  SQLstatement <- paste("SELECT DISTINCT ratings_story_id FROM infinite.user_ratings ",
                        "WHERE ratings_platform IN ('", 
                        paste(platforms, collapse="', '"),
                        "')", sep="")
  if(!missing(start.date)) {
    SQLstatement <- paste(SQLstatement, " AND ratings_timestamp >= '", start.date, "'", sep="")
  }
  if(!missing(end.date)) {
    SQLstatement <- paste(SQLstatement, " AND ratings_timestamp < '", as.Date(end.date) + 1, "'", sep="")
  }
  SQLstatement <- paste(SQLstatement, " ORDER BY ratings_story_id")
  rs <- dbSendQuery(con, SQLstatement)
  df <- fetch(rs, n=-1)
  dbDisconnect(con)
  if(0 == nrow(df)) return (NULL)
  else return(df[,1])
}
# story.ids <- GetStories(start.date="2014-10-09", end.date="2014-10-10")

GetRatingRatesForStory <- function(story.id, 
                                   platforms=default.platforms,
                                   driver=m, 
                                   group=mysql.group) {
  #' Given a Seamus story id return a data.frame with the ratings
  #' for the story and the rate (fraction) for each of the ratings.
  con <- dbConnect(driver, group = group)
  SQLstatement <- paste("SELECT ratings_rating, count(ratings_rating) FROM user_ratings ",
                        "WHERE ratings_story_id = '", story.id, "' AND ",
                        "ratings_user_id NOT IN (", paste(robo.ids, collapse=", "), ") AND ",
                        "ratings_platform IN ('", paste(platforms, collapse="', '"),"') ",
                        "GROUP BY ratings_rating",
                        sep="")
  rs <- dbSendQuery(con, SQLstatement)
  ratings <- fetch(rs, n=-1)
  dbDisconnect(con)
  if(0==ncol(ratings)) return(NULL)
  names(ratings) <- c("rating", "count")
  ratings$share <- ratings$count / sum(ratings$count)
  return(ratings)
}
#' rt <- GetRatingRatesForStory(189522647)

UserSessionDurations <- function(start.date, 
                                 end.date, 
                                 max.n.users, 
                                 session.delta.seconds,
                                 platforms=default.platforms, 
                                 driver=m,
                                 group=mysql.group) {
  #' Given a date range and a possible sample size of max.n.users
  #' returns mean session length (minutes), mean number of sessions per user,
  #' and total TSL (minutes) for a date rarnge.
  start.date <- as.Date(start.date)
  end.date <- as.Date(end.date)

# Require users to have have installed before period.start and have some activity during the period
  cat("Generating list of users active during set period...\n")
  user.ids <- intersect(GetValidUserIds(end.date=start.date - 1, 
                                        driver=driver, group=group),
                        GetValidUserIds(start.date=start.date, end.date=end.date, 
                                        driver=driver, group=group))
  
  # Sample the users if necessary
  if(length(user.ids) > max.n.users) {
    cat("Sampling from ", length(user.ids), " users.\n")
    user.ids <- sample(user.ids, max.n.users)
  }
  n.users <- length(user.ids)
  
  ######################################################
  ###  Generate list (by user) of lists of sessions  ###
  ######################################################
  cat("Parsing users sessions...\n")
  pb <- txtProgressBar(max=n.users, style=2)
  user.sessions <- lapply(1:n.users, function(i) {
    setTxtProgressBar(pb, i)
    uid <- user.ids[i]
    ur <- GetUserRatings(uid, driver=driver, group=group)   # get user ratings for this user
    ur$date <- as.Date(ur$ratings_timestamp)
    ur <- ur[ur$date >= start.date & ur$date <= end.date,]  # filter on period
    ur <- ur[order(ur$ratings_timestamp),]                  # sort user ratings chronologically
    break.after <- which(diff(ur$ratings_timestamp) > session.delta.seconds)  # find breaks between sessions
    # identify which ratings (rows in ur) a grouped into sessions
    if(length(break.after) > 0) {
      # if there's more than one session
      session.ids <- lapply(1:length(break.after), function(break.i) {
        if(break.i == 1) return(1:break.after[1])
        else return((break.after[break.i - 1]+1):break.after[break.i])
      })
      session.ids <- c(session.ids, list((break.after[length(break.after)]+1):nrow(ur)))
    } else {
      # only one session
      session.ids <- list(1:nrow(ur))
    }
    return(lapply(session.ids, function(ids) {
      out <- ur[ids,]
      rownames(out) <- NULL
      return(out)
    }))
  })
  close(pb)
  
  mean.num.sessions <- mean(sapply(user.sessions, length))
  
  # Examine session durations
  user.session.durations <- lapply(user.sessions, function(this.users.sessions) {
    sapply(this.users.sessions, function(this.session) {
      sum(this.session$ratings_elapsed)
    })
  })
  
  all.session.durations.sec <- unlist(user.session.durations)
  all.session.durations.sec <- all.session.durations.sec[!(0 == all.session.durations.sec)]
  
  out <- list(mean.session.min=mean(all.session.durations.sec) / 60,
              mean.num.sessions=mean.num.sessions,
              all.session.min=all.session.durations.sec / 60)
  return(out)
}
#us <- UserSessionDurations(start.date="2014-06-29", end.date="2014-07-12", max.n.users=1e4, session.delta.seconds=30 * 60, platforms=default.platforms, driver=m)

ActiveDaysPerWeek <- function(start.date, 
                              per.length.days, 
                              platforms=default.platforms, 
                              driver=m, 
                              group=mysql.group) {
  #' Given a date range return the average number of days users were active per week
  end.date <- format(ymd(start.date) + (per.length.days - 1) * 24 * 60 * 60, "%Y-%m-%d")
  con <- dbConnect(driver, group=group)
  rs <- dbSendQuery(con, paste("SELECT COUNT(DISTINCT DATE(ratings_timestamp)) AS num_days_active FROM infinite.user_ratings ",
                               "WHERE DATE(ratings_timestamp) >= '", start.date, 
                               "' AND DATE(ratings_timestamp) <= '", end.date,
                               "' AND ratings_platform IN ('", paste(platforms, collapse="', '"), "')",
                               " AND ratings_rating IN ('SKIP','COMPLETED')",
                               " AND ratings_user_id NOT IN (", paste(robo.ids, collapse=", "), ")", 
                               " GROUP BY ratings_user_id",
                               sep=""))
  df <- fetch(rs, n=-1)
  dbDisconnect(con)
  return(df[,"num_days_active"] / per.length.days * 7)
}
# adw <- ActiveDaysPerWeek(start="2014-03-31", per.length.days=14)
# hist(adw, main="Active Days per Week by User", xlab=paste("days  mean =", round(mean(adw), 2)))
# abline(v=mean(adw))

##########################################
###  Calculations on things Retrieved  ###
##########################################
UserHadActivityDuring <- function(ur, start, end) {
  # Given user ratings for a user and start and end dates formatted as
  # strings YYYY-MM-DD returns TRUE if the user has any activity in
  # the specified date range.
  start.date <- as.Date(ymd(start))
  end.date <- as.Date(ymd(end))
  ur$ratings_timestamp <- as.Date(ur$ratings_timestamp)
  return(sum(ur$ratings_timestamp >= start.date) * sum(ur$ratings_timestamp <= end.date) > 0)
}
# UserHadActivityDuring(ur=GetUserRatings(1764670), start="2014-03-31", end="2014-04-13")


DaysSinceLastActiveDay <- function(start.date, 
                                   end.date, 
                                   n.sample, 
                                   driver=m, 
                                   group=mysql.group) {
  #' Given a date range, for each user active during that time,
  #' for each day that user was active, given the number of days
  #' since the user was previously active.
  #' 
  #' Supports sampling suitable for post-release.
  days.since.last.active.day <- numeric(0)
  
  days <- seq(from=as.Date(start.date), to=as.Date(end.date), by="days")
  
  user.ids <- GetValidUserIds(start.date=start.date, 
                              end.date=end.date, 
                              driver=driver, 
                              group=group)
  if(!missing(n.sample)) user.ids <- sample(user.ids, n.sample)
  
  pb <- txtProgressBar(max=length(user.ids), style=2)
  for(i in 1:length(user.ids)) {
    setTxtProgressBar(pb, i)
    uid <- user.ids[i]
    ur <- user.ratings <- GetUserRatings(uid, driver=driver, group=group)
    if(UserHadActivityDuring(ur, start=start.date, end=end.date)) {
      for(j in 1:length(days)) {
        this.day <- days[j]
        if(UserHadActivityDuring(ur, start=as.character(this.day), end=as.character(this.day))) {
          previous.days <- as.Date(ur$ratings_timestamp)[as.Date(ur$ratings_timestamp) < this.day]
          if(length(previous.days) > 0) {
            previous.active.date <- max(previous.days)
            days.since.last.active.day <- c(days.since.last.active.day,
                                            as.POSIXct(this.day) - as.POSIXct(previous.active.date))
          }
        }
      }
    }
  }
  close(pb)
  return(days.since.last.active.day)
}
# dslad <- DaysSinceLastActiveDay(start.date="2014-03-31", end.date="2014-04-13", n.sample=1000)
# dslad <- DaysSinceLastActiveDay(start.date="2014-03-31", end.date="2014-04-13")
# hist(dslad, 250, main="Days Since Last Active Day", xlab=paste("days  median =", round(median(dslad), 2)), xlim=c(0, 40))
# abline(v=median(dslad))
# mean(dslad[dslad < quantile(dslad, .95)])
# mean(dslad[dslad < 60])

CountActions <- function(start.date, end.date, action, platforms=default.platforms, driver=m) {
  #' Given a date range and an action type, return the number of those events
  con <- dbConnect(driver, group = "stage4")
  rs <- dbSendQuery(con, paste("SELECT COUNT(ratings_rating) AS rating_count FROM user_ratings ",
                               "WHERE DATE(ratings_timestamp) >= '", start.date, 
                               "' AND DATE(ratings_timestamp) <= '", end.date,
                               "' AND ratings_platform IN ('", paste(platforms, collapse="', '"), "')",
                               " AND ratings_rating = '", action, "'",
                               " AND ratings_user_id NOT IN (", paste(robo.ids, collapse=", "), ")", 
                               sep=""))
  df <- fetch(rs, n=-1)
  dbDisconnect(con)
  return(df[1,"rating_count"])
}
# CountActions(start.date="2014-03-31", end.date="2014-04-13", action="SKIP")
# CountActions(start.date="2014-03-31", end.date="2014-04-13", action="COMPLETED")

MeanNonZeroValues <- function(x) {
  if(all(x==0)) return(0)
  else return(mean(x[x!=0]))
}

LinesWithConfInt <- function(xvalues, 
                             ymatrix, 
                             int.probs=c(.25, .75), 
                             col='grey', 
                             middle=median) {
  #' Not sure what this does.
  lines.w.conf.int.all <- apply(ymatrix, 2, function(yvalues) {
    clean.y.values <- yvalues[!is.na(yvalues)]
    clean.y.values <- clean.y.values[!clean.y.values==0]
    out <- quantile(clean.y.values, probs=c(int.probs[1], .5, int.probs[2]))
    out["50%"] <- middle(clean.y.values)
    out
  })
  polygon(x=c(xvalues, rev(xvalues)),
          y=c(lines.w.conf.int.all[1,], rev(lines.w.conf.int.all[3,])),
          col=col)
  lines(x=xvalues, y=lines.w.conf.int.all[2,], lwd=2)
}

ZipToTimezoneOffset <- function(zip.code) {
  #' Given a zip code returns the offset for the timezone from Zulu (Greenwich)
  stopifnot(is.character(zip.code))
  
  if(1 == length(zip.code)) {
    row.ids <- which(zip.db == zip.code)
    if(length(row.ids) < 1) offset <- NA
    else offset <- zip.db$timezone[row.ids[1]]
  } else {
    offset <- sapply(zip.code, ZipToTimezoneOffset)
  }
  return(offset)
}
# ZipToTimezoneOffset('00213')
# ZipToTimezoneOffset(c('00213', '95695'))

AQH <- function(start.date, 
                end.date, 
                platforms=default.platforms, 
                sample.size=5000, 
                driver=m, 
                group=mysql.group) {
  #' Returns number of users listening in each quarter hour during a date range.
  #' Limits to users listening at least five minutes during that quarter hour.
  start.date <- as.Date(start.date)
  end.date <- as.Date(end.date)
  
  user.ids <- GetValidUserIds(start.date=start.date, 
                              end.date=end.date, 
                              platforms=platforms,
                              driver=driver, 
                              group=group)
  original.n <- length(user.ids)
  if(original.n > sample.size) {
    user.ids <- sample(user.ids, sample.size, replace=TRUE)
  }
  
  pb <- txtProgressBar(max=length(user.ids), style=2)
  user.qh <- laply(1:length(user.ids), function(uid.index) {
    #' Return an array with users as rows, quarter hours as columns, 
    #' and entries TRUE if that user was listening that quarter hour for at least 5 minutes
    setTxtProgressBar(pb, uid.index)
    uid <- user.ids[uid.index]
    ur <- GetUserRatings(uid, driver=driver, group=group)
    ur$date <- as.Date(ur$ratings_timestamp)
    ur <- ur[ur$date >= start.date & ur$date <= end.date & !is.na(ur$ratings_elapsed),]
    quarter.hours <- seq(from=as.POSIXlt(paste(start.date, "00:00")),
                         to=as.POSIXlt(paste(as.Date(end.date)+1, "00:00")),
                         by="15 min")
    groups <- cut(ur$ratings_timestamp, quarter.hours)
    elapsed.groups <- split(ur$ratings_elapsed, groups)
    qh <- sapply(elapsed.groups, function(this.qh) sum(this.qh) > 5 * 60) # where 5 is the minimum number of minutes
    return(qh)
  })
  close(pb)
  
  aqh <- mean(colSums(user.qh)) * original.n / length(user.ids)
  listeners.by.qh <- data.frame("qh.starting"=colnames(user.qh),
                                "n.listeners"=colSums(user.qh) * original.n / length(user.ids))
  rownames(listeners.by.qh) <- NULL
  return(list(aqh=aqh, listeners.by.qh=listeners.by.qh))
}
#' AQH('2014-06-15', '2014-06-21')
#' AQH('2014-09-28', '2014-09-28', sample.size=50)
#' AQH('2014-09-21', '2014-09-27')