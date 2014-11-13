#' Checks of personalization modest first step

source('~/github/MPX/WorkingQueries/sprint_report_functions.R')

GetSkippingUsers <- function(start.date, end.date, driver=m) {
  #' Given a date range returns the unique users in the 
  #' personalization table, infinite.user_skip_personalization,
  #' with records in that date range.
}

GetPersonalizationRecords <- function(start.date, end.date, driver=m, group=mysql.group) {
  #' Given dates returns records in infinite.user_skip_personalization
  #' during that period. Generally used as a helper function.
  #' 
  #' The fields sper_from_period and sper_to_period say that the record
  #' represents a change of state from/to states:
  #'   0: no/some skips
  #'   1: hiatus
  #'   2: recovery
  con <- dbConnect(driver, group=group)
  SQLstatement <- "SELECT * FROM infinite.user_skip_personalization"
  if(!missing(start.date)) {
    if(!missing(end.date)) {
      SQLstatement <- paste(SQLstatement, " WHERE DATE(sper_timestamp) >= '", start.date,
                            "' AND DATE(sper_timestamp) <= '", end.date, "'", sep="")
    } else {
      SQLstatement <- paste(SQLstatement, " WHERE DATE(sper_timestamp) >= '", start.date, 
                            "'", sep="")
    }
  } else {
    if(!missing(end.date)) {
      SQLstatement <- paste(SQLstatement, " WHERE DATE(sper_timestamp) <= '", end.date, 
                            "'", sep="")  
    }
  }
  rs <- dbSendQuery(con, SQLstatement)
  records <- fetch(rs, n=-1)
  dbDisconnect(con)
  if(0==ncol(records)) {
    return(NULL)
  } else {
    records$date <- as.Date(records$sper_timestamp)
    return(records)
  }
}
#' pers.rec <- GetPersonalizationRecords("2014-09-25", "2014-09-30")
#' pers.rec <- GetPersonalizationRecords()

PersonalizationDuringPeriod <- function(start.date, 
                                        end.date, 
                                        start.search=as.Date(start.date) - 30, 
                                        verbose=FALSE,
                                        driver=m,
                                        group=mysql.group) {
  #' Gets all users with any hiatus during the period specified.
  #' To do this, it determines all users with hiatus initiation
  #' on or after after start.search and on or prior to end.date 
  #' then checks to see if that hiatus has ended.  Returns list 
  #' with vector of unique users having any hiatus and a data.frame 
  #' with user.ids and ORIGINs on hiatus at any point during the 
  #' period.
  if(!missing(start.date)) {
    if(!missing(end.date)) {
      pr <- GetPersonalizationRecords(start.date=start.search,
                                      end.date=end.date,
                                      driver=driver,
                                      group=group)  
    } else {
      pr <- GetPersonalizationRecords(start.date=start.search,
                                      driver=driver,
                                      group=group)  
    }
  } else {
    if(!missing(end.date)) {
      pr <- GetPersonalizationRecords(end.date=end.date,
                                      driver=driver,
                                      group=group)  
    }
  }
  pr <- pr[order(pr$sper_timestamp),]
  
  #' initialize output
  user.origins.on.hiatus <- data.frame(user.id=numeric(0), origin=character(0))
  
  #' Loop through users and origins, checking status
  uids <- unique(pr$sper_user_id)
  if(verbose) pb <- txtProgressBar(max=length(uids))
  for(i in 1:length(uids)) {
    if(verbose) setTxtProgressBar(pb, i)
    uid <- uids[i]
    user.pr <- pr[pr$sper_user_id == uid,]
    for(origin in unique(user.pr$sper_origin)) {
      user.origin.pr <- user.pr[user.pr$sper_origin == origin,]
      hiatus <- FALSE
      
      pre.uopr <- user.origin.pr[user.origin.pr$date < start.date,]
      during.uopr <- user.origin.pr[user.origin.pr$date >= start.date,]
      if(nrow(pre.uopr) > 0 && 1 == pre.uopr$sper_to_period[nrow(pre.uopr)]) {
        #' on hiatus as of beginning of period period
        hiatus <- TRUE
      } else {
        #' transitions to hiatus/recovery after start.date, on hiatus
        if(0 == nrow(during.uopr)) next
        if(any(1 == during.uopr$sper_to_period)) hiatus <- TRUE
      }
      if(hiatus) {
        user.origins.on.hiatus <- rbind(user.origins.on.hiatus,
                                        data.frame(user.id=uid,
                                                   origin=origin))
      }
    }
  }
  if(verbose) close(pb)
  return( list(user.ids=unique(user.origins.on.hiatus$user.id),
               users.and.origins=user.origins.on.hiatus) )
}
#' pdp <- PersonalizationDuringPeriod(start.date="2014-10-01", end.date="2014-10-01", verbose=TRUE)

CurrentPersonalization <- function(earliest.date=Sys.Date() - 30,
                                   include.in.recovery=TRUE,
                                   verbose=FALSE,
                                   driver=m, 
                                   group=mysql.group) {
  
  if(include.in.recovery == TRUE) {
    status.codes <- 1:2
  } else {
    status.codes <- 1
  }
  
  pr <- GetPersonalizationRecords(start.date=earliest.date,
                                  driver=driver,
                                  group=group)  
  #' sort the personalization records
  pr <- pr[order(pr$sper_timestamp),]
  #' get the list of users with records
  uids <- unique(pr$sper_user_id)
  #' prepare the output
  out <- data.frame(user.id=numeric(0), origin=character(0))
  # cycle through the user ids
  if(verbose) pb <- txtProgressBar(max=length(uids))
  for(i in 1:length(uids)) {
    if(verbose) setTxtProgressBar(pb, i)
    uid <- uids[i]
    user.pr <- pr[pr$sper_user_id == uid,]
    for(origin in unique(user.pr$sper_origin)) {
      user.origin.pr <- user.pr[user.pr$sper_origin == origin,]
      hiatus <- FALSE
      if(nrow(user.origin.pr) > 0 && 
           user.origin.pr$sper_to_period[nrow(user.origin.pr)] %in% status.codes) {
        #' on hiatus or in recovery if included
        out <- rbind(out,
                     data.frame(user.id=uid, origin=origin))
        
      }
    }
  }
  if(verbose) close(pb)
  return(list(spec=ifelse(include.in.recovery, "Includes users on hiatus or in recovery",
                          "Includes only users on hiatus"),
              current.hiatus=out))
}
#' cp <- CurrentPersonalization()

GetUserStation <- function(user.ids, driver=m, group=mysql.group) {
  #' Given a vector of user.ids return a data from with information about the
  #' station each is localized to.
  
  con <- dbConnect(driver, group=group)
  
  #' Get org ids for each
  org.ids <- sapply(user.ids, function(uid) {
    SQLstatement <- paste("SELECT * FROM public_user.public_user_stations WHERE stations_preference_order = 0 AND stations_public_user_id = '", uid, "'", sep="")
    rs <- dbSendQuery(con, SQLstatement)
    records <- fetch(rs, n=-1)
    if(0 == nrow(records)) {
      return(NA_integer_)
    } else {
      return(records[1,"stations_org_id"])
    }
  })
  out <- data.frame(user.id=user.ids, org.id=org.ids)
  
  SQLstatement <- "SELECT * FROM public_user.organization"
  rs <- dbSendQuery(con, SQLstatement)
  org.data <- fetch(rs, n=-1)
  
  out <- merge(x=out, y=org.data, by.x="org.id", by.y="org_id", all.x=TRUE)
  
  dbDisconnect(con)
  
  return(out)
}
#' cp <- CurrentPersonalization()
#' cp.org <- GetUserStation(cp$current.hiatus$user.id)
#' table(cp.org$org_abbr)
