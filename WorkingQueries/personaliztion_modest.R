#' Checks of personalization modest first step

source('~/github/MPX/WorkingQueries/sprint_report_functions.R')

GetSkippingUsers <- function(start.date, end.date, driver=m) {
  #' Given a date range returns the unique users in the 
  #' personalization table, infinite.user_skip_personalization,
  #' with records in that date range.
}

GetPersonalizationRecords <- function(start.date, end.date, driver=m) {
  #' Given dates returns records in infinite.user_skip_personalization
  #' during that period. Generally used as a helper function.
  #' 
  #' The fields sper_from_period and sper_to_period say that the record
  #' represents a change of state from/to states:
  #'   0: no/some skips
  #'   1: hiatus
  #'   2: recovery
  con <- dbConnect(driver, group = "stage4")
  SQLstatement <- paste("SELECT * FROM infinite.user_skip_personalization ",
                        "WHERE DATE(sper_timestamp) >= '", start.date, "' ",
                        "AND DATE(sper_timestamp) <= '", end.date, "'", sep="")
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

CurrentPersonalization <- function(start.date, 
                                   end.date, 
                                   start.search=as.Date(start.date) - 30, 
                                   verbose=FALSE,
                                   driver=m) {
  #' Gets all users with any hiatus during the period specified.
  #' To do this, it determines all users with hiatus initiation
  #' on or after after start.search and on or prior to end.date 
  #' then checks to see if that hiatus has ended.  Returns list 
  #' with vector of unique users having any hiatus and a data.frame 
  #' with user.ids and ORIGINs on hiatus at any point during the 
  #' period.
  pers.rec <- GetPersonalizationRecords(start.date=start.date,
                                        end.date=end.date,
                                        driver=driver)
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
#' cp <- CurrentPersonalization(start.date="2014-10-01", end.date="2014-10-01", verbose=TRUE)
