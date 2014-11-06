library(lubridate)
library(RMySQL)
library(plyr)
library(reshape2)
m <- dbDriver("MySQL")
default.platforms <- c('IPHONE', 'ANDROID')

try(setwd("/home/developer/MPX/cronjobs/results"))

# start.date <- Sys.Date() - 4
# end.date <- Sys.Date() - 4
test_cohorts <- c('P','Q')

Get_Users <- function(start.date, end.date, test_cohorts, platforms=default.platforms, driver=m, group="stage4") {

  co <- c()
  for(i in 1:length(test_cohorts)){
  co <- c(co,paste("(SELECT count(ratings_rating) from infinite.user_ratings WHERE ratings_cohort = ",
                                               test_cohorts[i]," AND ratings_user_id = this.ratings_user_id) as ",
                                               test_cohorts[i],", ",sep="", collapse="', '"))}
    
  con <- dbConnect(m, group = group)
  SQLstatement <- paste("SELECT ratings_user_id, 
                        min(date(ratings_timestamp)) as FirstDayActive,
                        max(date(ratings_timestamp)) as LastDayActive, 
                        count(ratings_rating) as TotalRatings, 
                        count(DISTINCT DATE(ratings_timestamp)) as activeDays, 
                        sum(ratings_elapsed) as TotalSeconds, ",
                        paste(co, collapse=""), "
                        (SELECT count(ratings_rating) from infinite.user_ratings WHERE ratings_platform = 'IPHONE' AND ratings_user_id = this.ratings_user_id) as IPHONEratings,
                        (SELECT count(ratings_rating) from infinite.user_ratings WHERE ratings_platform = 'ANDROID' AND ratings_user_id = this.ratings_user_id) as ANDROIDratings,
                        (SELECT count(ratings_rating) from infinite.user_ratings WHERE ratings_platform = 'CRMAPP' AND ratings_user_id = this.ratings_user_id) as WEBAPPratings,
                        (SELECT count(ratings_rating) from infinite.user_ratings WHERE ratings_rating = 'SKIP' AND ratings_user_id = this.ratings_user_id) as Skips,
                        (SELECT count(ratings_rating) from infinite.user_ratings WHERE ratings_rating = 'THUMBUP' AND ratings_user_id = this.ratings_user_id) as Thumbups,
                        (SELECT count(ratings_rating) from infinite.user_ratings WHERE ratings_rating = 'COMPLETED' AND ratings_user_id = this.ratings_user_id) as Completes,
                        (SELECT count(ratings_rating) from infinite.user_ratings WHERE ratings_rating IN ('SRCHSTART','SRCHCOMP') AND ratings_user_id = this.ratings_user_id) as Searches,
                        (SELECT count(ratings_rating) from infinite.user_ratings WHERE ratings_origin IN ('LOCALPOD','FEATURED','RATED') AND ratings_user_id = this.ratings_user_id) as TotalPodcasts,
                        (SELECT count(ratings_rating) from infinite.user_ratings WHERE ratings_origin IN ('LOCALPOD','FEATURED','RATED') AND ratings_rating = 'SKIP' AND ratings_user_id = this.ratings_user_id) as PodcastSkips,
                        (SELECT count(ratings_rating) from infinite.user_ratings WHERE ratings_origin = 'RATED' AND ratings_user_id = this.ratings_user_id) as RatedPlays,
                        (SELECT count(ratings_rating) from infinite.user_ratings WHERE ratings_origin = 'RATED' AND ratings_rating = 'SKIP' AND ratings_user_id = this.ratings_user_id) as RatedSkips,
                        (SELECT count(ratings_rating) from infinite.user_ratings WHERE ratings_origin = 'LOCALNC' AND ratings_user_id = this.ratings_user_id) as LocalNewscasts,
                        (SELECT count(ratings_rating) from infinite.user_ratings WHERE ratings_origin = 'LOCALNC' AND ratings_rating = 'SKIP' AND ratings_user_id = this.ratings_user_id) as LocalNCskips,
                        (SELECT count(ratings_rating) from infinite.user_ratings WHERE ratings_origin = 'ORGZN' AND ratings_user_id = this.ratings_user_id) as Localstories,
                        (SELECT count(ratings_rating) from infinite.user_ratings WHERE ratings_origin = 'HRNC' AND ratings_user_id = this.ratings_user_id) as HRNCs,
                        (SELECT count(ratings_rating) from infinite.user_ratings WHERE ratings_origin = 'HRNC' AND ratings_rating = 'SKIP' AND ratings_user_id = this.ratings_user_id) as HRNCskips,
                        (SELECT count(ratings_rating) from infinite.user_ratings WHERE ratings_origin = 'LEAD' AND ratings_user_id = this.ratings_user_id) as LeadPlays,
                        (SELECT count(ratings_rating) from infinite.user_ratings WHERE ratings_origin = 'LEAD' AND ratings_rating = 'SKIP' AND ratings_user_id = this.ratings_user_id) as LeadSkips,
                        (SELECT count(ratings_rating) from infinite.user_ratings WHERE ratings_origin = 'BREAK' AND ratings_user_id = this.ratings_user_id) as BreakPlays,
                        (SELECT count(ratings_rating) from infinite.user_ratings WHERE ratings_origin = 'BREAK' AND ratings_rating = 'SKIP' AND ratings_user_id = this.ratings_user_id) as BreakSkips,
                        (SELECT count(ratings_rating) from infinite.user_ratings WHERE ratings_origin = 'ASSIST' AND ratings_user_id = this.ratings_user_id) as AssistPlays,
                        (SELECT count(ratings_rating) from infinite.user_ratings WHERE ratings_origin = 'ASSIST' AND ratings_rating = 'SKIP' AND ratings_user_id = this.ratings_user_id) as AssistSkips,
                        FROM infinite.user_ratings as this ",
                        "WHERE ratings_platform IN ('", paste(default.platforms, collapse="', '"), "') ",
                        "AND DATE(ratings_timestamp) >= '", start.date, "' ",
                        "AND DATE(ratings_timestamp) <= '", end.date, "' ",
                        "GROUP BY ratings_user_id limit 100000000", 
                        sep='')
  rs <- dbSendQuery(con, SQLstatement)
  df <- fetch(rs, n=-1)
  print(paste("the query has", nrow(df), "ratings"))

}
