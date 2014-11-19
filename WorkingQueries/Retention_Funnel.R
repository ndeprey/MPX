
default.platforms <- c('IPHONE', 'ANDROID')
library(lubridate)
library(RMySQL)
library(plyr)
library(reshape2)


m <- dbDriver("MySQL")

## IF YOU GET AN ERROR for too many connections open, use the following code
## cons <- dbListConnections(MySQL())
## for (con in cons){
## dbDisconnect(con) }

Get_Retention_Funnel <- function(start.date, end.date){

  ### Create a list of start and end dates for week calculations used in MySQL call ###
  start.dates <- seq.Date(as.Date(start.date),as.Date(end.date),by="week")
  end.dates <- seq.Date(as.Date(start.date)+6,start.dates[length(start.dates)]+6,by="week")
  Weeks <- c()
  for(i in 1:length(start.dates)){
    Weeks[i] <- as.character(paste("Week",i,sep=""))
  }
  
  weeks.statement <- c()
  for(i in 1:length(start.dates)){
    weeks.statement[i] <- paste("(SELECT count(ratings_rating) from user_ratings WHERE date(ratings_timestamp) >= '",
          start.dates[i],"' AND date(ratings_timestamp) <= '",
          end.dates[i],"' AND ratings_user_id = this.ratings_user_id) as ",
          Weeks[i],", ",
          sep='',collapse='')
  }
  weeks.statement <- paste(weeks.statement,sep='',collapse='')
  
  Get_Users_Master <- function(platforms=default.platforms, driver=m, group="stage4") {
    
    con <- dbConnect(m, group = group)
    SQLstatement <- paste("SELECT ratings_user_id,
                          min(date(ratings_timestamp)) as create_date,
                          max(date(ratings_timestamp)) as last_listen_date, 
                          count(DISTINCT DATE(ratings_timestamp)) as total_active_days, 
                          round(sum(ratings_elapsed) / 60) as lifetime_listening_minutes, ",
                          weeks.statement, "
                          (SELECT count(ratings_rating) from user_ratings WHERE ratings_platform = 'IPHONE' AND ratings_user_id = this.ratings_user_id) as IPHONEratings,
                          (SELECT count(ratings_rating) from user_ratings WHERE ratings_platform = 'ANDROID' AND ratings_user_id = this.ratings_user_id) as ANDROIDratings,
                          (SELECT count(ratings_rating) from user_ratings WHERE ratings_platform = 'CRMAPP' AND ratings_user_id = this.ratings_user_id) as WEBAPPratings,
#                           (SELECT count(ratings_rating) from user_ratings WHERE ratings_rating = 'SKIP' AND ratings_user_id = this.ratings_user_id) as Skips,
#                           (SELECT count(ratings_rating) from user_ratings WHERE ratings_rating = 'THUMBUP' AND ratings_user_id = this.ratings_user_id) as Thumbups,
#                           (SELECT count(ratings_rating) from user_ratings WHERE ratings_rating = 'COMPLETED' AND ratings_user_id = this.ratings_user_id) as Completes,
#                           (SELECT count(ratings_rating) from user_ratings WHERE ratings_rating = 'SHARE' AND ratings_user_id = this.ratings_user_id) as Shares,
#                           (SELECT count(ratings_rating) from user_ratings WHERE ratings_rating IN ('SRCHSTART','SRCHCOMP') AND ratings_user_id = this.ratings_user_id) as Searches,
#                           (SELECT count(ratings_rating) from user_ratings WHERE ratings_origin IN ('LOCALPOD','FEATURED','RATED') AND ratings_user_id = this.ratings_user_id) as TotalPodcasts,
#                           (SELECT count(ratings_rating) from user_ratings WHERE ratings_origin IN ('LOCALPOD','FEATURED','RATED') AND ratings_rating = 'SKIP' AND ratings_user_id = this.ratings_user_id) as PodcastSkips,
#                           (SELECT count(ratings_rating) from user_ratings WHERE ratings_origin = 'RATED' AND ratings_user_id = this.ratings_user_id) as RatedPlays,
#                           (SELECT count(ratings_rating) from user_ratings WHERE ratings_origin = 'RATED' AND ratings_rating = 'SKIP' AND ratings_user_id = this.ratings_user_id) as RatedSkips,
#                           (SELECT count(ratings_rating) from user_ratings WHERE ratings_origin = 'LOCALNC' AND ratings_user_id = this.ratings_user_id) as LocalNewscasts,
#                           (SELECT count(ratings_rating) from user_ratings WHERE ratings_origin = 'LOCALNC' AND ratings_rating = 'SKIP' AND ratings_user_id = this.ratings_user_id) as LocalNCskips,
#                           (SELECT count(ratings_rating) from user_ratings WHERE ratings_origin = 'ORGZN' AND ratings_user_id = this.ratings_user_id) as Localstories,
#                           (SELECT count(ratings_rating) from user_ratings WHERE ratings_origin = 'HRNC' AND ratings_user_id = this.ratings_user_id) as HRNCs,
#                           (SELECT count(ratings_rating) from user_ratings WHERE ratings_origin = 'HRNC' AND ratings_rating = 'SKIP' AND ratings_user_id = this.ratings_user_id) as HRNCskips,
#                           (SELECT count(ratings_rating) from user_ratings WHERE ratings_origin = 'LEAD' AND ratings_user_id = this.ratings_user_id) as LeadPlays,
#                           (SELECT count(ratings_rating) from user_ratings WHERE ratings_origin = 'LEAD' AND ratings_rating = 'SKIP' AND ratings_user_id = this.ratings_user_id) as LeadSkips,
#                           (SELECT count(ratings_rating) from user_ratings WHERE ratings_origin = 'BREAK' AND ratings_user_id = this.ratings_user_id) as BreakPlays,
#                           (SELECT count(ratings_rating) from user_ratings WHERE ratings_origin = 'BREAK' AND ratings_rating = 'SKIP' AND ratings_user_id = this.ratings_user_id) as BreakSkips,
#                           (SELECT count(ratings_rating) from user_ratings WHERE ratings_origin = 'ASSIST' AND ratings_user_id = this.ratings_user_id) as AssistPlays,
                          (SELECT count(ratings_rating) from user_ratings WHERE ratings_origin = 'ASSIST' AND ratings_rating = 'SKIP' AND ratings_user_id = this.ratings_user_id) as AssistSkips ",
                          "FROM infinite.user_ratings as this
                          GROUP BY ratings_user_id limit 100000000",
                          sep='')
    rs <- dbSendQuery(con, SQLstatement)
    df <- fetch(rs, n=-1)
    print(paste("the query has", nrow(df), "ratings"))
    return(df)
  }
          
  
  ### Read in and Prepare Data ###
  # u <- read.csv("/Users/ndeprey/Documents/users_11_04.csv")
  u <- Get_Users_Master()
  
  print("removing temp users...")
  # Remove temp users #
  u <- u[u$ratings_user_id < 1000000000,]
  
  print("munging dates...")
  # Munge dates #
  u$create_date <- sapply(u$create_date,function(i){
    max(as.Date(strptime(i,format="%Y-%m-%d")),as.Date(strptime("2014-07-28",format="%Y-%m-%d")))
  })
  u$create_date <- as.Date(u$create_date, origin="1970-01-01")
  
  u$last_listen_date <- as.Date(strptime(u$last_listen_date, "%Y-%m-%d"))
  
  print("removing old beta testers...")
  # remove old beta users and testers prior to launch #
  u <- u[u$last_listen_date >= "2014-07-28",]
  
  print("subsetting by create_date...")
  #filter on only the users who created during the study period
  u <- u[(u$create_date >= as.Date(start.date)) & (u$create_date <= as.Date(end.date)),]
  
  print("calculating key metrics...")
  # calculate key metrics #
  u$tlm <- as.numeric(as.character(u$lifetime_listening_minutes))
  u$activity_range <- as.numeric(u$last_listen_date - u$create_date + 1)
  u$pct_days_active <- u$total_active_days / u$activity_range
  
  print("analyzing weeks alive and weeks active...")
  ### Analyzing Weeks alive and active ###
  weeks.cols <- grepl("^Week",names(u),perl=TRUE)
  u$weeks.active <- apply(u[,weeks.cols],1,function(row){
    sum(row>0)
  })
  u$weeks_alive <- apply(u,1,function(row){
    max(ceiling(as.numeric(difftime(Sys.Date(), row['create_date'], units="weeks"))),1)
  })
  # u$weeks_alive <- apply(u,1,function(row){
  #   as.numeric(strftime(Sys.Date(), format="%Y %U")) - as.numeric(strftime(row['create_date'], format="%Y %U")) + 1
  # })
  u$pct_weeks_active <- u$weeks.active / u$weeks_alive
  #u <- u[u$create_date >= "2014-01-01",]
  
  ### Breakout u only ###
  iphone <- u[u$IPHONEratings>0,]
  android <- u[u$ANDROIDratings>0,]
  
  print("writing data to a table...")
  ### Write Relevant Data to a table ###
  Milestone <- c("Download","Launch","Register","Listen.1.min","Return","Active.3.Weeks","Active.Most.Weeks","Active.Every.Week")
  all.platforms <- c(453330,
                     371856,
                     nrow(u),
                     nrow(u[u$tlm >=1,]),
                     nrow(u[u$total_active_days>=2,]),
                     nrow(u[u$weeks.active>=3,]),
                     nrow(u[u$pct_weeks_active>=0.5 & u$weeks_alive>=3,]),
                     nrow(u[u$pct_weeks_active>=0.9 & u$weeks_alive>=3,]))
  
  ios.totals <- c(284107,
                  213537, 
                  nrow(iphone),
                  nrow(iphone[iphone$tlm >=1,]),
                  nrow(iphone[iphone$total_active_days>=2,]),
                  nrow(iphone[iphone$weeks.active>=3,]),
                  nrow(iphone[iphone$pct_weeks_active>=0.5 & iphone$weeks_alive>=3,]),
                  nrow(iphone[iphone$pct_weeks_active>=0.9 & iphone$weeks_alive>=3,])
                  )
  
  android.totals <- c(169223,
                      158319,
                      nrow(android),
                      nrow(android[android$tlm >=1,]),
                      nrow(android[android$total_active_days>=2,]),
                      nrow(android[android$weeks.active>=3,]),
                      nrow(android[android$pct_weeks_active>=0.5 & android$weeks_alive>=3,]),
                      nrow(android[android$pct_weeks_active>=0.9 & android$weeks_alive>=3,])
                      )
  
  alltime_funnel <- data.frame(Milestone,ios.totals,android.totals,all.platforms)
  return(alltime_funnel)
}

alltime <- Get_Retention_Funnel("2014-07-28",Sys.Date()-1)
last3 <- Get_Retention_Funnel(Sys.Date()-21,Sys.Date()-1)

try(write.csv(alltime,file="/home/developer/retention_funnels/retention_funnel_current.csv"))
try(write.csv(alltime,file=paste("/home/developer/retention_funnels/retention_funnel_",Sys.Date()-1,".csv",sep='')))
try(write.csv(last3,file="/home/developer/retention_funnels/retention_funnel_last3.csv"))

print("done producing funnel and writing to csv")
print(summary)
          
