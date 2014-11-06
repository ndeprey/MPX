
suppressPackageStartupMessages(library('ROCR'))

u <- read.csv("all_users_9_26.csv")

u$FirstDayActive <- as.Date(strptime(u$FirstDayActive, "%Y-%m-%d"))
u$LastDayActive <- as.Date(strptime(u$LastDayActive, "%Y-%m-%d"))

u$range_active <- 1 + as.numeric(u$LastDayActive - u$FirstDayActive)
u$pct_days_active <- u$activeDays / (1 + as.numeric(u$LastDayActive - u$FirstDayActive))

internal <- u[u$FirstDayActive < "2014-07-28",]
u <- u[u$FirstDayActive >= "2014-07-28",]

u$TotalSeconds <- as.numeric(u$TotalSeconds)

u$TLM.per.day <- u$TotalSeconds / u$activeDays / 60

u$skip_rate <- u$Skips / u$TotalRatings
u$thumbup_rate <- u$Thumbups / u$TotalRatings

u$HRNC_skip_rate <- u$HRNCskips / u$HRNCs
u$HRNC_skip_rate[is.nan(u$HRNC_skip_rate)] <- 0

u$LOCALNCskip_rate <- u$LocalNCskips / u$LocalNewscasts
u$LOCALNCskip_rate[is.nan(u$LOCALNCskip_rate)] <- 0

u$Lead_skip_rate <- u$LeadSkips / u$LeadPlays
u$Lead_skip_rate[is.nan(u$Lead_skip_rate)] <- 0

u$Break_skip_rate <- u$BreakSkips / u$BreakPlays
u$Break_skip_rate[is.nan(u$Break_skip_rate)] <- 0

u$Rated_skip_rate <- u$RatedSkips / u$RatedPlays
u$Rated_skip_rate[is.nan(u$Rated_skip_rate)] <- 0

u$Podcast_skip_rate <- u$PodcastSkips / u$TotalPodcasts
u$Podcast_skip_rate[is.nan(u$Podcast_skip_rate)] <- 0

u$HRNCs.per.day <- u$HRNCs / u$activeDays
u$LOCALNCs.per.day <- u$LocalNewscasts / u$activeDays
u$podcasts.per.day <- u$TotalPodcasts / u$activeDays
u$localstories.per.day <- u$Localstories / u$activeDays
u$got_Rated <- u$RatedPlays>= 1
u$used_iPhone <- u$IPHONEratings >= 1
u$used_Android <- u$ANDROIDratings >= 1
u$used_webapp <- u$WEBAPPratings >= 1




u$lapsed_08_24 <- u$LastDayActive <= "2014-08-24"
u$lapsed_08_31 <- u$LastDayActive <= "2014-08-31"
u$lapsed_09_07 <- u$LastDayActive <= "2014-09-07"

# lm3 <- lm(data=u, lapsed_08_31 ~ HRNCs.per.day + LOCALNCs.per.day + 
#             podcasts.per.day + localstories.per.day + skip_rate
#           + thumbup_rate + TLM.per.day + Searches + got_Rated + used_iPhone + used_Android
#           + used_webapp + HRNC_skip_rate + LOCALNCskip_rate + Lead_skip_rate
#           + Break_skip_rate + Rated_skip_rate + Podcast_skip_rate)

lapse <- function(df, lapse_date){

  # lapse_var <- paste("df$","lapse_date",sep="")
  df$lapse <- df$LastDayActive <= lapse_date
  df <- df[df$FirstDayActive <= lapse_date,]
  
  
  model <- glm(formula = lapse ~ HRNCs.per.day + LOCALNCs.per.day + 
      podcasts.per.day + localstories.per.day + skip_rate + thumbup_rate + 
      TLM.per.day + Searches + got_Rated + used_iPhone + used_Android + 
      used_webapp + HRNC_skip_rate + LOCALNCskip_rate + Lead_skip_rate + 
      Break_skip_rate + Rated_skip_rate + Podcast_skip_rate, data = df)

  probs <- predict(model, df, type="response")
  head(data.frame(actual=df$lapse, prediction=probs))

  # suppressPackageStartupMessages(library('ROCR'))

  pred <- prediction(predictions=probs, labels = df$lapse)
  acc <- performance(pred, measure='acc')
  prec <- performance(pred, measure='prec')
  rec <- performance(pred, measure='rec')

  plot(acc)
  accs <- data.frame(cutoff = unlist(acc@x.values), accuracy = unlist(acc@y.values))
  optimal.cutoff <- accs$cutoff[accs$accuracy == max(accs$accuracy)][1]
  best.accuracy <- max(accs$accuracy)

  return.objects <- list("acc"=acc, "pred" = pred, "prec" = prec, "rec" = rec, "optimal.cutoff"=optimal.cutoff, "best.accuracy" = best.accuracy)
  
  return(return.objects)
  
}
