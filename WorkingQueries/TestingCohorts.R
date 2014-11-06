#' How to test cohorts

source('~/github/MPX/WorkingQueries/sprint_report_functions.R') # provides GetUserDailyListening
source('~/github/MPX/WorkingQueries/DifferenceOfMedians_bootstrap.R') # provides DifferenceOfMedians
source('~/github/MPX/WorkingQueries/DifferenceOfMedians_Bayes.R') # provides Probs


TestCohortListening <- function(start.date, 
                                end.date, 
                                cohort.group.1, 
                                cohort.group.2,
                                min.meaningful.effect.seconds=30,
                                use.daily=TRUE,
                                use.total=!use.daily) {
  if(use.total) {
    data.1 <- GetUserTotalListening(start.date=start.date,
                                    end.date=end.date,
                                    cohort=cohort.group.1)
    data.2 <- GetUserTotalListening(start.date=start.date,
                                    end.date=end.date,
                                    cohort=cohort.group.2)
  } else {
    data.1 <- GetUserDailyListening(start.date=start.date,
                                    end.date=end.date,
                                    cohort=cohort.group.1)
    data.2 <- GetUserDailyListening(start.date=start.date,
                                    end.date=end.date,
                                    cohort=cohort.group.2)  
  }
  #' Convert from minutes to seconds
  data.1 <- data.1 * 60
  data.2 <- data.2 * 60
  
  draws.1.beat.2 <- DifferenceOfMedians(data.1, data.2, silent=TRUE)
  
  #' Probabilities one group won by more than min.meaningful.effect.seconds
  cat("Difference of medians: ", 
      paste(cohort.group.1, collapse=""), 
      " over ", 
      paste(cohort.group.2, collapse=""), 
      "\n", sep="")
  print(Probs(draws.1.beat.2, mme=min.meaningful.effect.seconds))
  cat("Difference in medians:", round(median(draws.1.beat.2)), "seconds\n")
  
  #' Plot draws for median(group 1) > median(group 2)
  hist(draws.1.beat.2, 50,
       main=paste("Difference of medians: ", 
                  paste(cohort.group.1, collapse=""), 
                  " over ", 
                  paste(cohort.group.2, collapse="")),
       xlab=paste("median(", paste(cohort.group.1, collapse=""), 
                  ") - median(", paste(cohort.group.2, collapse=""), 
                  ")", sep=""),
       probability=TRUE)
  out <- list(draws.1.beat.2=draws.1.beat.2,
              probs.1.minus.2=Probs(draws.1.beat.2, mme=min.meaningful.effect.seconds),
              cohort.group.1=cohort.group.1,
              cohort.group.2=cohort.group.2,
              min.meaningful.effect.seconds=min.meaningful.effect.seconds)
  invisible(out)
}
#' TestCohortListening("2014-09-20", "2014-09-21", "T", "U")
#' res <- TestCohortListening("2014-07-28", "2014-08-22", c("E", "F"), c("G", "H"))
#' res <- TestCohortListening("2014-09-20", "2014-09-21", "T", "U", use.total=TRUE)

