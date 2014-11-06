users <- read.csv("Last_Listen_Date_10_03.csv")
users$LastDayActive <- as.POSIXct(users$LastDayActive)
users$FirstDayActive <- as.POSIXct(users$FirstDayActive)
users$FirstDayActive <- as.Date(users$FirstDayActive)
users$LastDayActive <- as.Date(users$LastDayActive)

users$ActiveDateRange <- as.numeric(users$LastDayActive - users$FirstDayActive + 1)
users$pct_days_active <- users$activeDays / users$ActiveDateRange

users$start_week <- cut(users$FirstDayActive, "weeks", start.on.monday = FALSE)
users$end_week <- cut(users$LastDayActive, "weeks", start.on.monday = FALSE)

library(reshape2)
rolling_attr <- dcast(users, start_week ~ end_week)