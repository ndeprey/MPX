#' Analyze hiatus personalization

source('~/github/MPX/WorkingQueries/personaliztion_modest.R')

#' Get numbers for those on hiatus
cp <- CurrentPersonalization()
cp.org <- GetUserStation(cp$current.hiatus$user.id)
cp.all <- cbind(cp$current.hiatus, cp.org)
shares.pers <- table(cp.all$org_abbr) / nrow(cp.all)
share.df.pers <- data.frame(station=names(shares.pers), share=as.vector(shares.pers), count=as.vector(table(cp.all$org_abbr)))

#' Get shares for stations of current users
current.users <- GetValidUserIds(start.date=Sys.Date()-30)
current.org <- GetUserStation(current.users)
shares.current <- table(current.org$org_abbr) / nrow(current.org)
share.df.current <- data.frame(station=names(shares.current), share=as.vector(shares.current))
share.df.current$city <- sapply(share.df.current$station, function(s) {
  current.org$org_market_city[current.org$org_abbr == s][1]
})

shares <- merge(x=share.df.pers, y=share.df.current, by.x="station", by.y="station", all.y=TRUE)
shares$hiatus.by.current <- ifelse(shares$count==0, 0, shares$share.x / shares$share.y)

shares <- shares[order(shares$hiatus.by.current, decreasing=TRUE),]

hist(shares$hiatus.by.current, 50)

top.hiatus <- shares[1:20, c("station", "city")]
rownames(top.hiatus) <- NULL
bottom.hiatus <- shares[(nrow(shares)-19):nrow(shares),c("station", "city")]
rownames(bottom.hiatus) <- NULL



