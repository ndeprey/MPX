## Plot listeners by quarter hour
plot(qh.count,xaxt="n")
ticks <- seq(0,96,4)
labs <- seq(0,24,1)
axis(1,at=ticks,labels=labs)