set xdata time
set timefmt "%H:%M:%S"


plot "vmstat.dat" using 1:2 with lines title "free", "" using 1:3 with lines title "cached", \
"" using 1:4 title "blocks in" axes x1y2, "" using 1:5 title "blocks out" axes x1y2

pause -1

