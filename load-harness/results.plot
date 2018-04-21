set xdata time
set timefmt "%H:%M:%S"

plot "results.txt" using 1:3 with lines title "min", "" using 1:5 with lines title "avg", \
"" using 1:4 with lines title "max"

pause -1

