### Spark lag 

This is little bit challenging task for calculate dayscount from batch of log file.
sample output.

![](https://imgur.com/8z21vgJ.jpg)

usually spark calculation are row based . so that can be easily parallelized and partition.
but some context we need to window functioning for calculation.
for our context also, days count calulation depend on privious row.

 