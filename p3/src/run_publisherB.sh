#!/bin/bash
brokerid=0
for i in {1..500}
do
# echo $i
./publisher -t 2 -b $brokerid -m "B: $i"
brokerid=$?
# echo $brokerid
done
