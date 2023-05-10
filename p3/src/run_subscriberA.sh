#!/bin/bash
RET=0

for i in {1..2}
do
# echo "-------------------------------------"
echo "Running subscriber A for $i th time"


RET=$(./subscriber -t 2 -i $RET | tee >(wc -l))
# RET=$?

echo "$RET index"
# RET=$RET+1
done
