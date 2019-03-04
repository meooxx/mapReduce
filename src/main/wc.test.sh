#!/bin/bash


sort -n -k2 mrtemp-test.txt | tail -10 | diff - out.test.txt > diff.out

if [ -s diff.out ]
then
echo "Failed test. Output should be as in mr-testout.txt. Your output differs as follows (from diff.out):"
  cat diff.out
else
  echo "Passed test"
fi

