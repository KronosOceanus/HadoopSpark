#!/bin/bash

echo "AAA"
wk=`date +%w`
echo "{\"wk\":$wk}"
echo "{\"wk\":$wk}" > $JOB_OUTPUT_PROP_FILE
