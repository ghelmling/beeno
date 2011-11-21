#!/bin/bash

BASEDIR=`dirname $0`

$BASEDIR/jyunit $BASEDIR/../src/main/jython/jyunit/run.py $@
