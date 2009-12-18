#!/bin/bash

BASEDIR=`dirname $0`

$BASEDIR/jyunit $BASEDIR/../src/jython/jyunit/run.py $@
