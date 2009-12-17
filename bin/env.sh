#!/bin/sh

# get the full path to the jython script
prgdir=$(
prg=$0
case $prg in
(*/*) ;;
(*) [ -e "$prg" ] || prg=$(command -v -- "$0")
esac
prgbase=$(dirname -- "$prg")
cd -P -- "$prgbase" > /dev/null && pwd -P
)

basedir=$( cd -P -- "$prgdir/.." > /dev/null && pwd -P )

for f in `ls $basedir/lib/*.jar`;
do
	CP=$CP:$f;
done
CP=$CP:$basedir/conf

export CP
