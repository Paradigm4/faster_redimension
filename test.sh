#!/bin/bash

MYDIR=`dirname $0`
pushd $MYDIR > /dev/null
MYDIR=`pwd`
OUTFILE=$MYDIR/test.out
EXPFILE=$MYDIR/test.expected

rm -rf $OUTFILE > /dev/null 2>&1

iquery -anq "remove(foo)" > /dev/null 2>&1
iquery -anq "store(build(<a:double,b:string,c:int64,x:int64>[i=1:10,10,0], '[(1.1,a,0,0),(2.2,b,1,null),(3.3,c,null,2),(4.4,d,null,null),(5.5,f,4,3),(6.6,g,4,4),(7.7,h,4,5),(8.8,null,4,6),(9.9,i,0,7),(10.1,k,0,8)]', true), foo)" > /dev/null 2>&1

iquery -aq "faster_redimension(foo, <a:double>[c=0:*,1,0,x=0:*,1,0])" >> $OUTFILE 2>&1
iquery -aq "faster_redimension(foo, <a:double>[c=0:*,1,0,x=0:*,2,0])" >> $OUTFILE 2>&1
iquery -aq "faster_redimension(foo, <a:double, b:string>[c=0:*,3,0,x=0:*,2,0])" >> $OUTFILE 2>&1
iquery -aq "faster_redimension(foo, <a:double, b:string>[c=0:*,10,0,x=0:*,10,0])" >> $OUTFILE 2>&1
iquery -aq "faster_redimension(foo, <b:string>[x=0:*,10,0])" >> $OUTFILE 2>&1
iquery -aq "faster_redimension(foo, <b:string>[x=0:*,4,0])" >> $OUTFILE 2>&1

diff $OUTFILE $EXPFILE

