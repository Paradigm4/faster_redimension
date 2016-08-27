#!/bin/bash

REGENERATE=0

if [ $REGENERATE -ne 0 ] ; then
iquery -anq "remove(foo_big)" > /dev/null 2>&1
iquery -anq "remove(foo_big_randomized)" > /dev/null 2>&1

iquery -anq "store(
 apply(
   build(<val1:double>[i=0:39999999,100000,0], double(random())),
   val2, double(random()),
   val3, double(random()),
   val4, double(random()),
   val5, double(random()),
   val6, double(random()),
   val7, double(random()),
   val8, double(random()),
   val9, double(random()),
   val10, double(random()),
   val11, double(random()),
   val12, double(random()),
   val13, double(random()),
   val14, double(random()),
   val15, double(random()),
   val16, double(random()),
   val17, double(random()),
   val18, double(random()),
   val19, double(random()),
   val20, double(random()),
   y, i%10000 * 10,
   x, i/10000 * 10
  ),
  foo_big
 )"

 iquery -anq "store(sort(apply(foo_big, r, random()), r, 100000), foo_big_randomized)"
fi

echo
echo "1 attr"
time iquery -anq "consume(redimension(foo_big, <val1:double>[x=0:*,10000,0, y=0:*,10000,0]))"
time iquery -anq "consume(faster_redimension(foo_big, <val1:double>[x=0:*,10000,0, y=0:*,10000,0]))"
echo
echo "5 attrs"
time iquery -anq "consume(redimension(foo_big, 
 <val1:double,val2:double,val3:double,val4:double,val5:double>
 [x=0:*,10000,0, y=0:*,10000,0]))"
time iquery -anq "consume(faster_redimension(foo_big, 
 <val1:double,val2:double,val3:double,val4:double,val5:double>
 [x=0:*,10000,0, y=0:*,10000,0]))"
echo
echo "10 attrs"
time iquery -anq "consume(redimension(foo_big, 
 <val1:double,val2:double,val3:double,val4:double,val5:double,
  val6:double,val7:double,val8:double,val9:double,val10:double>
 [x=0:*,10000,0, y=0:*,10000,0]))"
time iquery -anq "consume(faster_redimension(foo_big, 
 <val1:double,val2:double,val3:double,val4:double,val5:double,
  val6:double,val7:double,val8:double,val9:double,val10:double>
 [x=0:*,10000,0, y=0:*,10000,0]))"
echo
echo "20 attrs"
time iquery -anq "consume(redimension(foo_big, 
 <val1:double,val2:double,val3:double,val4:double,val5:double,
  val6:double,val7:double,val8:double,val9:double,val10:double,
  val11:double,val12:double,val13:double,val14:double,val15:double,
  val16:double,val17:double,val18:double,val19:double,val20:double>
 [x=0:*,10000,0, y=0:*,10000,0]))"
time iquery -anq "consume(faster_redimension(foo_big, 
 <val1:double,val2:double,val3:double,val4:double,val5:double,
  val6:double,val7:double,val8:double,val9:double,val10:double,
  val11:double,val12:double,val13:double,val14:double,val15:double,
  val16:double,val17:double,val18:double,val19:double,val20:double>
 [x=0:*,10000,0, y=0:*,10000,0]))"

echo
echo "randomized 1 attr"
time iquery -anq "consume(redimension(foo_big_randomized, <val1:double>[x=0:*,10000,0, y=0:*,10000,0]))"
time iquery -anq "consume(faster_redimension(foo_big_randomized, <val1:double>[x=0:*,10000,0, y=0:*,10000,0]))"
echo
echo "randomized 5 attrs"
time iquery -anq "consume(redimension(foo_big_randomized, 
 <val1:double,val2:double,val3:double,val4:double,val5:double>
 [x=0:*,10000,0, y=0:*,10000,0]))"
time iquery -anq "consume(faster_redimension(foo_big_randomized, 
 <val1:double,val2:double,val3:double,val4:double,val5:double>
 [x=0:*,10000,0, y=0:*,10000,0]))"
echo
echo "randomized 10 attrs"
time iquery -anq "consume(redimension(foo_big_randomized, 
 <val1:double,val2:double,val3:double,val4:double,val5:double,
  val6:double,val7:double,val8:double,val9:double,val10:double>
 [x=0:*,10000,0, y=0:*,10000,0]))"
time iquery -anq "consume(faster_redimension(foo_big_randomized, 
 <val1:double,val2:double,val3:double,val4:double,val5:double,
  val6:double,val7:double,val8:double,val9:double,val10:double>
 [x=0:*,10000,0, y=0:*,10000,0]))"
echo
echo "randomized 20 attrs"
time iquery -anq "consume(redimension(foo_big_randomized, 
 <val1:double,val2:double,val3:double,val4:double,val5:double,
  val6:double,val7:double,val8:double,val9:double,val10:double,
  val11:double,val12:double,val13:double,val14:double,val15:double,
  val16:double,val17:double,val18:double,val19:double,val20:double>
 [x=0:*,10000,0, y=0:*,10000,0]))"
time iquery -anq "consume(faster_redimension(foo_big_randomized, 
 <val1:double,val2:double,val3:double,val4:double,val5:double,
  val6:double,val7:double,val8:double,val9:double,val10:double,
  val11:double,val12:double,val13:double,val14:double,val15:double,
  val16:double,val17:double,val18:double,val19:double,val20:double>
 [x=0:*,10000,0, y=0:*,10000,0]))"

