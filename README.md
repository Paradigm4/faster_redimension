# faster_redimension
This is a use-as-is prototype of a faster code path for SciDB's `redimension` operator. The syntax is similar to regular `redimension` and is as follows:
```
faster_redimension( INPUT, TARGET)
```
Where `INPUT` is a SciDB array expression and `TARGET` is a schema expression or the name of an array whose schema is used. For example:
```
$ iquery -aq "faster_redimension(build(<a:int64> [x=0:9,10,0], x%5), <x:int64> [a=0:*,5,0, b=0:1,2,0])"
{a,b} x
{0,0} 0
{0,1} 5
{1,0} 1
{1,1} 6
{2,0} 2
{2,1} 7
{3,0} 3
{3,1} 8
{4,0} 4
{4,1} 9
```

# Performance
Faster performance is achieved with a number of factors:

1. reduced usage of the `Value` class for mid-query results
2. different algorithm for merging partially-filled chunks from different instances, particularly advantageous when the array has many attributes
3. when synthetics are used, a second whole-array sort is avoided

Depending on the input and output array shapes, this operator may be over 6x faster than regular `redimension`. The improvement will vary on a case-by-case basis. 

# Restrictions
`faster_redimension` does not support auto-chunking, aggregates, overlaps and always errors out on cell collisions - does not support the `, false` flag that `redimension` has.

# Installation
Use https://github.com/paradigm4/dev_tools and remember to check out the branch that matches your SciDB version.

# Future
P4 Dev is investigating incorporating these changes into regular `redimension` in future releases. This prototype will remain available in the meantime.
