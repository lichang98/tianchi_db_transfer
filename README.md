# tianchi_db_transfer
Tianchi competition, transfer DB data

The master branch is deprecated because it is so slow.  But lately, I found it is the inefficiency of the C++ regex library that leads to it.

So, branch refined is a faster implementation with adjustment of data structures and with mmap. 
It can be further optimized with pmem library according to the competition, but I do not have enough time to finish it.
