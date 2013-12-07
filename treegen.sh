rm disk.data
rm disk.bitmap
rm disk.config

./makedisk disk 1024 64 1 16 64 100 10 .28
./btree_init disk 100 4 4

./btree_insert disk 100 hhhh 8888
./btree_insert disk 100 iiii 9999
./btree_insert disk 100 jjjj 1000
./btree_insert disk 100 kkkk 2000
./btree_insert disk 100 llll 3000
./btree_insert disk 100 mmmm 4000
./btree_insert disk 100 nnnn 5000
./btree_insert disk 100 aaaa 1111
./btree_insert disk 100 bbbb 2222
./btree_insert disk 100 cccc 3333
./btree_insert disk 100 dddd 4444
./btree_insert disk 100 eeee 5555
./btree_insert disk 100 ffff 6666
./btree_insert disk 100 gggg 7777
