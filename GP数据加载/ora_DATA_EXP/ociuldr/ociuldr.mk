export OH=$ORACLE_HOME
#cc -D_LARGE_FILES \
cc -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64 -Dlint \
-I$OH/rdbms/demo -I$OH/rdbms/public \
-L${OH}/lib32 -lm -lclntsh -o sqluldr.bin \
ociuldr.c
