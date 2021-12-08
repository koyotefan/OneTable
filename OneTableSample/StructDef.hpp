
#include <iostream>
#include <time.h>

struct stPacket {
    unsigned int    id;
    time_t          ctime;
    char            data[1024];
};
