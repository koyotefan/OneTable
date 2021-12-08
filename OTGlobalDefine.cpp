
#include <iostream>
#include <atomic>

namespace nsOneTable {
        std::atomic<bool>     gbDataSyncCompleted(false);
        std::atomic<int>     gnPushingStatus(0);

} // nsOneTable
