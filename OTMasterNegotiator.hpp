
#ifndef OT_MASTER_NEGOTIATOR_HPP
#define OT_MASTER_NEGOTIATOR_HPP

#include <iostream>
#include <string>
#include <time.h>

#include "OTSocket.hpp"

namespace nsOneTable {

class OTMasterNegotiator {
public:
    explicit OTMasterNegotiator();
    ~OTMasterNegotiator();

    bool Negotiate(OTTCPClient & _client);
    bool Negotiate(OTTCPAccepter * _conn);
    bool IsMaster() { return bMaster_; }
    void SetMaster() { bMaster_ = true; }

private:
    bool isLeftSmall(struct timespec _left, struct timespec _right);

private:
    struct timespec     ts_;
    bool                bMaster_;

};

} // nsOneTable

#endif // OT_MASTER_NEGOTIATOR_HPP
