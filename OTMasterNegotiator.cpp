
#include "OTMasterNegotiator.hpp"

namespace nsOneTable {

OTMasterNegotiator::OTMasterNegotiator()
    : bMaster_(false) {
    clock_gettime(CLOCK_REALTIME, &ts_);
}

OTMasterNegotiator::~OTMasterNegotiator() {

}

bool OTMasterNegotiator::Negotiate(OTTCPClient & _client) {

    _client.SetWait(100);

    if(_client.Send((char *)&ts_, sizeof(ts_)) == false) {
        // std::cout << "[DEBUG] OTMasterNegotiator::Negotiate Send fail" << std::endl;
        return false;
    }

    struct timespec recvTs;
    int             readn = 0;

    if((readn = _client.Recv((char *)&recvTs, sizeof(recvTs))) <= 0) {
        // std::cout << "[DEBUG] OTMasterNegotiator::Negotiate Recv fail" << readn << std::endl;
        return false;
    }

    bMaster_ = (isLeftSmall(ts_, recvTs))?true:false;
    return true;
}

bool OTMasterNegotiator::Negotiate(OTTCPAccepter * _conn) {

    if(_conn == nullptr)
        return false;

    _conn->SetWait(500);

    struct timespec recvTs;
    int             readn = 0;

    if((readn = _conn->Recv((char *)&recvTs, sizeof(recvTs))) <= 0) {
        // std::cout << "[DEBUG] OTMasterNegotiator::Negotiate Recv fail" << readn << std::endl;
        return false;
    }

    if(_conn->Send((char *)&ts_, sizeof(ts_)) == false) {
        // std::cout << "[DEBUG] OTMasterNegotiator::Negotiate Send fail" << std::endl;
        return false;
    }

    bMaster_ = (isLeftSmall(ts_, recvTs))?true:false;
    return true;
}

bool OTMasterNegotiator::isLeftSmall(struct timespec _left, struct timespec _right) {
    if(_left.tv_sec < _right.tv_sec)
        return true;

    if(_left.tv_sec > _right.tv_sec)
        return false;

    if(_left.tv_nsec < _right.tv_nsec)
        return true;

    return false;
}

}

