
#ifndef OT_REPLICATION_HPP
#define OT_REPLICATION_HPP

#include <functional>
#include <atomic>

#include "OneTable.hpp"
#include "OTReplicationDef.hpp"
#include "OTReplicationPull.hpp"
#include "OTReplicationPush.hpp"

namespace nsOneTable {

extern std::atomic<bool>     gbDataSyncCompleted;
extern std::atomic<int>      gnPushingStatus;

///////////////////////////////////////////////////////////////////////////////
// OTReplication //////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

template <typename dataT>
class OTReplication {
public:
    explicit OTReplication(std::shared_ptr<OneTable<dataT>>  _pTbl)
        :pTbl_(_pTbl),
        serverPort_(-1),
        maxWaitMilliSec_(1000),
        bReady_(false),
        bEnabledToGetCommand_(true) {
            gbDataSyncCompleted = false;
            gnPushingStatus = 0;
    }
    ~OTReplication();

    void SetPeer(std::string _pip,
                 std::string _sip,
                 int _port,
                 ReplMemberType _type);
    void SetServerPort(int _port) { serverPort_ = _port; }
    bool Init();
    bool IsReady() { return gbDataSyncCompleted; }
    bool IsMaster() { return pTPull_->IsMaster(); }
    bool IsPushing() { return gnPushingStatus > 0; }
    void SlaveIsDisabledToGetCommand() { bEnabledToGetCommand_ = false; }
    void SlaveIsEnabledToGetCommand() { bEnabledToGetCommand_ = true; }
    bool AllowedGetCommand();
    size_t GetPushMapSize() { return (pTPush_.use_count() == 0)?0:pTPush_->GetMapSize(); }
    size_t GetPushMapCapacity() { return (pTPush_.use_count() == 0)?0:pTPush_->GetCapacity(); }

    void PutHist(size_t _tid, bool _bInsert, const dataT & _d);

private:
    std::shared_ptr<OneTable<dataT>>    pTbl_;

    int  serverPort_;
    int  maxWaitMilliSec_;
    bool bReady_;
    bool bEnabledToGetCommand_;

    std::shared_ptr<OTReplicationPull<dataT>> pTPull_;
    std::shared_ptr<OTReplicationPush<dataT>> pTPush_;

};

template <typename dataT>
OTReplication<dataT>::~OTReplication() {
    bReady_ = false;

    pTPush_->Stop();
    pTPull_->Stop();

    int cnt = 0;
    for(cnt=0; pTPush_->IsRun() && cnt < 10; ++cnt) {
        poll(nullptr, 0, maxWaitMilliSec_ / 10);
    }

    I_LOG("OTReplication::~OTReplication push wait cnt [%d]", cnt);

    for(cnt=0; pTPull_->IsRun() && cnt < 10; ++cnt) {
        poll(nullptr, 0, maxWaitMilliSec_ / 10);
    }

    I_LOG("OTReplication::~OTReplication pull wait cnt [%d]", cnt);
}

template <typename dataT>
void OTReplication<dataT>::SetPeer(std::string _pip,
                              std::string _sip,
                              int _port,
                              ReplMemberType _type) {

    OTReplicationMember  member;
    member.pip = _pip;
    member.sip = _sip;
    member.port = _port;
    member.type = _type;

    pTPull_ =
      std::shared_ptr<OTReplicationPull<dataT>>(
          new OTReplicationPull<dataT>(member));

    if(serverPort_ < 0)
      serverPort_ = _port;
}

template <typename dataT>
bool OTReplication<dataT>::Init() {

    // std::cout << "[DEBUG] OTReplication::Init()" << std::endl;

    if(pTPull_.use_count() == 0) {

        W_LOG("OTReplication::Init SetPeer() is not called");
        return false;
    }

    pTPush_ =
      std::shared_ptr<OTReplicationPush<dataT>>(
          new OTReplicationPush<dataT>(serverPort_));

    if(pTPush_->Run(pTbl_) == false) {
        W_LOG("OTReplication::Init Push Run() fail");
        return false;
    }

    // Client Thread 를 띄웁니다.
    // - Peer 에게 마지막으로 받았던, 데이터의 tid 로 요청합니다.
    if(pTPull_->Run(pTbl_) == false) {
        W_LOG("OTReplication::Init Pull Run() fail");
        return false;
    }

    pTbl_->SetReplicationRawPtr(this);

    return true;

}

template <typename dataT>
void OTReplication<dataT>::PutHist(size_t _tid, bool _bInsert, const dataT & _d) {

    if(pTPush_.use_count() > 0)
        pTPush_->PutHist(_tid, stHistory<dataT>(_tid, _bInsert, _d));
}

template <typename dataT>
bool OTReplication<dataT>::AllowedGetCommand() {

    if(bEnabledToGetCommand_)
        return true;

    if(pTPull_->IsMaster())
        return true;

    return false;
}


} // nsOneTable


#endif // OT_REPLICATION_HPP
