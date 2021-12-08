
#ifndef OT_REPLICATION_PULL_HPP
#define OT_REPLICATION_PULL_HPP

#include <poll.h>
#include <thread>
#include <atomic>
#include <time.h>

#include "OTSocket.hpp"
#include "OTMasterNegotiator.hpp"
#include "OTReplication.hpp"

namespace nsOneTable {

extern std::atomic<bool>  gbDataSyncCompleted;

struct OTReplicationMember {
    std::string pip;
    std::string sip;
    int port;
    ReplMemberType  type;

    OTReplicationMember() : port(-1) {}
    OTReplicationMember & operator=(const OTReplicationMember & _st) {
        if(this != &_st) {
            pip = _st.pip;
            sip = _st.sip;
            port = _st.port;
            type = _st.type;
        }

        return *this;
    }
};

///////////////////////////////////////////////////////////////////////////////
// OTPullActionState //////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

template <typename dataT>
class OTPullAction;

template <typename dataT>
class OTPullActionState {
public:
    explicit OTPullActionState(std::shared_ptr<OneTable<dataT>> _pTbl, 
                               std::memory_order=std::memory_order_seq_cst)
        : bChanged_(false) {
            pTbl_ = std::atomic_load(&_pTbl);
        }

    ~OTPullActionState() {}
    bool IsChanged() { return bChanged_; }
    std::shared_ptr<OTPullAction<dataT>>    GetAction();
    void SetAction(std::shared_ptr<OTPullAction<dataT>>     _action);

private:
    bool    bChanged_;

    std::shared_ptr<OneTable<dataT>>        pTbl_;
    std::shared_ptr<OTPullAction<dataT>>    pAction_;
};

template <typename dataT>
void OTPullActionState<dataT>::SetAction(std::shared_ptr<OTPullAction<dataT>> _pAction) {
    pAction_ = _pAction;
    pAction_->Init(pTbl_, this);

    bChanged_ = true;
}

template <typename dataT>
std::shared_ptr<OTPullAction<dataT>>  OTPullActionState<dataT>::GetAction() {
    bChanged_ = false;
    return pAction_;
}

///////////////////////////////////////////////////////////////////////////////
// OTPullAction ///////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

template <typename dataT>
class OTPullAction {

public:
    explicit OTPullAction() {}
    virtual ~OTPullAction() {}

    virtual void Init(std::shared_ptr<OneTable<dataT>> _pTbl,
                      OTPullActionState<dataT> * _pState,
                      std::memory_order=std::memory_order_seq_cst) = 0;
    virtual bool Resume(OTTCPClient & _client) = 0;
    virtual bool Do(OTTCPClient & _client) = 0;
    virtual size_t GetTid() = 0;
};

///////////////////////////////////////////////////////////////////////////////
// OTInitPullAction ///////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

template <typename dataT>
class OTPullingAction;

template <typename dataT>
class OTSyncPullAction;

template <typename dataT>
class OTInitPullAction : public OTPullAction <dataT> {
public:
    explicit OTInitPullAction(size_t    _recvTid)
        : recvTid_(_recvTid),
          pState_(nullptr) {
            D_LOG("OTInitPullAction::OTInitPullAction()");
        }
    virtual ~OTInitPullAction() { D_LOG("OTInitPullAction::~OTInitPullAction()"); }

    void Init(std::shared_ptr<OneTable<dataT>> _pTbl,
                OTPullActionState<dataT> * _pState,
                std::memory_order=std::memory_order_seq_cst);
    bool Resume(OTTCPClient & _client) { (void)_client; return true; }
    bool Do(OTTCPClient & _client);
    size_t GetTid() { return recvTid_; }

private:
    size_t  recvTid_;
    OTPullActionState<dataT> * pState_;

    std::shared_ptr<OneTable<dataT>>    pTbl_;

};

template <typename dataT>
void OTInitPullAction<dataT>::Init(std::shared_ptr<OneTable<dataT>> _pTbl,
                                OTPullActionState<dataT> * _pState,
                                std::memory_order) {
    pTbl_ = std::atomic_load(&_pTbl);
    pState_ = _pState;
}

template <typename dataT>
bool OTInitPullAction<dataT>::Do(OTTCPClient & _client) {

    _client.SetWait(100);

    stReplicationCmd     st;
    st.cmd = ReplicationCmd::ReqPush;
    st.recvTid = recvTid_;

    if(_client.Send((char *)&st, sizeof(st)) == false) {
        W_LOG("OTInitPullAction::Do Send() fail : ReqPush recvTid : [%zu]", st.recvTid);
        return false;
    }

    I_LOG("OTInitPullAction::Do Send() Success : ReqPush recvTid : [%zu]", st.recvTid);

    int readn = 0;
    if((readn = _client.Recv((char *)&st, sizeof(st))) <= 0) {
        W_LOG("OTInitPullAction::Do Recv fail readn : [%d]", readn);
        return false;
    }

    if(st.cmd == ReplicationCmd::OkPush) {
        I_LOG("OTInitPullAction::Do Recv() Success : OkPush sentTid : [%zu]", st.sentTid);

        std::shared_ptr<OTPullAction<dataT>>    pullingAction =
            std::shared_ptr<OTPullingAction<dataT>>(
                new OTPullingAction<dataT>(st.sentTid));

        pState_->SetAction(pullingAction);
        return true;
    }

    if(st.cmd == ReplicationCmd::MustSync) {
        I_LOG("OTInitPullAction::Do Recv() Success : MustSync");

        std::shared_ptr<OTPullAction<dataT>>    syncPullAction =
            std::shared_ptr<OTSyncPullAction<dataT>>(
                new OTSyncPullAction<dataT>());

        pState_->SetAction(syncPullAction);
        return true;
    }

    if(st.cmd == ReplicationCmd::NotReady) {
        poll(nullptr, 0, 50);
        return true;
    }

    W_LOG("OTInitPullAction::Do Unexpected Data");
    return false;
}

///////////////////////////////////////////////////////////////////////////////
// OTPullingAction ////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

template <typename dataT>
class OTPullingAction : public OTPullAction <dataT> {
public:
    explicit OTPullingAction(size_t _recvTid)
        : recvTid_(_recvTid),
          capabilityCnt_(100),
          arrData_(nullptr),
          pState_(nullptr) {
            // 상대방으로 부터 Sync 를 다 받고, 이제 실시간 데이터를 받는 상태니까,
            // Push 가 가능하다고 알리는 중 입니다.
            gbDataSyncCompleted = true;

            timer_.Init(3);
            D_LOG("OTPullingAction::OTPullingAction()");
          }

    virtual ~OTPullingAction();

    void Init(std::shared_ptr<OneTable<dataT>> _pTbl,
              OTPullActionState<dataT> * _pState,
              std::memory_order=std::memory_order_seq_cst);
    bool Resume(OTTCPClient & _client);
    bool Do(OTTCPClient & _client);
    size_t GetTid() { return recvTid_; }

private:
    bool doReqDelete(OTTCPClient & _client);

private:
    CAppTimer   timer_;

    size_t  recvTid_;

    size_t  capabilityCnt_;
    stHistory<dataT> * arrData_;

    std::shared_ptr<OneTable<dataT>> pTbl_;
    OTPullActionState<dataT> * pState_;

};

template <typename dataT>
OTPullingAction<dataT>::~OTPullingAction() {

    // D_LOG("OTPullingAction::~OTPullingAction()");

    // 한번 PullingAction 으로 들어온 적이 있다면, Sync 를 다시 안해요.
    // gbDataSyncCompleted = false;

    if(arrData_) {
        delete [] arrData_;
        arrData_ = nullptr;
    }

    pState_ = nullptr;
}

template <typename dataT>
void OTPullingAction<dataT>::Init(std::shared_ptr<OneTable<dataT>> _pTbl,
                                OTPullActionState<dataT> * _pState,
                                std::memory_order) {
    pTbl_ = std::atomic_load(&_pTbl);
    pState_ = _pState;

    if(arrData_)
        delete [] arrData_;

    arrData_ = new stHistory<dataT>[capabilityCnt_];
}

template <typename dataT>
bool OTPullingAction<dataT>::Resume(OTTCPClient & _client) {

    _client.SetWait(100);

    stReplicationCmd     st;
    st.cmd = ReplicationCmd::ReqResume;
    st.recvTid = recvTid_;

    // std::cout << "[DEBUG] OTPullingAction::Resume Try" << std::endl;

    if(_client.Send((char *)&st, sizeof(st)) == false) {
        W_LOG("OTPullingAction::Resume Send() fail : ReqResume recvTid : [%zu]", st.recvTid);
        return false;
    }

    // std::cout << "[DEBUG] OTPullingAction::Resume Send ReqResume, Tid:" << st.recvTid << std::endl;

    int readn = 0;
    if((readn = _client.Recv((char *)&st, sizeof(st))) <= 0) {
        W_LOG("OTPullingAction::Resume Recv() fail readn : [%d]", readn);
        return false;
    }

    if(st.cmd != ReplicationCmd::OkResume) {
        W_LOG("OTPullingAction::Resume Recv() Unexpected cmd [%d]", static_cast<int>(st.cmd));
        return false;
    }

    recvTid_ = st.sentTid;

    I_LOG("OTPullingAction::Resume Ok recvTid : [%zu]", recvTid_);
    pTbl_->SetReplicatedTid(recvTid_);
    return true;
}

template <typename dataT>
bool OTPullingAction<dataT>::Do(OTTCPClient & _client) {
    _client.SetWait(50);

    stReplicationCmd     st;
    int readn = 0;

    if((readn = _client.Recv((char *)&st, sizeof(st))) < 0) {
        W_LOG("OTPullingAction::Do Recv() fail readn : [%d]", readn);
        return false;
    }

    if(readn == 0)
        return true;

    if(st.cmd == ReplicationCmd::OkDelete)
        return true;

    if(st.cmd != ReplicationCmd::DoPush) {
        // 물론 아닐수도 있어요... 그런 경우 있나요?
        W_LOG("OTPullingAction::Do Unexpected cmd [%d]", static_cast<int>(st.cmd));
        return false;
    }

    // 한번에 다 받아야 해요.
    if(st.dataCnt > capabilityCnt_) {
        delete [] arrData_;
        arrData_ = new stHistory<dataT>[st.dataCnt];
        capabilityCnt_ = st.dataCnt;
    }

    // std::cout << "[DEBUG] OTPullingAction::Do() before Recv cnt:" << st.dataCnt << std::endl;

    if((readn = _client.Recv((char *)arrData_, st.dataCnt * sizeof(stHistory<dataT>))) <= 0) {
        W_LOG("OTPullingAction::Do Data Recv() fail cnt : [%zu] readn : [%d]",
            st.dataCnt, readn);
        return false;
    }

    for(size_t cnt=0; cnt < st.dataCnt; ++cnt) {

        stHistory<dataT> & stHist = arrData_[cnt];

        if((recvTid_ + 1) != stHist.tid) {
            // 깨끗하게 끊었다가 다시 붙여요.
            W_LOG("OTPullingAction::Do Invaild Tid : [%zu], expected Tid : [%zu]",
                stHist.tid, recvTid_+1);
            return false;
        }

        //std::cout << "[DEBUG] OTPullingAction::Do tid:" << stHist.tid << " id:" << stHist.d.id << std::endl;
        //std::cout << "[DEBUG] OTPullingAction::Do data:" << stHist.d.data << std::endl;
        // bool bRet = (stHist.bInsert)?pTbl_->PutByRepl(stHist.tid, stHist.d):pTbl_->DelByRepl(stHist.tid, stHist.d);

        if(stHist.bInsert) {
            if(pTbl_->PutByRepl(stHist.tid, false, stHist.d) == false) {
                pTbl_->DelByRepl(stHist.tid, false, stHist.d);
                pTbl_->PutByRepl(stHist.tid, false, stHist.d);
            }
        } else {
            pTbl_->DelByRepl(stHist.tid, false, stHist.d);
        }

        recvTid_ = stHist.tid;

        // std::cout << "[DEBUG] OTPullingAction::Recv() Put Ok lastTid:" << recvTid_ << std::endl;
    }

    if(timer_.IsTimeOut()) {
        timer_.Update();
        return doReqDelete(_client);
    }

    return true;
}

template <typename dataT>
bool OTPullingAction<dataT>::doReqDelete(OTTCPClient & _client) {

    stReplicationCmd     st;
    st.cmd = ReplicationCmd::ReqDelete;
    st.recvTid = recvTid_;

    if(_client.Send((char *)&st, sizeof(st)) == false) {
        W_LOG("OTPullingAction::doReqDelete Send() fail recvTid : [%zu]", st.recvTid);
        return false;
    }

    D_LOG("OTPullingAction::doReqDelete Send() Success recvTid : [%zu]", st.recvTid);

    return true;
}

///////////////////////////////////////////////////////////////////////////////
// OTSyncPullAction ///////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

template <typename dataT>
class OTSyncPullAction : public OTPullAction <dataT> {
public:
    explicit OTSyncPullAction()
        : bRequested_(false),
          myMaxTid_(0),
          yourMaxTid_(0),
          capabilityCnt_(100),
          arrData_(nullptr),
          pState_(nullptr) {
                D_LOG("OTSyncPullAction::OTSyncPullAction");
          }

    virtual ~OTSyncPullAction();

    void Init(std::shared_ptr<OneTable<dataT>> _pTbl,
              OTPullActionState<dataT> * _pState,
              std::memory_order=std::memory_order_seq_cst);
    bool Resume(OTTCPClient & _client) { (void)_client; return true; }
    bool Do(OTTCPClient & _client);
    size_t GetTid() { return yourMaxTid_; }

private:
    bool    doReqSync(OTTCPClient & _client);
    bool    doRecv(OTTCPClient & _client);

private:
    bool    bRequested_;

    size_t  myMaxTid_;
    size_t  yourMaxTid_;

    size_t  capabilityCnt_;
    stSyncData<dataT> * arrData_;

    std::shared_ptr<OneTable<dataT>> pTbl_;
    OTPullActionState<dataT> * pState_;

};

template <typename dataT>
OTSyncPullAction<dataT>::~OTSyncPullAction() {
    if(arrData_) {
        delete [] arrData_;
        arrData_ = nullptr;
    }

    pState_ = nullptr;
    D_LOG("OTSyncPullAction::~OTSyncPullAction");
}

template <typename dataT>
void OTSyncPullAction<dataT>::Init(std::shared_ptr<OneTable<dataT>> _pTbl,
                                    OTPullActionState<dataT> * _pState,
                                    std::memory_order) {

    pTbl_ = std::atomic_load(&_pTbl);
    pState_ = _pState;

    if(arrData_)
        delete [] arrData_;

    arrData_ = new stSyncData<dataT>[capabilityCnt_];
}

template <typename dataT>
bool OTSyncPullAction<dataT>::Do(OTTCPClient & _client) {

    if(bRequested_ == false) {
        if(doReqSync(_client) == false) {
            return false;
        }

        bRequested_ = true;
    }

    return doRecv(_client);

}

template <typename dataT>
bool OTSyncPullAction<dataT>::doReqSync(OTTCPClient & _client) {

    pTbl_->Clear();
    // pTbl_->Truncate();

    _client.SetWait(100);

    stReplicationCmd     st;
    st.cmd = ReplicationCmd::ReqSync;

    if(_client.Send((char *)&st, sizeof(st)) == false) {
        W_LOG("OTSyncPullAction::doReqSync Send() fail : ReqSync");
        return false;
    }

    int readn = 0;
    if((readn = _client.Recv((char *)&st, sizeof(st))) <= 0) {
        W_LOG("OTSyncPullAction::doReqSync Recv() fail [%d]", readn);
        return false;
    }

    if(st.cmd != ReplicationCmd::OkSync) {
        W_LOG("OTSyncPullAction::doReqSync Unexpected : [%d]", static_cast<int>(st.cmd));
        return false;
    }

    I_LOG("OTSyncPullAction::doReqSync OkSync");

    return true;
}

template <typename dataT>
bool OTSyncPullAction<dataT>::doRecv(OTTCPClient & _client) {

    _client.SetWait(50);

    stReplicationCmd     st;
    int readn = 0;
    if((readn = _client.Recv((char *)&st, sizeof(st))) < 0) {
        W_LOG("OTSyncPullAction::doRecv Recv() fail readn [%d]", readn);
        return false;
    }

    if(readn == 0)
        return true;

    // std::cout << "[DEBUG] OTSyncPullAction::doRecv Cmd readn :" << readn << std::endl;

    if(st.cmd == ReplicationCmd::EndSync) {
        myMaxTid_ = st.recvTid;
        yourMaxTid_ = st.sentTid;

        pTbl_->SetReplicatedTid(yourMaxTid_);
        pTbl_->SetTid(myMaxTid_);

        I_LOG("OTSyncPullAction::doRecv : EndSync myTid : [%zu] yourTid : [%zu]",
            myMaxTid_, yourMaxTid_);

        std::shared_ptr<OTPullAction<dataT>>     initAction =
            std::shared_ptr<OTInitPullAction<dataT>>(
                new OTInitPullAction<dataT>(yourMaxTid_));

        pState_->SetAction(initAction);
        return true;
    }

    if(st.cmd != ReplicationCmd::DoSync) {
        W_LOG("OTSyncPullAction::doRecv Unexpected data [%d]", static_cast<int>(st.cmd));
        return false;
    }

    if(st.dataCnt > capabilityCnt_) {
        delete [] arrData_;
        arrData_ = new stSyncData<dataT>[st.dataCnt];
        capabilityCnt_ = st.dataCnt;
    }

    if((readn = _client.Recv((char *)arrData_, st.dataCnt * sizeof(stSyncData<dataT>))) <= 0) {
        W_LOG("OTSyncPullAction::doRecv Data Recv fail readn [%d]", readn);
        return false;
    }

    // std::cout << "[DEBUG] OTSyncPullAction::doRecv Data after Recv :" << readn << std::endl;

    for(size_t cnt=0; cnt < st.dataCnt; ++cnt) {

        stSyncData<dataT> & stSync = arrData_[cnt];

        // Sync 는 넣을 것만 줘요..
        // 전송한 쪽이 bMyTid 가 true 면, 반대로 넣어야 죠.
        if(pTbl_->PutByRepl(stSync.tid, !stSync.bMyTid, stSync.d) == false) {
            pTbl_->DelByRepl(stSync.tid, !stSync.bMyTid, stSync.d);
            if(pTbl_->PutByRepl(stSync.tid, !stSync.bMyTid, stSync.d) == false) {
                W_LOG("OTSyncPullAction::doRecv OneTable Put fail Tid : [%zu] bMyTid : [%d]",
                    stSync.tid, !stSync.bMyTid);
                return false;
            }
        }
    }

    return true;
}

///////////////////////////////////////////////////////////////////////////////
// OTReplicationPull //////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

template <typename dataT>
class OTReplicationPull {
public:
    explicit OTReplicationPull(OTReplicationMember _member)
        : member_(_member),
          bRun_(true) {

        }

    ~OTReplicationPull() {}
    bool Run(std::shared_ptr<OneTable<dataT>>   _pTbl,
             std::memory_order=std::memory_order_seq_cst);

    void Stop() { bRun_ = false; }
    bool IsRun() { return pRunRef_.use_count() > 1; }
    bool IsMaster() { return masterNegotiator_.IsMaster(); }

private:
    void run();

private:
    OTReplicationMember     member_;

    std::shared_ptr<OneTable<dataT>>    pTbl_;
    std::shared_ptr<bool>               pRunRef_;
    std::atomic<bool>                   bRun_;

    OTMasterNegotiator  masterNegotiator_;
};

template <typename dataT>
bool OTReplicationPull<dataT>::Run(std::shared_ptr<OneTable<dataT>>   _pTbl, std::memory_order) {
    pTbl_ = std::atomic_load(&_pTbl);

    // pTbl_ 을 가지고, 상대방에게 받았던, max tid 값을 구하여 가지고 있어야 합니다.

    pRunRef_ = std::shared_ptr<bool>(new bool(true));
    std::thread thr = std::thread(&OTReplicationPull<dataT>::run, std::ref(*this));
    thr.detach();

    return true;
}


template <typename dataT>
void OTReplicationPull<dataT>::run() {

    std::shared_ptr<bool> pRun = pRunRef_;
    OTTCPClient     client;

    CAppTimer       timer;
    timer.Init(10);

    size_t  tid = pTbl_->GetReplicatedTid();
    I_LOG("OTReplicationPull::run() myTid : [%zu]", tid);

    OTPullActionState<dataT>    state(pTbl_);

    std::shared_ptr<OTPullAction<dataT>>   initAction =
        std::shared_ptr<OTInitPullAction<dataT>>(
            new OTInitPullAction<dataT>(tid));

    state.SetAction(initAction);
    std::shared_ptr<OTPullAction<dataT>>  action = state.GetAction();

    while(bRun_) {
        if(client.IsConnected() == false) {
            // time 보고, 연결할 시간이면 연결 합니다.
            if(timer.IsTimeOut() == false) {
                poll(nullptr, 0, 50);
                continue;
            }

            timer.Update();

            if(client.ConnectDualVersion(member_.pip, member_.port) == false &&
               client.ConnectDualVersion(member_.sip, member_.port) == false) {
                // Peer 가 살아 있지 않은듯 하니, 받을 일이 없겠다고 판단합니다.
                I_LOG("OTReplicationPull::run Connect failed, so ready");

                masterNegotiator_.SetMaster();

                std::shared_ptr<OTPullAction<dataT>>   pullingAction =
                    std::shared_ptr<OTPullingAction<dataT>>(
                        new OTPullingAction<dataT>(action->GetTid()));

                state.SetAction(pullingAction);
                action = state.GetAction();

                continue;
            }

            if(masterNegotiator_.Negotiate(client) == false) {
                W_LOG("OTReplicationPull::run Negotiate() fail");
                client.Close();
                continue;
            }

            I_LOG("OTReplicationPull::run I am %s", masterNegotiator_.IsMaster()?"Master":"Slave");

            if(masterNegotiator_.IsMaster()) {

                std::shared_ptr<OTPullAction<dataT>>   pullingAction =
                    std::shared_ptr<OTPullingAction<dataT>>(
                            new OTPullingAction<dataT>(action->GetTid()));

                state.SetAction(pullingAction);
                action = state.GetAction();
            }

            // initAction 이였다면, 마치 처음 생겨난 듯이 합니다.
            // pullingAction 이였다면, 상대방의 InitPush 에게 알림니다.
            // 걍 니가 쏘고 싶은 값부터 쏘라고요..
            if(action->Resume(client) == false) {
                W_LOG("OTReplicationPull::run Resume() fail");
                client.Close();
                continue;
            }

            // 아 바로 여기서... 붙게 된다면...
            // 나는 Pulling 이예요... Pulling 인데..
            // 내가 doSync 를 보낸다고 하면 말이 안되죠..
            // 상대방에게 막바로...됐고.. 보내라고 지시해야 해요..?
            //  1) 상대방이 단순히 연결만 끊어졌다가 붙었다면..상대방이 원하는 값부터 쏘라고 해야 합니다.

            /*---
            if(gbDataSyncCompleted == false ) {
                std::shared_ptr<OTPullAction<dataT>>   initAction =
                    std::shared_ptr<OTInitPullAction<dataT>>(
                        new OTInitPullAction<dataT>(action->GetTid()));

                state.SetAction(initAction);
                action = state.GetAction();
            }
            ------*/

        }

        if(action->Do(client) == false) {
            W_LOG("OTReplicationPull::run Do() fail");
            client.Close();
        }

        if(state.IsChanged())
            action = state.GetAction();
    }

    I_LOG("OTReplicationPull::run out of service");
}



}

#endif // OT_REPLICATION_PULL_HPP
