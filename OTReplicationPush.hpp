
#ifndef OT_REPLICATION_PUSH_HPP
#define OT_REPLICATION_PUSH_HPP

#include <poll.h>
#include <thread>
#include <mutex>
#include <atomic>

#include "OTReplication.hpp"
#include "OTSocket.hpp"

namespace nsOneTable {

extern std::atomic<bool>  gbDataSyncCompleted;
extern std::atomic<int>   gnPushingStatus;

///////////////////////////////////////////////////////////////////////////////
// OTPushMap //////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

template <typename dataT>
class OTPushMap {
public:
    explicit OTPushMap(int _capacity)
        : capacity_(_capacity),
          bDelete_(true) {

        }

    ~OTPushMap() {}
    void Clear();

    bool Put(size_t _id, const stHistory<dataT> && _d);
    bool Get(size_t _id, stHistory<dataT> & _d);
    bool GetGT(size_t _id, stHistory<dataT> & _d);
    bool GetMinium(size_t & _id);
    size_t GetSize();
    size_t GetCapacity() { return capacity_; }
    bool IsValid(size_t _id);
    size_t Del(size_t _id);
    size_t DelLT(size_t _id);
    void ForbidToDelete() { bDelete_ = false; }
    void AllowToDelete() { bDelete_ = true; }

private:
    size_t capacity_;
    std::atomic<bool>   bDelete_;

    std::mutex      mutex_;
    std::map<size_t, stHistory<dataT>>   m_;
};

template <typename dataT>
void OTPushMap<dataT>::Clear() {
    std::lock_guard<std::mutex>   guard(mutex_);

    m_.clear();
}

template <typename dataT>
bool OTPushMap<dataT>::Put(size_t _tid, const stHistory<dataT> && _d) {
    std::lock_guard<std::mutex>   guard(mutex_);

    if(m_.size() >= capacity_ && bDelete_)
        m_.erase(m_.begin());

    m_.emplace(_tid, _d);
    // std::cout << "[DEBUG] OTPushMap::Put map size:" << m_.size() << std::endl;
    // std::cout << "[DEBUG] OTPushMap::Put tid:" << _tid << std::endl;

    return true;
}

template <typename dataT>
bool OTPushMap<dataT>::Get(size_t _tid, stHistory<dataT> & _d) {
    std::lock_guard<std::mutex>   guard(mutex_);

    auto iter = m_.find(_tid);

    if(iter == m_.end())
        return false;

    _d = iter->second;
    return true;
}

template <typename dataT>
bool OTPushMap<dataT>::GetGT(size_t _tid, stHistory<dataT> & _d) {
    std::lock_guard<std::mutex>   guard(mutex_);

    auto iter = m_.upper_bound(_tid);

    if(iter == m_.end())
        return false;

    _d = iter->second;
    return true;
}

template <typename dataT>
size_t OTPushMap<dataT>::GetSize() {
    std::lock_guard<std::mutex>   guard(mutex_);
    return m_.size();
}

template <typename dataT>
bool OTPushMap<dataT>::GetMinium(size_t & _id) {
    std::lock_guard<std::mutex>   guard(mutex_);

    if(m_.empty())
        return false;

    auto iter = m_.begin();
    _id = iter->first;
    return true;
}

template <typename dataT>
bool OTPushMap<dataT>::IsValid(size_t _tid) {
    std::lock_guard<std::mutex>   guard(mutex_);

    if(m_.empty())
        return false;

    auto iter = m_.begin();
    if(iter->first <= _tid)
        return true;

    return false;
}

template <typename dataT>
size_t OTPushMap<dataT>::Del(size_t _tid) {
    std::lock_guard<std::mutex>   guard(mutex_);
    return m_.erase(_tid);
}

template <typename dataT>
size_t OTPushMap<dataT>::DelLT(size_t _tid) {
    std::lock_guard<std::mutex>   guard(mutex_);

    size_t  beforeSize = m_.size();
    auto iBegin = m_.begin();
    auto iIt = m_.upper_bound(_tid);
    m_.erase(iBegin, iIt);

    return beforeSize - m_.size();
}

///////////////////////////////////////////////////////////////////////////////
// OTPushActionState ////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

template <typename dataT>
class OTPushAction;

template <typename dataT>
class OTPushActionState {

public:
    explicit OTPushActionState(std::shared_ptr<OneTable<dataT>> _pTbl,
                               std::shared_ptr<OTPushMap<dataT>> _pMap,
                               OTTCPAccepter * _conn,
                               std::memory_order=std::memory_order_seq_cst) :
        pMap_(_pMap),
        conn_(_conn),
        bChanged_(false) {
            pTbl_ = std::atomic_load(&_pTbl);
        }
     ~OTPushActionState() {}
     bool IsChanged() { return bChanged_; }
     std::shared_ptr<OTPushAction<dataT>> GetAction();
     void SetAction(std::shared_ptr<OTPushAction<dataT>> _action);

private:
    std::shared_ptr<OneTable<dataT>>    pTbl_;
    std::shared_ptr<OTPushMap<dataT>>   pMap_;

    OTTCPAccepter * conn_;
    bool   bChanged_;

    std::shared_ptr<OTPushAction<dataT>>  pAction_;
};

template <typename dataT>
void OTPushActionState<dataT>::SetAction(std::shared_ptr<OTPushAction<dataT>> _pAction) {
    pAction_ = _pAction;
    pAction_->Init(pTbl_, pMap_, conn_, this);

    bChanged_ = true;
}

template <typename dataT>
std::shared_ptr<OTPushAction<dataT>>  OTPushActionState<dataT>::GetAction() {
    bChanged_ = false;
    return pAction_;
}

///////////////////////////////////////////////////////////////////////////////
// OTPushAction ///////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

template <typename dataT>
class OTPushAction {

public:
    explicit OTPushAction() {}
    virtual ~OTPushAction() {}

    virtual void Init(std::shared_ptr<OneTable<dataT>> _pTbl,
                      std::shared_ptr<OTPushMap<dataT>> _pMap,
                      OTTCPAccepter * _conn,
                      OTPushActionState<dataT> * _pState,
                      std::memory_order=std::memory_order_seq_cst) = 0;

    virtual bool Recv() = 0;
    virtual bool Do() = 0;
    virtual bool Send() = 0;
    virtual size_t SendAll() = 0;
};

///////////////////////////////////////////////////////////////////////////////
// OTInitPushAction ///////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

template <typename dataT>
class OTPushingAction;

template <typename dataT>
class OTSyncPushAction;

template <typename dataT>
class OTInitPushAction : public OTPushAction<dataT> {
public:
    explicit OTInitPushAction()
        : conn_(nullptr),
          pState_(nullptr) {
            D_LOG("OTInitPushAction::OTInitPushAction()");
        }
    virtual ~OTInitPushAction();

    void Init(std::shared_ptr<OneTable<dataT>> _pTbl,
              std::shared_ptr<OTPushMap<dataT>> _pMap,
              OTTCPAccepter * _conn,
              OTPushActionState<dataT> * _pState,
              std::memory_order=std::memory_order_seq_cst);

    bool Recv();
    bool Do() { return true; }
    bool Send() { return true; }
    size_t SendAll() { return 0; }

private:
    bool doReqPush(size_t _tid);
    bool doReqSync();
    bool doResume(size_t _tid);

private:
    std::shared_ptr<OneTable<dataT>>    pTbl_;
    std::shared_ptr<OTPushMap<dataT>>   pMap_;
    OTTCPAccepter * conn_;
    OTPushActionState<dataT> * pState_;
};

template <typename dataT>
OTInitPushAction<dataT>::~OTInitPushAction() {

    D_LOG("OTInitPushAction::~OTInitPushAction()");
    conn_ = nullptr;
    pState_ = nullptr;
}

template <typename dataT>
void OTInitPushAction<dataT>::Init(std::shared_ptr<OneTable<dataT>> _pTbl,
                              std::shared_ptr<OTPushMap<dataT>> _pMap,
                              OTTCPAccepter * _conn,
                              OTPushActionState<dataT> * _pState,
                              std::memory_order)
{
    pTbl_ = std::atomic_load(&_pTbl);
    pMap_ = _pMap;
    conn_ = _conn;
    pState_ = _pState;
}


template <typename dataT>
bool OTInitPushAction<dataT>::Recv() {

    conn_->SetWait(50);

    stReplicationCmd   st;
    int                readn;

    if((readn = conn_->Recv((char *)&st, sizeof(st))) < 0) {
        W_LOG("OTInitPushAction::Recv() Recv fail : [%d]", readn);
        return false;
    }

    if(readn == 0)
        return true;

    if(st.cmd == ReplicationCmd::ReqPush)
        return doReqPush(st.recvTid);

    if(st.cmd == ReplicationCmd::ReqSync)
        return doReqSync();

    if(st.cmd == ReplicationCmd::ReqResume)
        return doResume(st.recvTid);

    // W_LOG("OTInitPushAction::Recv Unexpected cmd [%zu]", static_cast<size_t>(st.cmd));
    return false;
}

// Peer 가 나는 tid 까지 받았으니까, Sync 필요 없고.. 그 값 이후로 부터 나에게 주면 돼..
// 나는 최선을 다해서 너가 요청한 tid 부터 줄테지만, 만약에 없다면, 내가 새로 정해줄테니까..
// 그 tid 부터 나에게 요청하렴..

template <typename dataT>
bool OTInitPushAction<dataT>::doResume(size_t _recvTid) {

    // D_LOG("OTInitPushAction::doResume recvTid: [%zu]", _recvTid);
    stReplicationCmd    st;

    st.cmd = ReplicationCmd::OkResume;

    pMap_->ForbidToDelete();
    if(pMap_->IsValid(_recvTid + 1) == false) {
        // MAP 에는 없으니까, 내가 알고 있는 것 부터 순차적으로 보낼께..
        if(pMap_->GetMinium(st.sentTid) == false) {
            st.sentTid = pTbl_->GetTid();
        }
    }
    else
        st.sentTid = _recvTid;

    if(conn_->Send((char *)&st, sizeof(st)) == false) {
        pMap_->AllowToDelete();
        W_LOG("OTInitPushAction::doResume Send() fail : OkResume ") ;
        return false;
    }

    I_LOG("OTInitPushAction::doResume Send() Success : OkResume sentTid:[%zu]",
        st.sentTid) ;

    std::shared_ptr<OTPushAction<dataT>>  pushingAction =
        std::shared_ptr<OTPushingAction<dataT>>(
            new OTPushingAction<dataT>(st.sentTid));

    pState_->SetAction(pushingAction);

    return true;
}

template <typename dataT>
bool OTInitPushAction<dataT>::doReqPush(size_t _recvTid) {

    // D_LOG("OTInitPushAction::doReqPush recvTid:[%zu]", _recvTid);
    stReplicationCmd    st;

    if(gbDataSyncCompleted == false) {
        st.cmd = ReplicationCmd::NotReady;

        if(conn_->Send((char *)&st, sizeof(st)) == false) {
            W_LOG("OTInitPushAction::doReqPush Send() fail : NotReady");
            return false;
        }

        I_LOG("OTInitPushAction::doReqPush Send() Success : NotReady");
        return true;
    }

    pMap_->ForbidToDelete();

    size_t myTid = pTbl_->GetTid();
    if(pMap_->IsValid(_recvTid + 1) == false && _recvTid != myTid) {

        pMap_->AllowToDelete();

        st.cmd = ReplicationCmd::MustSync;

        if(conn_->Send((char *)&st, sizeof(st)) == false) {
            W_LOG("OTInitPushAction::doReqPush Send() fail : MustSync");
            return false;
        }

        I_LOG("OTInitPushAction::doReqPush Send() Success: MustSync recv Tid [%zu] my Tid [%zu]",
            _recvTid, myTid);
        return true;
    }

    st.cmd = ReplicationCmd::OkPush;
    st.sentTid = _recvTid;

    if(conn_->Send((char *)&st, sizeof(st)) == false) {
        W_LOG("OTInitPushAction::doReqPush Send() fail : OkPush sentTid:[%zu]", st.sentTid);
        return false;
    }

    I_LOG("OTInitPushAction::doReqPush Send() Success : OkPush sentTid:[%zu]", st.sentTid);

    std::shared_ptr<OTPushAction<dataT>>  pushingAction =
        std::shared_ptr<OTPushingAction<dataT>>(
            new OTPushingAction<dataT>(_recvTid));

    pState_->SetAction(pushingAction);
    return true;
}

template <typename dataT>
bool OTInitPushAction<dataT>::doReqSync() {

    // D_LOG("OTInitPushAction::doReqSync");

    stReplicationCmd    st;

    if(gbDataSyncCompleted == false) {
        st.cmd = ReplicationCmd::NotReady;

        if(conn_->Send((char *)&st, sizeof(st)) == false) {

            W_LOG("OTInitPushAction::doReqSync Send() fail : NotReady");
            return false;
        }

        I_LOG("OTInitPushAction::doReqSync Send() Success : NotReady");
        return true;
    }

    st.cmd = ReplicationCmd::OkSync;

    if(conn_->Send((char *)&st, sizeof(st)) == false) {
        W_LOG("OTInitPushAction::doReqSync Send() fail : OkSync");
        return false;
    }

    I_LOG("OTInitPushAction::doReqSync Send() Success: OkSync");

    std::shared_ptr<OTPushAction<dataT>>  syncAction =
        std::shared_ptr<OTPushAction<dataT>>(
            new OTSyncPushAction<dataT>());

    pState_->SetAction(syncAction);
    return true;
}

///////////////////////////////////////////////////////////////////////////////
// OTPushingAction ////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

template <typename dataT>
class OTPushingAction : public OTPushAction<dataT> {
public:
    explicit OTPushingAction(size_t _tid, int _maxCnt=100)
        : lastSentTid_(_tid),
          maxCnt_(_maxCnt),
          cnt_(0),
          arrData_(nullptr),
          conn_(nullptr),
          pState_(nullptr) {
            D_LOG("OTPushingAction::OTPushingAction() lastSentTid [%zu]", lastSentTid_);
            ++gnPushingStatus;
          }
    virtual ~OTPushingAction();

    void Init(std::shared_ptr<OneTable<dataT>> _pTbl,
              std::shared_ptr<OTPushMap<dataT>> _pMap,
              OTTCPAccepter * _conn,
              OTPushActionState<dataT> * _pState,
              std::memory_order=std::memory_order_seq_cst);

    bool Recv();
    bool Do();
    bool Send();
    size_t SendAll();

private:
    bool doDelete(size_t _tid);
    bool doRespPush(size_t _tid);

private:
    size_t lastSentTid_;

    size_t maxCnt_;
    size_t cnt_;
    stHistory<dataT> * arrData_;

    std::shared_ptr<OneTable<dataT>>    pTbl_;
    std::shared_ptr<OTPushMap<dataT>>   pMap_;
    OTTCPAccepter * conn_;
    OTPushActionState<dataT> * pState_;
};

template <typename dataT>
OTPushingAction<dataT>::~OTPushingAction() {
    D_LOG("OTPushingAction::~OTPushingAction()");

    // 오 마이갓이죠.. 왜냐하면.. 연결이 많다면.. 문제가 되는거죠..
    --gnPushingStatus;
    pMap_->AllowToDelete();

    if(arrData_) {
        delete [] arrData_;
        arrData_ = nullptr;
    }

    cnt_ = 0;
    conn_ = nullptr;
    pState_ = nullptr;
}

template <typename dataT>
void OTPushingAction<dataT>::Init(std::shared_ptr<OneTable<dataT>> _pTbl,
                              std::shared_ptr<OTPushMap<dataT>> _pMap,
                              OTTCPAccepter * _conn,
                              OTPushActionState<dataT> * _pState,
                              std::memory_order)
{
    pTbl_ = std::atomic_load(&_pTbl);
    pMap_ = _pMap;
    conn_ = _conn;
    pState_ = _pState;

    if(arrData_)
        delete [] arrData_;

    arrData_ = new stHistory<dataT>[maxCnt_];
}

template <typename dataT>
bool OTPushingAction<dataT>::Recv() {
    conn_->SetWait(0);

    int     readn;
    stReplicationCmd    st;

    if((readn = conn_->Recv((char *)&st, sizeof(st))) < 0) {
        W_LOG("OTPushingAction::Recv Recv() fail");
        return false;
    }

    if(readn == 0)
        return true;

    if(st.cmd == ReplicationCmd::ReqDelete)
        return doDelete(st.recvTid);

    if(st.cmd != ReplicationCmd::ReqPush) {
        // W_LOG("OTPushingAction::Recv Unexpected cmd : [%zu]", static_cast<size_t>(st.cmd));
        return false;
    }

    return doRespPush(st.recvTid);
}

template <typename dataT>
bool OTPushingAction<dataT>::doDelete(size_t _tid) {

    size_t cnt = pMap_->DelLT(_tid);
    D_LOG("OTPushingAction::doDelete tid : [%zu] deleted cnt : [%zu]", _tid, cnt);

    stReplicationCmd    st;

    st.cmd = ReplicationCmd::OkDelete;
    if(conn_->Send((char *)&st, sizeof(st)) == false) {
        W_LOG("OTPushingAction::doDelete Send() fail : OkDelete");
        return false;
    }

    // D_LOG("OTPushingAction::doDelete Send() Success : OkDelete");

    return true;
}

template <typename dataT>
bool OTPushingAction<dataT>::doRespPush(size_t _recvTid) {

    // AllowToDelete() 는 실제로 Do() 에서 해소됩니다.
    pMap_->ForbidToDelete();

    if(pMap_->IsValid(_recvTid + 1) == false && _recvTid != pTbl_->GetTid()) {
        pMap_->AllowToDelete();

        stReplicationCmd    st;

        st.cmd = ReplicationCmd::MustSync;

        if(conn_->Send((char *)&st, sizeof(st)) == false) {
            W_LOG("OTPushingAction::doRespPush Send() fail : MustSync recvTid : [%zu]", _recvTid);
            return false;
        }

        I_LOG("OTPushingAction::doRespPush Send() Success : MustSync recvTid : [%zu]", _recvTid);

        std::shared_ptr<OTPushAction<dataT>>  initAction =
            std::shared_ptr<OTInitPushAction<dataT>>(
                new OTInitPushAction<dataT>());

        pState_->SetAction(initAction);
        return true;
    }

    lastSentTid_ = _recvTid;

    stReplicationCmd    st;
    st.cmd = ReplicationCmd::OkPush;
    st.sentTid = _recvTid;

    if(conn_->Send((char *)&st, sizeof(st)) == false) {

        W_LOG("OTPushingAction::doRespPush Send() fail : OkPush");
        return false;
    }

    D_LOG("OTPushingAction::doRespPush Send() Success : OkPush sentTid : [%zu]", st.sentTid);

    return true;
}

template <typename dataT>
bool OTPushingAction<dataT>::Do() {

    pMap_->ForbidToDelete();

    size_t  tid = lastSentTid_;

    // std::cout << "[DEBUG] OTPushingAction::Do map size:" << pMap_->GetSize() << std::endl;
    for(cnt_=0; cnt_ < maxCnt_; ++cnt_) {
        if(pMap_->Get(++tid, arrData_[cnt_]) == false)
            break;

        // std::cout << "[DEBUG] OTPushingAction::Do tid:" << tid ;
        // std::cout << "[DEBUG] OTPushingAction::Do value:" << arrData_[cnt_].d.data;
        // std::cout << " [DEBUG] OTPushingAction::Do cnt:" << cnt_ << std::endl;
    }

    pMap_->AllowToDelete();
    return true;
}

template <typename dataT>
bool OTPushingAction<dataT>::Send() {

    if(cnt_ == 0) {
        poll(nullptr, 0, 50);
        return true;
    }

    stReplicationCmd    st;
    st.cmd = ReplicationCmd::DoPush;
    st.dataCnt = cnt_;

    if(conn_->Send((char *)(&st), sizeof(st)) == false) {
        W_LOG("OTPushingAction::Send Send fail : DoPush");
        return false;
    }

    if(conn_->Send((char *)arrData_, cnt_ * sizeof(stHistory<dataT>)) == false) {
        W_LOG("OTPushingAction::Send Send fail : DoPush cnt : [%zu]", cnt_);
        return false;
    }

    lastSentTid_ += cnt_;

    // D_LOG("OTPushingAction::Send Send Success cnt : [%zu] lastSentTid : [%zu]", cnt_, lastSentTid_);

    return true;
}

template <typename dataT>
size_t OTPushingAction<dataT>::SendAll() {

    size_t sum = 0;

    while(true) {
        if(Do() == false)
            break;

        if(cnt_ == 0)
            break;

        if(Send() == false)
            break;

        sum += cnt_;
    }

    return sum;
}

///////////////////////////////////////////////////////////////////////////////
// OTSyncPushAction ///////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

template <typename dataT>
class OTSyncPushAction : public OTPushAction<dataT> {
public:
    explicit OTSyncPushAction()
        : myMaxTid_(0),
          yourMaxTid_(0),
          maxCnt_(100),
          index_(0),
          cnt_(0),
          arrData_(nullptr),
          conn_(nullptr),
          pState_(nullptr) {
            D_LOG("OTSyncPushAction::OTSyncPushAction()");
          }
    virtual ~OTSyncPushAction();

    void Init(std::shared_ptr<OneTable<dataT>> _pTbl,
              std::shared_ptr<OTPushMap<dataT>> _pMap,
              OTTCPAccepter * _conn,
              OTPushActionState<dataT> * _pState,
              std::memory_order=std::memory_order_seq_cst);

    bool Recv();
    bool Do();
    bool Send();
    size_t SendAll() { return 0; }

private:
    size_t myMaxTid_;
    size_t yourMaxTid_;
    size_t maxCnt_;

    size_t index_;
    size_t cnt_;
    stSyncData<dataT> * arrData_;

    std::shared_ptr<OneTable<dataT>>    pTbl_;
    std::shared_ptr<OTPushMap<dataT>>   pMap_;
    OTTCPAccepter * conn_;
    OTPushActionState<dataT> * pState_;

};

template <typename dataT>
OTSyncPushAction<dataT>::~OTSyncPushAction() {

    D_LOG("OTSyncPushAction::~OTSyncPushAction()");

    pMap_->AllowToDelete();

    cnt_ = 0;

    if(arrData_) {
        delete [] arrData_;
        arrData_ = nullptr;
    }

    conn_ = nullptr;
    pState_ = nullptr;
}

template <typename dataT>
void OTSyncPushAction<dataT>::Init(std::shared_ptr<OneTable<dataT>> _pTbl,
                              std::shared_ptr<OTPushMap<dataT>> _pMap,
                              OTTCPAccepter * _conn,
                              OTPushActionState<dataT> * _pState,
                              std::memory_order)
{
    pTbl_ = std::atomic_load(&_pTbl);
    pMap_ = _pMap;
    conn_ = _conn;
    pState_ = _pState;

    if(arrData_)
        delete [] arrData_;

    arrData_ = new stSyncData<dataT>[maxCnt_];

    // Sync 를 시작할테니까요.. 그전에 Keep 한 데이터는 필요 없어요.
    myMaxTid_ = _pTbl->GetTid();
    pMap_->Clear();
    pMap_->ForbidToDelete();
}

template <typename dataT>
bool OTSyncPushAction<dataT>::Recv() {
    conn_->SetWait(0);

    int     readn;
    stReplicationCmd    st;

    if((readn = conn_->Recv((char *)&st, sizeof(st))) < 0) {
        W_LOG("OTSyncPushAction::Recv Recv() fail");
        return false;
    }

    if(readn == 0)
        return true;

    // Sync 기간 동안 뭘 받을께 없어요..
    W_LOG("OTSyncPushAction::Recv Unexpected cmd : [%zu]", static_cast<size_t>(st.cmd));
    return false;
}

template <typename dataT>
bool OTSyncPushAction<dataT>::Do() {

    bool    bMyTid;
    size_t  tid;

    bool    use;
    dataT   d;

    for(cnt_=0; cnt_ < maxCnt_; ++index_) {
        if(pTbl_->DirectFetch(index_, tid, bMyTid, use, d) == false)
            break;

        if(!bMyTid)
            if(tid > yourMaxTid_) { yourMaxTid_ = tid; }

        if(use == false) {
            // use == false && tid == 0 은 한번도 사용되지 않음을 뜻함

            if(tid == 0)
                break;

            continue;
        }

        // std::cout << "[DEBUG] OTSyncPushAction::Do() index_: " << index_ << std::endl;
        // std::cout << "[DEBUG] OTSyncPushAction::Do() tid: " << tid << std::endl;
        // std::cout << "[DEBUG] OTSyncPushAction::Do() use : " << use << std::endl;

        arrData_[cnt_] = stSyncData<dataT>(tid, bMyTid, d);
        ++cnt_;
    }

    // std::cout << "[DEBUG] OTSyncPushAction::Do() Max My Tid: " << myTid_ << std::endl;
    // std::cout << "[DEBUG] OTSyncPushAction::Do() Max Your Tid: " << yourTid_ << std::endl;

    return true;
}

template <typename dataT>
bool OTSyncPushAction<dataT>::Send() {

    if(cnt_ == 0) {

        stReplicationCmd    st;
        st.cmd = ReplicationCmd::EndSync;

        st.sentTid = myMaxTid_;
        st.recvTid = yourMaxTid_;

        if(conn_->Send((char *)&st, sizeof(st)) == false) {
            W_LOG("OTSyncPushAction::Send Send() fail : EndSync sentTid : [%zu] recvTid : [%zu]",
                st.sentTid, st.recvTid);
            return false;
        }

        I_LOG("OTSyncPushAction::Send Send() Success : EndSync sentTid : [%zu] recvTid : [%zu]",
            st.sentTid, st.recvTid);

        std::shared_ptr<OTPushAction<dataT>>  initAction =
            std::shared_ptr<OTInitPushAction<dataT>>(
                new OTInitPushAction<dataT>());

        pState_->SetAction(initAction);

        return true;
    }

    // stHistory 데이터로 읽는 것이 Header 없이 가능할까요??
    stReplicationCmd    st;
    st.cmd = ReplicationCmd::DoSync;
    st.dataCnt = cnt_;

    if(conn_->Send((char *)&st, sizeof(st)) == false) {
        W_LOG("OTPushAction::Send Send fail : DoSync");
        return false;
    }

    if(conn_->Send((char *)arrData_, cnt_ * sizeof(stSyncData<dataT>)) == false) {
        W_LOG("OTPushAction::Send Send fail : DoSync cnt : [%zu]", cnt_);
        return false;
    }

    // D_LOG("OTPushAction::Send Send Success: DoSync cnt : [%zu]", cnt_);

    return true;
}

///////////////////////////////////////////////////////////////////////////////
// OTReplicationPushWorker ////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

template <typename dataT>
class OTReplicationPushWorker {
public:
    explicit OTReplicationPushWorker(OTTCPAccepter * _conn)
        : bRun_(true)
          , maxWaitMillisec_(500)
          , conn_(_conn) {
            D_LOG("OTReplicationPushWorker::OTReplicationPushWorker()");
        }

    ~OTReplicationPushWorker();

    bool Init(std::shared_ptr<OneTable<dataT>> _pTbl,
              std::shared_ptr<OTPushMap<dataT>> _pMap,
              std::memory_order=std::memory_order_seq_cst);
    void Start();

    void Stop() { bRun_ = false; }
    bool IsRun() { return pRunRef_.use_count() > 1; }

private:
    void run();
    size_t pushAll();

private:
    std::shared_ptr<bool>   pRunRef_;
    std::atomic<bool>       bRun_;
    int                     maxWaitMillisec_;

    std::shared_ptr<OneTable<dataT>> pTbl_;
    std::shared_ptr<OTPushMap<dataT>>    pMap_;

    OTTCPAccepter * conn_;
};

template <typename dataT>
OTReplicationPushWorker<dataT>::~OTReplicationPushWorker() {

    // D_LOG("OTReplicationPushWorker::~OTReplicationPushWorker()");
    Stop();

    int cnt;
    for(cnt=0; IsRun() && cnt < 10; ++cnt) {
        poll(nullptr, 0, maxWaitMillisec_ / 10);
    }

    I_LOG("OTReplicationPushWorker::~OTReplicationPushWorker wait cnt [%d]", cnt);

    if(conn_) {
        delete conn_;
        conn_ = nullptr;
    }
}

template <typename dataT>
bool OTReplicationPushWorker<dataT>::Init(std::shared_ptr<OneTable<dataT>> _pTbl,
                std::shared_ptr<OTPushMap<dataT>> _pMap,
                std::memory_order) {
    pTbl_ = std::atomic_load(&_pTbl);
    pMap_ = _pMap;

    return true;
}

template <typename dataT>
void OTReplicationPushWorker<dataT>::Start() {
    pRunRef_ = std::shared_ptr<bool>(new bool(true));
    std::thread thr = std::thread(&OTReplicationPushWorker<dataT>::run, std::ref(*this));
    thr.detach();
}

template <typename dataT>
void OTReplicationPushWorker<dataT>::run() {
    std::shared_ptr<bool>   pRun = pRunRef_;

    // std::cout << "in [DEBUG] OTReplicationPushWorker::run():" << pRunRef_.use_count() << std::endl;

    OTPushActionState<dataT>  state(pTbl_, pMap_, conn_);

    std::shared_ptr<OTPushAction<dataT>> initAction =
        std::shared_ptr<OTInitPushAction<dataT>>(
                        new OTInitPushAction<dataT>());

    state.SetAction(initAction);
    std::shared_ptr<OTPushAction<dataT>>  action = state.GetAction();

    while(bRun_) {
        if(action->Recv() == false) {
            W_LOG("OTReplicationPushWorker::run Recv() fail");
            break;
        }

        if(state.IsChanged()) {
            action = state.GetAction();
            continue;
        }

        if(action->Do() == false) {
            W_LOG("OTReplicationPushWorker::run Do() fail");
            break;
        }

        if(action->Send() == false) {
            W_LOG("OTReplicationPushWorker::run Send() fail");
            break;
        }

        if(state.IsChanged()) {
            action = state.GetAction();
            continue;
        }
    }

    size_t sentn = action->SendAll();
    I_LOG("OTReplicationPushWorker::run out of service sentn : [%zu]", sentn);

}

///////////////////////////////////////////////////////////////////////////////
// OTReplicationPush //////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

#include "OTMasterNegotiator.hpp"

template <typename dataT>
class OTReplicationPush {
public:
    explicit OTReplicationPush(int _serverPort)
        : serverPort_(_serverPort),
          bRun_(true) {

        }
    ~OTReplicationPush() {}
    bool Run(std::shared_ptr<OneTable<dataT>>   _pTbl, std::memory_order=std::memory_order_seq_cst);
    bool PutHist(size_t _tid, const stHistory<dataT> && _d) {
        return pMap_->Put(_tid, std::forward<const stHistory<dataT>>(_d));
    }

    void Stop() { bRun_ = false; }
    bool IsRun() { return pRunRef_.use_count() > 1; }
    // bool IsPushing();
    size_t GetMapSize() { return (pMap_.use_count() == 0)?0:pMap_->GetSize(); }

private:
    void run();
    void monitorWorker();

private:
    int     serverPort_;

    std::shared_ptr<bool>   pRunRef_;
    std::atomic<bool> bRun_;

    std::shared_ptr<OneTable<dataT>>    pTbl_;
    std::shared_ptr<OTPushMap<dataT>>    pMap_;
    std::vector<std::shared_ptr<OTReplicationPushWorker<dataT>>> vecWorkers_;

    OTMasterNegotiator      masterNegotiator_;
};

template <typename dataT>
bool OTReplicationPush<dataT>::Run(std::shared_ptr<OneTable<dataT>>   _pTbl,
                                   std::memory_order) {
    pTbl_ = std::atomic_load(&_pTbl);

    size_t size = _pTbl->GetCapacity() * 4;
    size = (size > 1000000)?1000000:size;

    pMap_ = std::shared_ptr<OTPushMap<dataT>>(
        new OTPushMap<dataT>(size));

    pRunRef_ = std::shared_ptr<bool>(new bool(true));
    std::thread thr = std::thread(&OTReplicationPush<dataT>::run, std::ref(*this));
    thr.detach();

    // std::cout << "[DEBUG] OTReplicationPush::Init() bool ref :" << pRunRef_.use_count() << std::endl;

    return true;
}

template <typename dataT>
void OTReplicationPush<dataT>::monitorWorker() {
    for(auto iter = vecWorkers_.begin(); iter != vecWorkers_.end(); ) {
        if((*iter)->IsRun() == false)
            iter = vecWorkers_.erase(iter);
        else
            ++iter;
    }
}

template <typename dataT>
void OTReplicationPush<dataT>::run() {

    std::shared_ptr<bool>   pRun = pRunRef_;
    // std::cout << "in [DEBUG] OTReplicationPush::run():" << pRunRef_.use_count() << std::endl;

    OTTCPServer     server;
    //if(server.CreateV6(serverPort_) == false) {
    if(server.Create(serverPort_) == false) {
        W_LOG("OTReplicationPush::run CreateV6() fail");
        Stop();
        return ;
    }

    int nRet = false;
    server.SetWait(100);
    OTTCPAccepter * conn = nullptr;

    while(bRun_) {

        conn = nullptr;
        if((nRet = server.WaitClient(&conn)) == 0) {
            monitorWorker();
            continue;
        }

        if(nRet < 0) {
            //if(server.CreateV6(serverPort_) == false) {
            if(server.Create(serverPort_) == false) {
				W_LOG("OTReplicationPush::run CreateV6() fail");
				Stop();
				return ;
            }

            server.SetWait(100);
			continue;
        }

        if(masterNegotiator_.Negotiate(conn) == false) {
            if(conn)
                delete conn;
            W_LOG("OTReplicationPush::run Negotiate() fail");
            continue;
        }

        I_LOG("OTReplicationPush::run I am %s", masterNegotiator_.IsMaster()?"Master":"Slave");

        std::shared_ptr<OTReplicationPushWorker<dataT>>
            worker = std::shared_ptr<OTReplicationPushWorker<dataT>>(
                        new OTReplicationPushWorker<dataT>(conn));

        if(worker->Init(pTbl_, pMap_) == false) {

            // conn 은 worker 안의 소멸자에서 삭제됩니다. 걱정 마세요.
            W_LOG("OTReplicationPush::run Worker Init fail");
            continue;
        }

        vecWorkers_.push_back(worker);
        worker->Start();
    }
}




}

#endif // OT_REPLICATION_PUSH_HPP
