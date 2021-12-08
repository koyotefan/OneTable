/*
 * **************************************************************************
 *
 * OneTable.hpp
 *
 * **************************************************************************
 * Description:
 *
 *
 * **************************************************************************
 *
 * (c) Copyright 2002 nTels Inc.
 *
 * ALL RIGHTS RESERVED
 *
 *
 * The software and information contained herein are proprietary to, and
 * comprise valuable trade secrets of, nTels Inc., which intends to preserve
 * as trade secrets such software and information.
 * This software is furnished pursuant to a written license agreement and
 * may be used, copied, transmitted, and stored only in accordance with
 * the terms of such license and with the inclusion of the above copyright
 * notice.  This software and information or any other copies thereof may
 * not be provided or otherwise made available to any other person.
 *
 * Notwithstanding any other lease or license that may pertain to, or
 * accompany the delivery of, this computer software and information, the
 * rights of the Government regarding its use, reproduction and disclosure
 * are as set forth in Section 52.227-19 of the FARS Computer
 * Software-Restricted Rights clause.
 *
 * Use, duplication, or disclosure by the Government is subject to
 * restrictions as set forth in subparagraph (c)(1)(ii) of the Rights in
 * Technical Data and Computer Software clause at DFARS 252.227-7013.
 * Contractor/Manufacturer is nTels Inc. Geumha Bldg. 15th Floor,Cheong
 * dam 2-Dong, Kangnam-Ku, Seoul, Korea 135-766
 *
 * This computer software and information is distributed with "restricted
 * rights."  Use, duplication or disclosure is subject to restrictions as
 * set forth in NASA FAR SUP 18-52.227-79 (April 1985) "Commercial
 * Computer Software-Restricted Rights (April 1985)."  If the Clause at
 * 18-52.227-74 "Rights in Data General" is specified in the contract,
 * then the "Alternate III" clause applies.
 *
 * *************************************************************************
 *
 *      Auther : jhchoi                       2017.01.20
 *
 * *************************************************************************
 *
 *      Patch Report:
 *
 * *************************************************************************
 *      Version    Date    Patch Number             Description
 * *************************************************************************
 *
 *
 * *************************************************************************
 *
 *      Amendment History
 *      Begin
 * *************************************************************************
 *      Version    Date          Author         Description
 * *************************************************************************
 *       0.01    2017.01.20      jhchoi         Initial
 *
 *      End of Amendment
 * *************************************************************************
 */

#ifndef ONE_TABLE_HPP
#define ONE_TABLE_HPP

#include <stddef.h>
#include <limits>
#include <functional>

#include "OTDefine.hpp"
#include "OTElement.hpp"
#include "OTElementManager.hpp"
#include "OTRepository.hpp"

namespace nsOneTable {

template <typename dataT>
class OTReplication;

template <typename dataT>
class OneTable {
public:
    explicit OneTable(std::string _tName, std::string _path);
    ~OneTable();

    bool DefinePK(off_t _offset, size_t _keySize, nsOneTable::KeyType _kType);
    bool DefineSK(int _index, off_t _offset, size_t _keySize, nsOneTable::KeyType _kType);
    bool Init(size_t    _capacity);
    void Clear();
    bool Truncate();

    // PK
    bool Put(const dataT & _d);
    bool Get(const void * _pKey, size_t _keySize, dataT & _d);
    bool Pop(const void * _pKey, size_t _keySize, dataT & _d);
    bool Del(const void * _pKey, size_t _keySize);
    bool Del(const dataT & _d);
    bool Upd(const dataT & _d);
    bool UpdForce(const dataT & _d);

    // SK
    bool GetOne(int _skeyOrder, const void * _sKey, size_t _sKeySize, dataT & _d);
    int  Get(int _skeyOrder, const void * _sKey, size_t _sKeySize, dataT ** _d);
    bool GetFront(int _skeyOrder, dataT & _d);
    bool GetOrderBy(int _skeyOrder, int _dataOrder, dataT & _d);
    bool PopWithCondition(int _skeyOrder, dataT & _d, std::function<int(dataT & _d)> _func);

    void FreeResult(dataT ** _p);
    bool PopFront(int _order, dataT & _d);
    int  Del(int _order, const void * _sKey, size_t _sKeySize);

    // Master or Slave
    bool IsMaster();

    size_t GetDataCount() { return repository_.GetDataCount(); }
    size_t GetCapacity() { return repository_.GetCapacity(); }
    size_t GetTid() { return repository_.GetTid(); }
    void SetTid(size_t _tid) { repository_.SetTid(_tid); }

    std::string ExtractKey(const dataT & _d);
    std::string ExtractKey(int _order, const dataT & _d);
    std::string ExtractKey(int _order, const void * _sKey, size_t _sKeySize);


    // For Replication
    void SetReplicationRawPtr(OTReplication<dataT> * _p);

    bool DirectFetch(size_t _index, size_t & _tid, bool & _bMyTid,  bool & _use, dataT & _d);
    // bool DirectTidApply(size_t _index, size_t _tid);
    bool PutByRepl(size_t _tid, bool _bMyTid, const dataT & _d);
    bool DelByRepl(size_t _tid, bool _bMyTid, const dataT & _d);

    size_t GetReplicatedTid() { return repository_.GetReplicatedTid(); }
    void SetReplicatedTid(size_t _tid) { repository_.SetReplicatedTid(_tid); }

    bool IsNormalReplicationPushing();

    // Set Auto Increase
    void SetAutoIncrease(bool _bAutoIncreasement=false) { bAutoIncreasement_ = _bAutoIncreasement; } 

private:
    // bool put(size_t _tid, bool _bMyTid, dataT & _d);
    PKElement * putElement(std::string & _key, const dataT & _d);

    bool del(const std::string & _key);
    bool del(PKElement * _pke);
    bool del(PKElement * _pke, const std::string & _key, bool _bRepl=true);

    bool getOrderByNotMutex(int _skeyOrder, int _dataOrder, dataT & _d);

    void dropIndex();
    bool makeupIndex();
    // bool putIndex(size_t _index, dataT & _d);
    // bool putIndex(dataT & _d);
    void cleanIndex();
    bool increaseSize();

private:
    std::string     tName_;
    std::string     path_;

    std::mutex  mutex_;
    Repository<dataT>     repository_;

    PKManager<dataT>  * pkm_;

    size_t  sKeyCnt_;
    SKManager<dataT> * arrSKm_[nsOneTable::MAX_MULTI_KEY_CNT];

    // For Replication
    OTReplication<dataT> * repl_;

   // for Auto Increasement
   bool     bAutoIncreasement_;
};


template <typename dataT>
OneTable<dataT>::OneTable(std::string _tName, std::string _path) :
        repository_(_tName, _path),
        pkm_(nullptr),
        sKeyCnt_(0) {

    D_LOG("OneTable construct()");
    for(int nLoop=0; nLoop < nsOneTable::MAX_MULTI_KEY_CNT; nLoop++)
        arrSKm_[nLoop] = nullptr;

    repl_               = nullptr;
    bAutoIncreasement_  = false;
}

template <typename dataT>
OneTable<dataT>::~OneTable() {
    D_LOG("OneTable destroy()");
    dropIndex();
}

template <typename dataT>
bool OneTable<dataT>::DefinePK(off_t _offset,
                                size_t _keySize,
                                nsOneTable::KeyType _kType) {
    std::lock_guard<std::mutex> guard(mutex_);

    pkm_ =
        new (std::nothrow) PKManager<dataT>(_offset, _keySize, _kType);

    if(!pkm_) {
        // std::cout << "PKManager new() fail" << std::endl;
        return false;
    }

    return true;
}

template <typename dataT>
bool OneTable<dataT>::DefineSK(int _index,
                                off_t  _offset,
                                size_t _keySize,
                                nsOneTable::KeyType _kType) {

    std::lock_guard<std::mutex> guard(mutex_);

    if(_index >= nsOneTable::MAX_MULTI_KEY_CNT)
        return false;

    if(sKeyCnt_ > size_t(_index))
        return false;

    if(arrSKm_[_index])
        return false;

    SKManager<dataT> * skm =
        new (std::nothrow) SKManager<dataT>(_offset, _keySize, _kType);

    if(!skm) {
        // std::cout << "SKManager new() fail" << std::endl;
        return false;
    }

    arrSKm_[_index] = skm;
    sKeyCnt_++;
    return true;
}

/*--------------------------
// 데이터를 줄이는 것을 테스트 해 보기 위함.
void OneTable<dataT>::TestDecreaseSize() {

    repository_.Decrease();
    size_t afterCapa = repository_.GetCapacity();

    if(pkm_->Init(afterCapa, sKeyCnt_) == false) {
        // std::cout << "------ OneTable::Decrease #1 pkm->Init() fail" << std::endl;
        return ;
    }

    // std::cout << "pkm Init() success" << std::endl;
    for(int nLoop=0; nLoop < sKeyCnt_; nLoop++) {
        SKManager<dataT> * skm = arrSKm_[nLoop];

        if(!skm)
            break;

        // std::cout << "skm Init() start" << std::endl;
        if(skm->Init(afterCapa) == false) {
            // std::cout << "------ OneTable::Decrease #2 skm->Init() fail" << std::endl;
            return ;
        }
    }

    // std::cout << "------- OneTable::Decrease Success" << std::endl;

}
--------------------------*/


template <typename dataT>
bool OneTable<dataT>::Init(size_t  _capacity) {

    if(_capacity == 0)
        return false;

    if(!pkm_) {
        E_LOG("The primary key is is not defined");
        return false;
    }

    // 여기도 repository 먼저 확인 하고요..
    // 그 다음에.... repository 를 얻은 후에요..

    if(repository_.Init(_capacity) == false) {
        E_LOG("Repository Init() fail");
        return false;
    }

    _capacity = repository_.GetCapacity();

    if(pkm_->Init(_capacity, sKeyCnt_) == false) {
        E_LOG("PKManager Init() fail");
        return false;
    }

    for(size_t nLoop=0; nLoop < sKeyCnt_; nLoop++) {
        SKManager<dataT> * skm = arrSKm_[nLoop];

        if(!skm)
            break;

        // std::cout << "skm Init() start" << std::endl;
        if(skm->Init(_capacity) == false) {
            E_LOG("SKManager Init() fail");
            return false;
        }

        // std::cout << "skm Init() success" << std::endl;
    }

    // repository 를 읽고, 구성해야 합니다.
    if(makeupIndex() == false) {

        E_LOG("Repository Init() fail");
        return false;
    }

    // std::cout << "Init() success" << std::endl;
    return true;
}

template <typename dataT>
void OneTable<dataT>::Clear() {
    std::lock_guard<std::mutex> guard(mutex_);

    repository_.Clear();
    cleanIndex();
}

template <typename dataT>
bool OneTable<dataT>::Truncate() {
    std::lock_guard<std::mutex> guard(mutex_);

    size_t capacity = repository_.GetCapacity();

    repository_.Clear();
    dropIndex();

    return Init(capacity);
}

template <typename dataT>
void OneTable<dataT>::dropIndex() {

    if(pkm_) {
        PKManager<dataT> * tempM = pkm_->Clone();
        delete pkm_;
        pkm_ = tempM;
    }

    for(size_t nLoop=0; nLoop < sKeyCnt_; nLoop++) {
        SKManager<dataT> * skm = arrSKm_[nLoop];

        if(skm) {

            SKManager<dataT> * tempM = skm->Clone();
            delete skm;
            arrSKm_[nLoop] = tempM;
        }
    }
}

template <typename dataT>
bool OneTable<dataT>::makeupIndex() {

    size_t index = 0;
    size_t readn = 0;
    size_t use = repository_.GetDataCount();
    size_t capacity = repository_.GetCapacity();

    dataT d;
    cleanIndex();

    while(true) {
        if(readn >= use || index >= capacity)
            break;

        if(repository_.Read(d, index)) {

            std::string key = pkm_->ExtractKey(d);
            PKElement * pke = nullptr;

            if((pke = putElement(key, d)) == nullptr) {
                E_LOG("OneTable::makeupIndex() putElement fail [%s]", key.c_str());
                return false;
            }

            if(index != pke->id)
                repository_.Swap(index, pke->id);

            /*-
            repository_.Erase(index);
            if(put(tid, bMyTid, d) == false) {
                E_LOG("OneTable::Init() makeUpIndex fail");
                return false;
            }
            -*/

            readn++;
        }

        /*-
        if(repository_.Read(d, index)) {
            // I_LOG("-------- makeupIndex : use index[%zu]", index);

            repository_.Erase(index);
            if(put(d) == false) {
                E_LOG("OneTable::Init() makeUpIndex fail");
                return false;
            }
            readn++;
        }
        -*/

        index++;

        if(index % 100000 == 0)
            I_LOG("makeupIndex : index[%zu] readn[%zu]", index, readn);
    }

    // 보정 작업
    repository_.SetDataCount(readn);

    I_LOG("makeUpIndex readn[%zu] use[%zu] index[%zu] capa[%zu]",
        readn, use, index, capacity);
    return true;
}

template <typename dataT>
void OneTable<dataT>::cleanIndex() {
    pkm_->Clear();
    for(size_t nLoop=0; nLoop < sKeyCnt_; nLoop++) {
        SKManager<dataT> * skm = arrSKm_[nLoop];
        skm->Clear();
    }
}

/*-
template <typename dataT>
bool OneTable<dataT>::putIndex(size_t _index, dataT & _d) {
    // 특정 위치에 지정 하기 위함 입니다.

    std::string key = pkm_->ExtractKey(_d);
    PKElement * pke = pkm_->Alloc(_index, key);

    if(!pke) {
        D_LOG("putIndex() PKElement Alloc() fail");
        return false;
    }

    for(int nLoop=0; nLoop < sKeyCnt_; nLoop++) {
        SKManager<dataT> * skm = arrSKm_[nLoop];

        if(!skm)
            break;

        skm->Alloc(nLoop, pke, _d);
    }

    return true;
}
-*/

template <typename dataT>
PKElement * OneTable<dataT>::putElement(std::string & _key, const dataT & _d) {

    PKElement * pke = pkm_->Alloc(_key);

    if(!pke) {
        // std::cout << "PK Alloc() fail - keySize:" << key.size() << std::endl;
        W_LOG("OneTable::putElement() used [%zu] capa [%zu]",
            repository_.GetDataCount(), repository_.GetCapacity());

        if(bAutoIncreasement_ == false)
            return nullptr;

        if(increaseSize() == false) {
            W_LOG("OneTable::Put() increaseSize() fail");
            return nullptr;
        }

        // 재시도.
        I_LOG("OneTable::putElement() increaseSize used [%zu] capa [%zu]",
            repository_.GetDataCount(), repository_.GetCapacity());
        pke = pkm_->Alloc(_key);

        if(!pke)
            return nullptr;
    }

    // std::cout << "Alloc OK :" << (void *)pke << std::endl;

    for(size_t nLoop=0; nLoop < sKeyCnt_; nLoop++) {
        SKManager<dataT> * skm = arrSKm_[nLoop];

        if(!skm)
            break;

        skm->Alloc(nLoop, pke, _d);
    }

    return pke;
}


/*-
template <typename dataT>
bool OneTable<dataT>::put(size_t _tid, bool _bMyTid, dataT & _d) {
    std::string key = pkm_->ExtractKey(_d);
    PKElement * pke = nullptr;

    if((pke = putElement(key, _d)) == nullptr) {
        W_LOG("OneTable::put() putElement Fail");
        return false;
    }

    if(repository_.WriteByRepl(pke->id, _tid, _bMyTid, _d) == false) {
        W_LOG("OneTable::put() WriteByRepl Fail [%zu] used [%zu] capa [%zu] key [%s]",
            pke->id, repository_.GetDataCount(), repository_.GetCapacity(), key.c_str());
        return false;
    }

    return true;
}
-*/


template <typename dataT>
bool OneTable<dataT>::Put(const dataT & _d) {

    std::lock_guard<std::mutex> guard(mutex_);

    std::string key = pkm_->ExtractKey(_d);
    // std::cout << "1. debug Put().....key :" << key << std::endl;

    if(pkm_->IsExist(key)) {
        // std::cout << "2. debug Put()..IsExist...key :" << key << std::endl;
        return false;
    }

    PKElement * pke = nullptr;

    if((pke = putElement(key, _d)) == nullptr) {
        W_LOG("OneTable::put() putElement Fail");
        return false;
    }

    if(repository_.Write(pke->id, _d) == false) {
        W_LOG("OneTable::Put() Write Fail [%zu] used [%zu] capa [%zu] key [%s]",
            pke->id, repository_.GetDataCount(), repository_.GetCapacity(), key.c_str());
        del(pke, key, false);
        return false;
    }

    // D_LOG("[DEBUG] OneTable::Put Write OK [%zu] Tid [%zu]" , pke->id, repository_.GetTid());

    if(repl_) {
        // D_LOG("[DEBUG] OneTable::Put() Tid : [%zu] Key [%s]",
        //    repository_.GetTid(), key.c_str());
        repl_->PutHist(repository_.GetTid(), true, _d);
    }

    return true;
}

template <typename dataT>
bool OneTable<dataT>::PutByRepl(size_t _tid, bool _bMyTid, const dataT & _d) {

    std::lock_guard<std::mutex> guard(mutex_);

    std::string key = pkm_->ExtractKey(_d);
    // std::cout << "1. debug Put().....key :" << key << std::endl;

    if(pkm_->IsExist(key)) {
        // std::cout << "2. debug Put()..IsExist...key :" << key << std::endl;
        // 지우고 다시 Insert 시도 해야 하는거 아님??
        return false;
    }

    PKElement * pke = nullptr;

    if((pke = putElement(key, _d)) == nullptr) {
        W_LOG("OneTable::put() putElement Fail");
        return false;
    }

    if(repository_.WriteByRepl(pke->id, _tid, _bMyTid, _d) == false) {
        W_LOG("OneTable::PutByRepl() WriteByRepl Fail [%zu] used [%zu] capa [%zu] tid [%zu]  key [%s]",
            pke->id, repository_.GetDataCount(), repository_.GetCapacity(), _tid, key.c_str());
        del(pke, key, false);
    }

    // D_LOG("[DEBUG] OneTable::PutByRepl() Success tid [%zu] key [%s]",
    //    _tid, key.c_str());

    return true;
}

template <typename dataT>
bool OneTable<dataT>::Get(const void * _pKey,
                            size_t _keySize,
                            dataT & _d) {

    std::lock_guard<std::mutex> guard(mutex_);

    /*-
    if(repl_ && repl_->AllowedGetCommand() == false) {
        D_LOG("OneTable::Get() I am Slave");
        return false;
    }
    -*/

    if(_pKey == nullptr || _keySize != pkm_->GetKeySize())
        return false;

    PKElement * pke = pkm_->Get(pkm_->ExtractKey((const char *)_pKey));

    if(!pke) {
        std::string key =  pkm_->ExtractKey((const char *)_pKey);
        D_LOG("OneTable::Get() Can't find in PKManager [%s]", key.c_str());
        return false;
    }

    return repository_.Read(_d, pke->id);
}

template <typename dataT>
bool OneTable<dataT>::Pop(const void * _pKey,
                            size_t _keySize,
                            dataT & _d) {

    if(Get(_pKey, _keySize, _d) == false) {
        // D_LOG("OneTable::Pop() Can't Get() [%s]", (const char *)_pKey);
        return false;
    }

    if(Del(_pKey, _keySize) == false) {
        // D_LOG("OneTable::Pop() Can't Del() [%s]", (const char *)_pKey);
        return false;
    }

    return true;
}

template <typename dataT>
bool OneTable<dataT>::Del(const void * _pKey,
                            size_t _keySize) {

    std::lock_guard<std::mutex> guard(mutex_);

    if(_pKey == nullptr || _keySize != pkm_->GetKeySize())
        return false;

    return del(pkm_->ExtractKey((const char *)_pKey));

}

template <typename dataT>
bool OneTable<dataT>::Del(const dataT & _d) {

    std::lock_guard<std::mutex> guard(mutex_);

    return del(pkm_->ExtractKey(_d));
}

template <typename dataT>
bool OneTable<dataT>::DelByRepl(size_t _tid, bool _bMyTid, const dataT & _d) {

    std::lock_guard<std::mutex> guard(mutex_);

    std::string key = pkm_->ExtractKey(_d);
    PKElement * pke = pkm_->Get(key);

    if(!pke) {
        W_LOG("OneTable::DelByRepl() Can't find in PKElement [%s]", key.c_str());
        //return false;
        return true;
    }

    repository_.EraseByRepl(pke->id, _tid, _bMyTid);

    for(size_t nLoop=0; nLoop < sKeyCnt_; nLoop++) {
        SKManager<dataT> * skm = arrSKm_[nLoop];

        if(!skm) {
            // std::cout << "DEBUG OneTable::del .... 3" << std::endl;
            continue;
        }
        skm->Release(pke->arrSKe[nLoop]);
    }

    pkm_->Release(key, pke);

    // D_LOG("*OneTable::DelByRepl() Success tid [%zu] key [%s]",
    //     _tid, key.c_str());
    return true;
}

template <typename dataT>
bool OneTable<dataT>::IsNormalReplicationPushing() {
    if(repl_ == nullptr)
        return false;

    return repl_->IsPushing();
}


template <typename dataT>
bool OneTable<dataT>::del(const std::string & _key) {

    PKElement * pke = pkm_->Get(_key);
    return del(pke, _key);
}

template <typename dataT>
bool OneTable<dataT>::del(PKElement * _pke) {

    if(!_pke) {
        // std::cout << "DEBUG OneTable::del ..... 1 - GetValue() fail" << std::endl;
        return false;
    }

    std::string key;
    if(repository_.ReadValue(key,
                         _pke->id,
                         pkm_->GetKeyOffset(),
                         pkm_->GetKeySize(),
                         pkm_->IsStringKeyType()) == false) {
        D_LOG("OneTable::del() by PKElement ReadValue() fail id[%zu]", _pke->id);
        return false;
    }

    return del(_pke, key);
}


template <typename dataT>
bool OneTable<dataT>::del(PKElement * _pke, const std::string & _key, bool _bRepl) {

    if(!_pke) {
        // std::cout << "DEBUG OneTable::del ..... 1 - GetValue() fail" << std::endl;
        return false;
    }

    repository_.Erase(_pke->id);

    for(size_t nLoop=0; nLoop < sKeyCnt_; nLoop++) {
        SKManager<dataT> * skm = arrSKm_[nLoop];

        if(!skm) {
            // std::cout << "DEBUG OneTable::del .... 3" << std::endl;
            continue;
        }
        skm->Release(_pke->arrSKe[nLoop]);
    }

    // std::cout << "........ del: " << key << std::endl;
    pkm_->Release(_key, _pke);

    if(_bRepl && repl_) {

        bool    bMyTid;
        size_t  tid;
        bool    use;
        dataT   d;

        if(repository_.DirectFetch(_pke->id, tid, bMyTid, use, d) == true) {
            //D_LOG("[DEBUG] OneTable::del() Tid : [%zu] Key [%s]",
            //    tid, _key.c_str());
            repl_->PutHist(tid, use, d);

        }
    }

    // std::cout << "[DEBUG] OneTable::del() GetTid :" << repository_.GetTid() << std::endl;

    return true;
}

template <typename dataT>
bool OneTable<dataT>::Upd(const dataT & _d) {

    {
        std::lock_guard<std::mutex> guard(mutex_);

        std::string    pKey = pkm_->ExtractKey(_d);

        if(del(pKey) == false)
            return false;
    }

    return Put(_d);
}

template <typename dataT>
bool OneTable<dataT>::UpdForce(const dataT & _d) {

    if(Upd(_d) == false)
        return Put(_d);

    return true;
}

template <typename dataT>
bool OneTable<dataT>::GetFront(int _skeyOrder, dataT & _d) {
    std::lock_guard<std::mutex> guard(mutex_);
    return getOrderByNotMutex(_skeyOrder, 0, _d);
}

template <typename dataT>
bool OneTable<dataT>::PopWithCondition(int _sKeyOrder, dataT & _d, std::function<int(dataT & _d)> _func) {

    std::lock_guard<std::mutex> guard(mutex_);

    for(int order=0;;++order) {
        if(getOrderByNotMutex(_sKeyOrder, order, _d) == false)
            return false;

        int nRet = _func(_d);

        if(nRet < 0)
            return false;

        if(nRet > 0) {
            del(pkm_->ExtractKey(_d));
            return true;
        }
    }
}

template <typename dataT>
bool OneTable<dataT>::GetOrderBy(int _skeyOrder, int _dataOrder, dataT & _d) {
    std::lock_guard<std::mutex> guard(mutex_);
    return getOrderByNotMutex(_skeyOrder, _dataOrder, _d);
}

template <typename dataT>
bool OneTable<dataT>::getOrderByNotMutex(int _skeyOrder, int _dataOrder, dataT & _d) {

    /*-
    if(repl_ && repl_->AllowedGetCommand() == false) {
        D_LOG("OneTable::GetOne() I am Slave");
        return false;
    }
    -*/

    if(_skeyOrder >= sKeyCnt_) {
        D_LOG("OneTable::GetOne() Get order is wrong");
        return false;
    }

    SKManager<dataT> * skm = arrSKm_[_skeyOrder];

    if(!skm)
        return false;

    SKElement * ske = skm->Get(_dataOrder);

    if(!ske)
        return false;

    PKElement * pke = ske->pke;

    return repository_.Read(_d, pke->id);
}

template <typename dataT>
bool OneTable<dataT>::GetOne(int _skeyOrder, const void * _sKey, size_t _sKeySize, dataT & _d) {

    std::lock_guard<std::mutex> guard(mutex_);

    /*-
    if(repl_ && repl_->AllowedGetCommand() == false) {
        D_LOG("OneTable::GetOne() I am Slave");
        return false;
    }
    -*/

    if(_skeyOrder >= sKeyCnt_) {
        D_LOG("OneTable::GetOne() Get order is wrong");
        return false;
    }

    if(_sKey == nullptr || _sKeySize == 0) {
        D_LOG("OneTable::GetOne() Key [%p] or KeySize [%zu] is wrong",
            _sKey, _sKeySize);
        return false;
    }

    SKManager<dataT> * skm = arrSKm_[_skeyOrder];

    if(!skm)
        return false;

    SKElement * ske = nullptr;

    if(_sKey && _sKeySize == skm->GetKeySize())
        ske = skm->Get(skm->ExtractKey((const char *)_sKey, _sKeySize));
    else
        return false;

    if(!ske)
        return false;

    PKElement * pke = ske->pke;

    return repository_.Read(_d, pke->id);
}

template <typename dataT>
int OneTable<dataT>::Get(int _skOrder, const void * _sKey, size_t _sKeySize, dataT ** _d) {

    std::lock_guard<std::mutex> guard(mutex_);

    /*-
    if(repl_ && repl_->AllowedGetCommand() == false) {
        D_LOG("OneTable::Get() I am Slave");
        return -2;
    }
    -*/

    if(_skOrder >= (int)sKeyCnt_) {
        D_LOG("OneTable::Get() Get order is wrong");
        return -1;
    }

    SKManager<dataT> * skm = arrSKm_[_skOrder];

    if(!skm)
        return -1;

    std::string     sKey = skm->ExtractKey((const char *)_sKey, _sKeySize);

    size_t cnt = skm->GetElementCount(sKey);
    if(cnt == 0)
        return 0;

    if((*_d = new (std::nothrow) dataT[cnt + 1]) == nullptr)
        return -1;

    dataT * p = *_d;
    size_t nLoop = 0;
    SKElement * ske = skm->Get(sKey);
    for( ; ske != nullptr && nLoop < cnt; ++nLoop) {
        PKElement * pke = ske->pke;
        repository_.Read(p[nLoop], pke->id);
        ske = static_cast<SKElement *>(ske->next);
    }

    return static_cast<int>(nLoop);
}

template <typename dataT>
void OneTable<dataT>::FreeResult(dataT ** _p) {

    if(*_p != nullptr) {
        delete [] *_p;
        *_p = nullptr;
    }
}

template <typename dataT>
bool OneTable<dataT>::PopFront(int _order, dataT & _d) {

    if(GetFront(_order, _d) == false)
        return false;

    return Del(_d);
}

template <typename dataT>
int OneTable<dataT>::Del(int _order, const void * _sKey, size_t _sKeySize) {

    std::lock_guard<std::mutex> guard(mutex_);

    if(_order >= (int)sKeyCnt_) {
        D_LOG("OneTable::Del() Get order is wrong");
        return -1;
    }

    SKManager<dataT> * skm = arrSKm_[_order];

    if(!skm)
        return -1;

    std::string sKey = skm->ExtractKey((const char *)_sKey, _sKeySize);

    size_t cnt = skm->GetElementCount(sKey);
    if(cnt == 0)
        return 0;

    size_t nLoop = 0;
    SKElement * ske = skm->Get(sKey);
    SKElement * next = static_cast<SKElement *>(ske->next);

    for( ; ske != nullptr && nLoop < cnt; ++nLoop) {
        if(del(ske->pke) == false)
            break;

        ske = next;

        if(ske)
            next = static_cast<SKElement *>(ske->next);
    }

    return static_cast<int>(nLoop);
}

template <typename dataT>
bool OneTable<dataT>::IsMaster() {
    if(repl_ == nullptr)
        return true;

    return repl_->IsMaster();
}

template <typename dataT>
std::string OneTable<dataT>::ExtractKey(const dataT & _d) {
    return pkm_->ExtractKey(_d);
}

template <typename dataT>
std::string OneTable<dataT>::ExtractKey(int _order, const dataT & _d) {
    if(_order >= sKeyCnt_)
        return std::string();

    SKManager<dataT> * skm = arrSKm_[_order];
    return skm->ExtractKey(_d);
}

template <typename dataT>
std::string OneTable<dataT>::ExtractKey(int _order, const void * _sKey, size_t _sKeySize) {
    if(_order >= sKeyCnt_)
        return std::string();

    SKManager<dataT> * skm = arrSKm_[_order];
    return skm->ExtractKey((const char *)_sKey, _sKeySize);
}

/*-----------------------------

template <typename dataT>
bool OneTable<dataT>::DirectTidApply(size_t _index, size_t _tid) {
    std::lock_guard<std::mutex> guard(mutex_);
    return repository_.DirectTidApply(_index, _tid);
}

template <typename dataT>
bool OneTable<dataT>::DirectApply(size_t _index, size_t _tid, bool _use, dataT & _d, bool _bLock) {

    if(_bLock)
        std::lock_guard<std::mutex> guard(mutex_);

    return repository_.DirectApply(_index, _tid, _use, _d);
}


template <typename dataT>
bool OneTable<dataT>::DirectFetchGT(size_t & _index, size_t & _tid, bool & _use, dataT & _d, bool _bLock) {

    if(_bLock)
        std::lock_guard<std::mutex> guard(mutex_);

    size_t orgTid = _tid;

    while(true) {
        if(repository_.DirectFetch(_index, _tid, _use, _d) == false)
            return false;

        if(_tid > orgTid)
            return true;

        _index++;
    }

}
-----------------------------*/

template <typename dataT>
void OneTable<dataT>::SetReplicationRawPtr(OTReplication<dataT> * _p) {
    repl_ = _p;
}

template <typename dataT>
bool OneTable<dataT>::DirectFetch(size_t _index, size_t & _tid, bool & _bMyTid, bool & _use, dataT & _d) {

    std::lock_guard<std::mutex> guard(mutex_);

    return repository_.DirectFetch(_index, _tid, _bMyTid, _use, _d);
}

template <typename dataT>
bool OneTable<dataT>::increaseSize() {

    size_t beforeCapacity = repository_.GetCapacity();
    size_t afterCapacity  = repository_.Increase();

    if(afterCapacity == 0 || beforeCapacity >= afterCapacity) {
        return false;
    }

    if(pkm_->Init(afterCapacity, sKeyCnt_) == false) {
        W_LOG("OneTable::increaseSize() PKManager Init() fail [%zu]", afterCapacity);
        repository_.Decrease(); // 마지막 dPage 를 줄인다.
        return false;
    }

    for(size_t nLoop=0; nLoop < sKeyCnt_; nLoop++) {
        SKManager<dataT> * skm = arrSKm_[nLoop];

        if(!skm)
            break;

        if(skm->Init(afterCapacity) == false) {
            W_LOG("OneTable::increaseSize() SKManager Init() fail [%zu]", afterCapacity);

            repository_.Decrease();
            pkm_->Init(beforeCapacity, sKeyCnt_);

            for(size_t rollback=0; rollback < nLoop; rollback++) {
                SKManager<dataT> * rSkm = arrSKm_[rollback];

                if(!rSkm)
                    break;

                rSkm->Init(beforeCapacity);
            }

            return false;
        }
    }

    return true;
}


}

#endif // ONE_TABLE_HPP
