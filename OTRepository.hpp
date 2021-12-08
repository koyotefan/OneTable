/*
 * **************************************************************************
 *
 * OTRepository.hpp
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

#ifndef OT_REPOSITORY_HPP
#define OT_REPOSITORY_HPP

#include <sys/mman.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <cstring>
#include <vector>
#include <algorithm>

#include "OTDefine.hpp"

namespace nsOneTable {

template <typename dataT>
struct  stRecord {
    bool    use;
    bool    bMyTid;
    size_t  tid;
    dataT   d;
} __attribute__((packed));

struct stMetaRecord {
    size_t  capacity;
    size_t  use;
    size_t  tid;
    size_t  replicatedTid;

    size_t  recordSize;
    size_t  recordCntPerOnePage;
    size_t  sizePerOnePage;
    size_t  pageCnt;
};

template <typename dataT>
class MetaPage;

template <typename dataT>
class DataPage {
public:
    explicit DataPage(size_t _pageId, MetaPage<dataT> & _metaPage);
    ~DataPage();

    bool Open();
    bool Create();
    void Init();
    void Clear();

    bool Write(size_t _index, const dataT & _d, size_t _tid, bool _bMyTid, bool _bUpdate);
    bool Read(dataT & _d, size_t _index);
    bool ReadCondition(dataT & _d, bool & _bMyTid, size_t & _tid, bool & _use, size_t _index);
    void Erase(size_t _index, size_t _tid, bool _bMyTid=true);
    bool Apply(size_t _index, size_t _tid);
    void Swap(size_t _myIndex, DataPage<dataT> * _dPage, size_t _peerIndex);

private:
    stRecord<dataT> * getRecord(size_t _index);

private:
    size_t  pageId_;
    size_t  sIndex_;
    size_t  eIndex_;
    size_t  pageSize_;

    std::string fName_;
    int     fd_;
    stRecord<dataT> *  dR_;
};

template <typename dataT>
DataPage<dataT>::DataPage(size_t _pageId, MetaPage<dataT> & _metaPage) {
    pageId_ = _pageId;

    // 모든 데이터는 metaPage 에서 계산되어 있어야 합니다.
    sIndex_ = _metaPage.GetStartIndex(_pageId);
    eIndex_ = _metaPage.GetEndIndex(_pageId);
    pageSize_ = _metaPage.GetDataPageSize();

    fName_ = _metaPage.GetDataFname(_pageId);
    // // std::cout << "DataPage:: FName :" << fName_ << " id:" << _pageId << std::endl;
    fd_ = -1;
    dR_ = nullptr;
}

template <typename dataT>
DataPage<dataT>::~DataPage() {

    if(dR_) {
        munmap(dR_, pageSize_);
        dR_ = nullptr;
    }

    if(fd_ >= 0) {
        close(fd_);
        fd_ = -1;
    }
}

template <typename dataT>
bool DataPage<dataT>::Open() {

    // file size 와 pageSize_ 가 다르다면.. ??
    // 파일 이름을 변경하고요.. ??
    // 그걸 읽어서 복사를 해야 합니다..
    // 11.20 어쩔까??


    if((fd_ = open(fName_.c_str(), O_RDWR, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH)) < 0) {
        // std::cout << "DataPage::Open() open fail :" << errno << std::endl;
        return false;
    }

    dR_ = (stRecord<dataT> *)mmap(nullptr, pageSize_, PROT_READ|PROT_WRITE, MAP_SHARED, fd_, 0);
    if(dR_ == MAP_FAILED) {
        // std::cout << "DataPage::Open() mmap fail :" << errno << std::endl;

        close(fd_);
        fd_ = -1;

        return false;
    }

    return true;
}

template <typename dataT>
void DataPage<dataT>::Init() {

    stRecord<dataT> * pR = dR_;
    for(size_t cnt=sIndex_; eIndex_ >= cnt; ++cnt) {
        pR->use = false;
        pR->bMyTid = true;
        pR->tid = 0;
        ++pR;
    }
}

template <typename dataT>
void DataPage<dataT>::Clear() {
    Init();
}

template <typename dataT>
bool DataPage<dataT>::Create() {

    // 파일도 새롭게 만들구요..
    if((fd_ = open(fName_.c_str(), O_RDWR|O_CREAT|O_TRUNC, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH)) < 0) {
        // std::cout << "DataPage::Create() open fail :" << errno << std::endl;
        return false;
    }

    int ret = ftruncate(fd_, pageSize_);

    if(ret < 0) {
        // std::cout << "DataPage::Create() ftruncate fail :" << errno << std::endl;
        return false;
    }

    // std::cout << "DataPage::Create() success :" << std::endl;
    return true;
}

template <typename dataT>
void DataPage<dataT>::Swap(size_t _myIndex, DataPage<dataT> * _dPage, size_t _peerIndex) {

    stRecord<dataT> *   a = getRecord(_myIndex);
    stRecord<dataT> *   b = _dPage->getRecord(_peerIndex);

    std::swap(*a, *b);
    return ;
}

template <typename dataT>
stRecord<dataT> * DataPage<dataT>::getRecord(size_t _index) {
    return dR_ + (_index - sIndex_);
}

template <typename dataT>
bool DataPage<dataT>::Write(size_t _index, const dataT & _d, size_t _tid, bool _bMyTid, bool _bUpdate) {

    if(sIndex_ > _index || eIndex_ < _index)
        return false;

    stRecord<dataT> * p = dR_ + (_index - sIndex_);

    if(!_bUpdate && p->use)
        return false;

    memcpy(&(p->d), &_d, sizeof(_d));
    p->use = true;
    p->bMyTid = _bMyTid;
    p->tid = _tid;
    return true;
}

template <typename dataT>
bool DataPage<dataT>::Apply(size_t _index, size_t _tid) {
    stRecord<dataT> * p = dR_ + (_index - sIndex_);
    p->tid = _tid;

    return true;
}


template <typename dataT>
bool DataPage<dataT>::Read(dataT & _d, size_t _index) {

    stRecord<dataT> * p = dR_ + (_index - sIndex_);

    if(!p->use)
        return false;

    memcpy(&_d, &(p->d), sizeof(_d));
    return true;
}

template <typename dataT>
bool DataPage<dataT>::ReadCondition(dataT & _d, bool & _bMyTid, size_t & _tid, bool & _use, size_t _index) {

    stRecord<dataT> * p = dR_ + (_index - sIndex_);

    _bMyTid = p->bMyTid;
    _tid = p->tid;
    _use = p->use;
    memcpy(&_d, &(p->d), sizeof(_d));

    return true;
}

template <typename dataT>
void DataPage<dataT>::Erase(size_t _index, size_t    _tid, bool _bMyTid) {

    stRecord<dataT> * p = dR_ + (_index - sIndex_);
    p->tid = _tid;
    p->bMyTid = _bMyTid;
    p->use = false;
}

template <typename dataT>
class MetaPage {
public:
    explicit MetaPage(std::string _tName);
    ~MetaPage();

    bool Open(std::string _path, size_t _capacity);
    bool Create(std::string _path, size_t _capacity);
    void Clear();

    void IncreaseDataPage();
    void DecreaseDataPage();

    size_t GetMaxIndex() { return mR_->capacity; }
    size_t GetPageCnt() { return mR_->pageCnt; }

    size_t GetPageId(size_t _index) { return _index / mR_->recordCntPerOnePage; }
    size_t GetStartIndex(size_t _id) { return _id * mR_->recordCntPerOnePage; }
    size_t GetEndIndex(size_t _id) { return GetStartIndex(_id+1) - 1; }
    size_t GetDataPageSize() { return mR_->sizePerOnePage; }
    std::string GetDataFname(size_t _pageId);

    size_t GetCapacity() { return mR_->capacity; }
    size_t GetDataCount() { return mR_->use; }
    void SetDataCount(size_t _size) { mR_->use = _size; }
    void AddDataCount() { ++mR_->use; }
    void DelDataCount() { --mR_->use; }

    size_t GenTid() { return ++(mR_->tid); }
    void RollBackTid() { --(mR_->tid); }
    size_t GetTid() { return mR_->tid; }
    void SetTid(size_t _tid) { mR_->tid = _tid; }

    size_t GetReplicatedTid() { return mR_->replicatedTid; }
    void SetReplicatedTid(size_t _tid) { mR_->replicatedTid = _tid; }

private:
    void calcuate(stMetaRecord & _mR, size_t _capacity);

private:
    std::string     tName_;
    size_t          pageSize_;

    std::string     path_;
    std::string     fName_;
    int     fd_;
    stMetaRecord *  mR_;
};

template <typename dataT>
MetaPage<dataT>::MetaPage(std::string _tName) {
    tName_      = _tName;
    pageSize_   = sizeof(stMetaRecord);

    fd_ = -1;
    mR_ = nullptr;
}

template <typename dataT>
MetaPage<dataT>::~MetaPage() {

    if(mR_) {
        munmap(mR_, pageSize_);
        mR_ = nullptr;
    }

    if(fd_ >= 0) {
        close(fd_);
        fd_ = -1;
    }
}

template <typename dataT>
void MetaPage<dataT>::Clear() {
    mR_->use = 0;
    mR_->tid = 0;
    mR_->replicatedTid = 0;
}

template <typename dataT>
bool MetaPage<dataT>::Open(std::string _path, size_t _capacity) {

    path_ = _path;

    // 테이블 네임을 이용하여 fName_ 을 찾습니다.
    // 클래스나 static method 로 빼고 싶은 욕망이...
    fName_ = _path + "/" + tName_ + ".meta";

    if((fd_ = open(fName_.c_str(), O_RDWR, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH)) < 0) {
        // std::cout << "MetaPage::Open() open fail :" << errno << std::endl;
        return false;
    }

    mR_ = (stMetaRecord  *)mmap(nullptr, pageSize_, PROT_READ|PROT_WRITE, MAP_SHARED, fd_, 0);
    if(mR_ == MAP_FAILED) {
        // std::cout << "MetaPage::Open() mmap fail :" << errno << std::endl;

        close(fd_);
        fd_ = -1;

        return false;
    }

    // SIZE 를 줄일면요... 데이터를 옮겨야 해요..? 그렇쵸..
    // 다음 기회에..
    if(mR_->capacity < _capacity)
        calcuate(*mR_, _capacity);

    D_LOG("[DEBUG] [%s] recordSize          [%zu]", tName_.c_str(), mR_->recordSize);
    D_LOG("[DEBUG] [%s] recordCntPerOnePage [%zu]", tName_.c_str(), mR_->recordCntPerOnePage);
    D_LOG("[DEBUG] [%s] sizePerOnePage      [%zu]", tName_.c_str(), mR_->sizePerOnePage);
    D_LOG("[DEBUG] [%s] pageCnt             [%zu]", tName_.c_str(), mR_->pageCnt);

    D_LOG("[DEBUG] [%s] tid                 [%zu]", tName_.c_str(), mR_->tid);
    D_LOG("[DEBUG] [%s] replicatedTid       [%zu]", tName_.c_str(), mR_->replicatedTid);
    D_LOG("[DEBUG] [%s] capacity            [%zu]", tName_.c_str(), mR_->capacity);
    D_LOG("[DEBUG] [%s] use                 [%zu]", tName_.c_str(), mR_->use);

    return true;
}

template <typename dataT>
void MetaPage<dataT>::calcuate(stMetaRecord & _mR, size_t _capacity) {

    // std::cout << "DEBUG : Calucate" << std::endl;
    _mR.recordSize = sizeof(stRecord<dataT>);

    // 1개 page 당 몇개가 들어갈까요?
    _mR.recordCntPerOnePage =
        (nsOneTable::MAX_SIZE_PER_PAGE / _mR.recordSize);

    // 1개의 Page 크기는 몇 일까요?
    _mR.sizePerOnePage =
        _mR.recordSize * _mR.recordCntPerOnePage;

    // 페이지는 총 몇개인가요?
    _mR.pageCnt =
        ( _capacity / _mR.recordCntPerOnePage ) +
        ((_capacity % _mR.recordCntPerOnePage)?1:0);

    // 2020.02.29 - 주석 처리 합니다.
    // Capacity 값을 입력 받은 대로 처리하기 위해서요.
    //_mR.capacity = _mR.recordCntPerOnePage * _mR.pageCnt;
    _mR.capacity = _capacity;

    /*--
    // 만약에, pageCnt 가 1 이고 capcity 가 작으면 그대로 따르리다.
    // 이러면 확장에서 곤란해 져요..
    if(_mR.pageCnt == 1) {
        _mR.recordCntPerOnePage = _capacity;
        _mR.sizePerOnePage = _mR.recordSize * _mR.recordCntPerOnePage;
        _mR.maxPageId = _capacity - 1;
        _mR.capacity = _capacity;
    }
    --*/

}

template <typename dataT>
bool MetaPage<dataT>::Create(std::string _path, size_t _capacity) {

    path_ = _path;

    // 테이블 네임을 이용하여 fName_ 을 찾습니다.
    // 클래스나 static method 로 빼고 싶은 욕망이...
    fName_ = _path + "/" + tName_ + ".meta";

    stMetaRecord    metaR;
    calcuate(metaR, _capacity);
    metaR.use = 0;
    metaR.tid = 0;
    metaR.replicatedTid = 0;

    if((fd_ = open(fName_.c_str(), O_RDWR|O_CREAT|O_TRUNC, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH)) < 0) {
        // std::cout << "MetaPage::() open fail :" << errno << std::endl;
        return false;
    }

    int ret = write(fd_, &metaR, sizeof(metaR));

    if(ret < 0) {
        // std::cout << "MetaPage::Create() write fail :" << errno << std::endl;

        close(fd_);
        return false;
    }

    close(fd_);
    return true;
}

template <typename dataT>
void MetaPage<dataT>::IncreaseDataPage() {
    mR_->pageCnt++;
    mR_->capacity = mR_->recordCntPerOnePage * mR_->pageCnt;
}

template <typename dataT>
void MetaPage<dataT>::DecreaseDataPage() {
    mR_->pageCnt--;
    mR_->capacity = mR_->recordCntPerOnePage * mR_->pageCnt;
}

template <typename dataT>
std::string MetaPage<dataT>::GetDataFname(size_t _pageId) {
    return std::string(path_) + "/" + tName_ + "." + std::to_string(_pageId);
}

template <typename dataT>
class Repository {
public:
    explicit Repository(std::string _tName, std::string _path);
    ~Repository();

    bool Init(size_t _capacity);
    void Clear();

    size_t Increase();
    size_t Decrease();

    bool Write(size_t _index, const dataT & _d, bool _bUpdate=false);
    bool Read(dataT & _d, size_t _index);
    bool ReadValue(std::string & _value, size_t _index, off_t _offset, size_t _size, bool _bStringKeyType);
    bool Update(size_t _index, const dataT & _d) { return Write(_index, _d, true); }
    bool Erase(size_t _index);
    // bool DirectTidApply(size_t _index, size_t _tid);
    bool DirectFetch(size_t _index, size_t & _tid, bool & _bMyTid, bool & _use, dataT & _d);
    bool Swap(size_t _aIndex, size_t _bIndex);

    size_t GetDataCount() { return metaPage_.GetDataCount(); }
    size_t GetCapacity() { return metaPage_.GetCapacity(); }
    size_t GetTid() { return metaPage_.GetTid(); }
    void SetTid(size_t _tid) { metaPage_.SetTid(_tid); }

    bool WriteByRepl(size_t _index, size_t _tid, bool _bMyTid, const dataT & _d);
    bool EraseByRepl(size_t _index, size_t _tid, bool _bMyTid);
    size_t GetReplicatedTid() { return metaPage_.GetReplicatedTid(); }
    void SetReplicatedTid(size_t _tid) { metaPage_.SetReplicatedTid(_tid); }

    void SetDataCount(size_t _cnt) { metaPage_.SetDataCount(_cnt); }
private:
    void drop();
    bool appendDataPage(int _id);
    bool removeDataPage();

private:
    std::string path_;

    MetaPage<dataT>     metaPage_;
    std::vector<DataPage<dataT> * >     vDataPages_;
};

template <typename dataT>
Repository<dataT>::Repository(std::string _tName, std::string _path) :
        path_(_path),
        metaPage_(_tName) {

}

template <typename dataT>
Repository<dataT>::~Repository() {
    drop();
}

template <typename dataT>
bool Repository<dataT>::Init(size_t _capacity) {

    struct stat     stBuf;
    if(lstat(path_.c_str(), &stBuf) != 0 || !S_ISDIR(stBuf.st_mode)) {
        // std::cout << "MetaPage::lstat() fail :" << errno << std::endl;
        return false;
    }

    if(metaPage_.Open(path_, _capacity) == false) {
        if(metaPage_.Create(path_, _capacity) == false) {
            // std::cout << "Repository::Init() can't create meta page" << std::endl;
            return false;
        }

        if(metaPage_.Open(path_, _capacity) == false) {
            // std::cout << "Repository::Init() can't reopen meta page" << std::endl;
            return false;
        }
    }

    for(size_t nLoop=0; nLoop < metaPage_.GetPageCnt(); nLoop++) {
        if(appendDataPage(nLoop) == false) {
            // std::cout << "Repository::appendDataPage() fail" << std::endl;
            return false;
        }
    }

    //std::cout << "[DEBUG] Repository::Init() Tid:" << metaPage_.GetTid() << std::endl;
    //std::cout << "[DEBUG] Repository::Init() Repl Tid:" << metaPage_.GetReplicatedTid() << std::endl;

    return true;
}

template <typename dataT>
void Repository<dataT>::Clear() {

    for(auto i : vDataPages_)
        i->Clear();

    metaPage_.Clear();
}

template <typename dataT>
void Repository<dataT>::drop() {

    for(auto i : vDataPages_) {
        if(i)
            delete i;
    }

    vDataPages_.clear();
}

template <typename dataT>
bool Repository<dataT>::appendDataPage(int _id) {

    DataPage<dataT> * dPage = new (std::nothrow) DataPage<dataT>(_id, metaPage_);
    if(!dPage) {
        // std::cout << "Repository::Init() can't new page" << std::endl;
        return false;
    }

    if(dPage->Open() == false) {
        if(dPage->Create() == false) {
            // std::cout << "Repository::Init() can't create data page" << std::endl;
            return false;
        }

        if(dPage->Open() == false) {
            // std::cout << "Repository::Init() can't reopen data page" << std::endl;
            return false;
        }

        dPage->Init();
    }

    vDataPages_.push_back(dPage);
    // std::cout << "------- appendPage OK:" << _id << std::endl;

    return true;
}

template <typename dataT>
bool Repository<dataT>::removeDataPage() {

    DataPage<dataT> * dPage = vDataPages_.back();
    delete dPage;
    vDataPages_.pop_back();

    // std::cout << "------- remove Data Page OK:" << _id << std::endl;

    return true;
}



template <typename dataT>
size_t Repository<dataT>::Increase() {
    int pageNo = metaPage_.GetPageCnt();
    if(appendDataPage(pageNo) == false)
        return 0;

    metaPage_.IncreaseDataPage();
    return GetCapacity();
}

template <typename dataT>
size_t Repository<dataT>::Decrease() {
    if(removeDataPage() == false)
        return 0;

    metaPage_.DecreaseDataPage();
    return GetCapacity();
}

template <typename dataT>
bool Repository<dataT>::Write(size_t _index, const dataT & _d, bool _bUpdate) {

    if(_index >= metaPage_.GetMaxIndex()) {
        // std::cout << "DEBUG: Write() GetMaxIndex()" << std::endl;
        return false;
    }

    size_t pageId = metaPage_.GetPageId(_index);

    DataPage<dataT> * dPage = vDataPages_.at(pageId);
    if(!dPage) {
        // 확장 flag 가 켜져 있다면, 확장 합니다.
        // std::cout << "DEBUG: Write() dPage NULL" << std::endl;
        return false;
    }

    if(dPage->Write(_index, _d, metaPage_.GenTid(), true, _bUpdate)) {
        if(!_bUpdate)
            metaPage_.AddDataCount();

        return true;
    } else {
        metaPage_.RollBackTid();
    }

    return false;
}

template <typename dataT>
bool Repository<dataT>::WriteByRepl(size_t _index, size_t _tid, bool _bMyTid, const dataT & _d) {

    if(_index >= metaPage_.GetMaxIndex()) {
        // std::cout << "DEBUG: Write() GetMaxIndex()" << std::endl;
        return false;
    }

    size_t pageId = metaPage_.GetPageId(_index);

    DataPage<dataT> * dPage = vDataPages_.at(pageId);
    if(!dPage) {
        // 확장 flag 가 켜져 있다면, 확장 합니다.
        // std::cout << "DEBUG: Write() dPage NULL" << std::endl;
        return false;
    }

    if(dPage->Write(_index, _d, _tid, _bMyTid, false)) {
        metaPage_.AddDataCount();
        metaPage_.SetReplicatedTid(_tid);

        return true;
    }

    return false;
}

template <typename dataT>
bool Repository<dataT>::Read(dataT & _d, size_t _index) {

    if(_index >= metaPage_.GetMaxIndex())
        return false;

    size_t pageId = metaPage_.GetPageId(_index);
    DataPage<dataT> * dPage = vDataPages_.at(pageId);
    if(dPage == nullptr)
        return false;

    return dPage->Read(_d, _index);
}

template <typename dataT>
bool Repository<dataT>::ReadValue(std::string & _value,
                                    size_t _index,
                                    off_t _offset,
                                    size_t _size,
                                    bool _bStringKeyType) {

    dataT   d;
    if(Read(d, _index) == false)
        return false;

    if(_bStringKeyType)
        _value = std::string((const char *)&d + _offset);
    else
        _value = std::string((const char *)&d + _offset, _size);

    return true;
}


template <typename dataT>
bool Repository<dataT>::Erase(size_t _index) {

    if(_index >= metaPage_.GetMaxIndex())
        return false;

    size_t pageId = metaPage_.GetPageId(_index);
    DataPage<dataT> * dPage = vDataPages_.at(pageId);

    if(!dPage)
        return false;

    dPage->Erase(_index, metaPage_.GenTid());
    metaPage_.DelDataCount();

    return true;
}

template <typename dataT>
bool Repository<dataT>::EraseByRepl(size_t _index, size_t _tid, bool _bMyTid) {

    if(_index >= metaPage_.GetMaxIndex())
        return false;

    size_t pageId = metaPage_.GetPageId(_index);
    DataPage<dataT> * dPage = vDataPages_.at(pageId);

    if(!dPage)
        return false;

    dPage->Erase(_index, _tid, _bMyTid);
    metaPage_.DelDataCount();

    metaPage_.SetReplicatedTid(_tid);

    return true;
}

template <typename dataT>
bool Repository<dataT>::DirectFetch(size_t _index, size_t & _tid, bool & _bMyTid,  bool & _use, dataT & _d) {

    if(_index >= metaPage_.GetMaxIndex())
        return false;

    size_t pageId = metaPage_.GetPageId(_index);
    DataPage<dataT> * dPage = vDataPages_.at(pageId);

    if(!dPage)
        return false;

    return dPage->ReadCondition(_d, _bMyTid, _tid, _use, _index);
}

template <typename dataT>
bool Repository<dataT>::Swap(size_t _aIndex, size_t _bIndex) {

    if(_aIndex >= metaPage_.GetMaxIndex())
        return false;

    size_t aPageId = metaPage_.GetPageId(_aIndex);
    DataPage<dataT> * aPage = vDataPages_.at(aPageId);

    if(!aPage)
        return false;

    if(_bIndex >= metaPage_.GetMaxIndex())
        return false;

    size_t bPageId = metaPage_.GetPageId(_bIndex);
    DataPage<dataT> * bPage = vDataPages_.at(bPageId);

    if(!bPage)
        return false;

    aPage->Swap(_aIndex, bPage, _bIndex);
    return true;
}

/*-
template <typename dataT>
bool Repository<dataT>::DirectTidApply(size_t _index, size_t _tid) {

    if(_index >= metaPage_.GetMaxIndex())
        return false;

    size_t pageId = metaPage_.GetPageId(_index);
    DataPage<dataT> * dPage = vDataPages_.at(pageId);

    if(!dPage)
        return false;

    return dPage->Apply(_index, _tid);
}

template <typename dataT>
bool Repository<dataT>::DirectFetch(size_t _index, size_t & _tid, bool & _use, dataT & _d) {

    if(_index >= metaPage_.GetMaxIndex())
        return false;

    size_t pageId = metaPage_.GetPageId(_index);
    DataPage<dataT> * dPage = vDataPages_.at(pageId);

    if(!dPage)
        return false;

    return dPage->ReadCondition(_d, _tid, _use, _index);
}
-*/

} // namespace

#endif // OT_REPOSITORY_HPP
