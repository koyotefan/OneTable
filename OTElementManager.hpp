/*
 * **************************************************************************
 *
 * OTElementManager.hpp
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

#ifndef OT_ELEMENT_MANAGER_HPP
#define OT_ELEMENT_MANAGER_HPP

#include <arpa/inet.h>
#include <algorithm>

#include "OTDefine.hpp"
#include "OTElement.hpp"

namespace nsOneTable {

template <typename dataT>
class PKManager {
public:
    explicit PKManager(off_t _offset,
                       size_t _keySize,
                       nsOneTable::KeyType _kType);
    ~PKManager();
    void Clear();
    PKManager * Clone();

    bool Init(size_t _capacity, size_t _sKeyCnt);
    bool IsExist(const std::string & _key);
    PKElement * Alloc(size_t _index, const std::string & _key);
    PKElement * Alloc(const std::string & _key);
    PKElement * Get(const std::string & _key);
    void Release(const std::string & _key, PKElement * _pke);

    std::string ExtractKey(const dataT & _d);
    std::string ExtractKey(const char * _pKey);

    off_t GetKeyOffset() { return offset_; }
    size_t GetKeySize() { return keySize_; }
    bool IsStringKeyType() { return bStringKeyType_; }

private:

    off_t   offset_;
    size_t  keySize_;
    bool    bStringKeyType_;


    std::unordered_map<std::string, PKElement *>    kmap_;
    ElementFactory<PKElement>  eFactory_;
};

template <typename dataT>
PKManager<dataT>::PKManager(off_t _offset,
                            size_t _keySize,
                            nsOneTable::KeyType _kType) {
    offset_ = _offset;
    keySize_ = _keySize;
    bStringKeyType_ = (_kType == nsOneTable::KeyType::STR);
}

template <typename dataT>
PKManager<dataT>::~PKManager() {
    Clear();
    // eFactory_ 는 알아서 하기를...
}

template <typename dataT>
void PKManager<dataT>::Clear() {
    for(auto & i : kmap_) {
        if(i.second)
            eFactory_.Release(i.second);
    }

    kmap_.clear();
}

template <typename dataT>
PKManager<dataT> * PKManager<dataT>::Clone() {
    return new PKManager<dataT>(offset_,
                                keySize_,
                                (bStringKeyType_)?nsOneTable::KeyType::STR:nsOneTable::KeyType::INT);
}

template <typename dataT>
bool PKManager<dataT>::Init(size_t _capacity, size_t _sKeyCnt) {
    // std::cout << "--------- PKManager::Init() capacity:" << _capacity << " keycnt:" << _sKeyCnt << std::endl;
    return eFactory_.Init(_capacity, _sKeyCnt);
}

template <typename dataT>
bool PKManager<dataT>::IsExist(const std::string & _key) {
    return kmap_.find(_key) != kmap_.end();
}

template <typename dataT>
PKElement * PKManager<dataT>::Alloc(size_t _index, const std::string & _key) {
    PKElement * pke = eFactory_.Alloc(_index);

    if(pke)
        kmap_.emplace(_key, pke);

    return pke;
}

template <typename dataT>
PKElement * PKManager<dataT>::Alloc(const std::string & _key) {
    PKElement * pke = eFactory_.Alloc();

    if(pke)
        kmap_.emplace(_key, pke);

    return pke;
}


template <typename dataT>
PKElement * PKManager<dataT>::Get(const std::string & _key) {
    auto iter = kmap_.find(_key);

    if(iter == kmap_.end())
        return (PKElement *)nullptr;

    return iter->second;
}



template <typename dataT>
void PKManager<dataT>::Release(const std::string & _key, PKElement * _pke) {

    eFactory_.Release(_pke);

    // // std::cout << " map size :" << kmap_.size();
    kmap_.erase(_key);
    // // std::cout << " # map size :" << kmap_.size() << std::endl;
}

template<typename dataT>
std::string PKManager<dataT>::ExtractKey(const dataT & _d) {
    return ExtractKey((const char *)&_d + offset_);
}

template<typename dataT>
std::string PKManager<dataT>::ExtractKey(const char * _pKey) {
    return bStringKeyType_?
            std::string(_pKey):
            std::string(_pKey, keySize_);
}

template <typename dataT>
class SKManager {
public:
    explicit SKManager(off_t _offset,
                        size_t _keySize,
                        nsOneTable::KeyType _kType);
    ~SKManager();

    void Clear();
    SKManager<dataT> * Clone();

    bool Init(size_t _capacity);
    SKElement * Alloc(int _order, PKElement * _pke, const dataT & _d);
    SKElement * Get(const dataT & _d);
    SKElement * Get(const std::string & _sKey);
    SKElement * Get();
    SKElement * Get(int _order);
    void Release(SKElement * _ske);
    std::string ExtractKey(const dataT & _d);
    std::string ExtractKey(const char * _sKey, size_t _sKeySize);

    size_t GetKeySize() { return keySize_; }
    size_t GetElementCount(const std::string & _sKey);

private:
    off_t   offset_;
    size_t  keySize_;
    bool    bStringKeyType_;
    bool    bIntKeyType_;
    bool    bLittleEndian_;

    std::map<std::string, SKHook *>    kmap_;
    ElementFactory<SKElement>   eFactory_;

};

template <typename dataT>
SKManager<dataT>::SKManager(off_t _offset,
                            size_t _keySize,
                            nsOneTable::KeyType _kType) {
    offset_ = _offset;
    keySize_ = _keySize;
    bStringKeyType_ = (_kType == nsOneTable::KeyType::STR);
    bIntKeyType_ = (_kType == nsOneTable::KeyType::INT);
    bLittleEndian_ = ( 1 != htonl(1));
}

template <typename dataT>
SKManager<dataT>::~SKManager() {
    Clear();
    // eFactory 는 알아서..
}

template <typename dataT>
void SKManager<dataT>::Clear() {

    // std::cout << "SKManager::Clear() map:" << kmap_.size() << std::endl;

    for(auto & i : kmap_) {
        if(i.second == nullptr)
            continue;

        SKHook * hook = i.second;
        SKElement * e = nullptr;
        while(true) {
            // // std::cout << "SKManager::Clear hook use:" << hook->use;
            hook->PopFront(&e);
            if(!e)
                break;
            eFactory_.Release(e);
        }

        delete hook;
    }

    kmap_.clear();
}

template <typename dataT>
SKManager<dataT> * SKManager<dataT>::Clone() {
    return new SKManager<dataT>(offset_,
                                keySize_,
                                (bStringKeyType_)?nsOneTable::KeyType::STR:nsOneTable::KeyType::INT);
}

template <typename dataT>
bool SKManager<dataT>::Init(size_t _capacity) {
    return eFactory_.Init(_capacity);
}

template <typename dataT>
SKElement * SKManager<dataT>::Alloc(int _order, PKElement * _pke, const dataT & _d) {
    SKElement * ske = eFactory_.Alloc();

    if(!ske)
        return ske;

    std::string sk = ExtractKey((const char *)&_d + offset_, keySize_);
    auto i = kmap_.find(sk);

    SKHook * hook = nullptr;
    if(i == kmap_.end()) {
        hook = new SKHook(sk, ske);
        kmap_.emplace(sk, hook);
    }
    else {
        hook = i->second;
        hook->PushBack(ske);
        // // std::cout << "* SKManager::Alloc ske :" << (void *)ske << std::endl;
    }

    ske->pke = _pke;
    ske->skh = hook;

    _pke->arrSKe[_order] = ske;
    return ske;
}

template <typename dataT>
SKElement * SKManager<dataT>::Get(const dataT & _d) {
    return Get(ExtractKey(_d));
}

template <typename dataT>
SKElement * SKManager<dataT>::Get() {
    auto iter = kmap_.begin();

    if(iter == kmap_.end())
        return (SKElement *)nullptr;

    SKHook * hook = iter->second;

    if(!hook)
        return (SKElement *)nullptr;

    return hook->PeekFront();
}

template <typename dataT>
SKElement * SKManager<dataT>::Get(int _order) {
    auto iter = kmap_.begin();

    if(iter == kmap_.end())
        return (SKElement *)nullptr;

    SKHook * hook = iter->second;

    if(!hook)
        return (SKElement *)nullptr;

    return hook->Peek(_order);
}

template <typename dataT>
SKElement * SKManager<dataT>::Get(const std::string & _sKey) {

    auto iter = kmap_.find(_sKey);

    if(iter == kmap_.end())
        return (SKElement *)nullptr;

    SKHook * hook = iter->second;

    if(!hook)
        return (SKElement *)nullptr;

    return hook->PeekFront();
}

template <typename dataT>
void SKManager<dataT>::Release(SKElement * _ske) {
    if(_ske == nullptr)
        return ;

    SKHook * hook = _ske->skh;

    if(!hook)
        return ;

    hook->Release(_ske);
    if(hook->use == 0) {
        kmap_.erase(hook->kVal);
        delete hook;
    }

    eFactory_.Release(_ske);
}

template<typename dataT>
std::string SKManager<dataT>::ExtractKey(const dataT & _d) {
    return ExtractKey((const char *)&_d + offset_, keySize_);
}

template<typename dataT>
std::string SKManager<dataT>::ExtractKey(const char * _sKey, size_t _sKeySize) {

    if(bStringKeyType_)
        return std::string(_sKey);

    std::string str(_sKey, _sKeySize);

    if(bIntKeyType_ && bLittleEndian_)
        std::reverse(std::begin(str), std::end(str));

    return str;

    /*-
    if(bIntKeyType_) {
        std::string str(_sKey, _sKeySize);
        if(bLittleEndian_)
            std::reverse(std::begin(str), std::end(str));
        return str;
    }

    return std::string(_sKey, _sKeySize);
    -*/

    /*-
    return bStringKeyType_?
        std::string(_sKey):
        std::string(_sKey, _sKeySize);
    -*/
}

template <typename dataT>
size_t SKManager<dataT>::GetElementCount(const std::string & _sKey) {

    auto iter = kmap_.find(_sKey);

    if(iter == kmap_.end())
        return 0;

    SKHook * hook = iter->second;

    if(!hook)
        return 0;

    return hook->use;
}

}

#endif // OT_ELEMENT_MANAGER_HPP
