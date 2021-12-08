/*
 * **************************************************************************
 *
 * OTElement.hpp
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

#ifndef OT_ELEMENT_HPP
#define OT_ELEMENT_HPP

#include "OTDefine.hpp"

namespace nsOneTable {

struct Element {
    Element  * prev;
    Element  * next;
    const size_t  id;

    explicit Element(size_t _index);
    virtual ~Element() {}

    void SetPrev(Element * _e);
    void SetNext(Element * _e);
    void ConnectBothEnds();
};

inline Element::Element(size_t _index) :
    prev(nullptr),
    next(nullptr),
    id(_index) {
}

inline void Element::SetPrev(Element * _e) {

    if(prev)
        prev->next = _e;
    _e->prev = prev;

    prev = _e;
    _e->next = this;
}

inline void Element::SetNext(Element * _e) {

    if(next)
        next->prev = _e;
    _e->next = next;

    next = _e;
    _e->prev = this;
}

inline void Element::ConnectBothEnds() {
    if(prev)
        prev->next = next;

    if(next)
        next->prev = prev;

    prev = nullptr;
    next = nullptr;
}

struct SKHook;
struct PKElement;

struct SKElement : public Element {
    SKHook * skh;
    PKElement * pke;

    explicit SKElement(size_t _index=0);
    ~SKElement();
    void Clean();
};

inline SKElement::SKElement(size_t _index)
    : Element(_index),
      skh(nullptr),
      pke(nullptr) {

}

inline SKElement::~SKElement() {
    skh = nullptr;
    pke = nullptr;
}

inline void SKElement::Clean() {
    skh = nullptr;
    pke = nullptr;
}

struct PKElement : public Element {

    // Multiple key's count
    int sKeyCnt;
    SKElement ** arrSKe;

    explicit PKElement(size_t _index, size_t _sKeyCnt=0);
    ~PKElement();
    void Clean();
};

inline PKElement::PKElement(size_t _index, size_t _sKeyCnt)
    : Element(_index),
      sKeyCnt(_sKeyCnt) {

    if(sKeyCnt > 0) {
        arrSKe = new SKElement * [sKeyCnt];
        for(int nLoop=0; nLoop < sKeyCnt; nLoop++)
            arrSKe[nLoop] = nullptr;
    }
    else
        arrSKe = nullptr;
}

inline PKElement::~PKElement() {
    if(arrSKe) {
        delete [] arrSKe;
        arrSKe = nullptr;
    }
}

inline void PKElement::Clean() {
    // std::cout << "---- arrSKe :" << (void *)arrSKe << std::endl;
    for(int nLoop=0; nLoop < sKeyCnt; nLoop++)
        arrSKe[nLoop] = nullptr;
}

struct SKHook {
    SKElement * firstE;
    SKElement * lastE;

    std::string     kVal;
    size_t  use;

    explicit SKHook(std::string & _kVal, SKElement * _e);
    ~SKHook();

    void PushBack(SKElement * _e);
    void PopFront(SKElement ** _e);
    SKElement * PeekFront() { return firstE; }
    SKElement * Peek(int _order);
    void Release(SKElement * _e);
};

inline SKHook::SKHook(std::string & _kVal, SKElement * _e) :
    firstE(_e),
    lastE(_e),
    kVal(_kVal),
    use(1) {

}

inline SKHook::~SKHook() {

    if(use == 0)
        return ;

    // 원래는 밖에서 클리어 하고, Element 를 반환해야 합니다.
    // 따라서 여기에 오면 안되요..
    SKElement * p = firstE;
    SKElement * next = nullptr;

    while(p) {
        next = dynamic_cast<SKElement *>(p->next);
        delete p;
        p = next;
        use--;
    }
}

inline void SKHook::PushBack(SKElement * _e) {
    lastE->SetNext(_e);
    lastE = _e;
    use++;
}

inline void SKHook::PopFront(SKElement ** _e) {
    if(firstE == nullptr) {
        *_e = nullptr;
        return ;
    }

    *_e = firstE;
    Release(*_e);
}

inline void SKHook::Release(SKElement * _e) {
    if(!_e)
        return ;

    if(_e == firstE)
        firstE = dynamic_cast<SKElement *>(_e->next);

    if(_e == lastE)
        lastE = dynamic_cast<SKElement *>(_e->prev);

    _e->ConnectBothEnds();
    use--;
}

inline SKElement * SKHook::Peek(int _order) {
    Element * ptr = firstE;

    for(int nLoop=0; nLoop < _order; ++nLoop) {
        if(ptr == nullptr)
            break;
        ptr = ptr->next;
    }

    return (SKElement *)ptr;
}

template <typename eT>
class ElementFactory {
public:
    explicit ElementFactory();
    ~ElementFactory();

    bool    Init(size_t _capacity, size_t _sKeyCnt);
    bool    Init(size_t _capacity);
    size_t  Capacity() { return capacity_; }
    size_t  Size() { return size_; }

    eT * Alloc();
    eT * Alloc(size_t _index);
    void    Release(eT * _e);

private:
    void    clear();
    eT *    getLastLinkElement(Element * _pE);
    bool    appendElement(size_t _from, size_t _to, size_t _sKeyCnt);
    bool    appendElement(size_t _from, size_t _to);
    bool    removeElement(size_t _from, size_t _to);

private:
    size_t  capacity_;
    size_t  size_;

    eT * firstEmptyElement_;
};

template <typename eT>
ElementFactory<eT>::ElementFactory() {
    capacity_ = 0;
    size_ = 0;
    firstEmptyElement_ = nullptr;
}

template <typename eT>
eT * ElementFactory<eT>::getLastLinkElement(Element * _pE) {
    if(_pE == nullptr)
        return static_cast<eT *>(_pE);

    while(_pE) {
        if(_pE->next == nullptr)
            break;

        _pE = _pE->next;
    }

    return static_cast<eT *>(_pE);
}

template <typename eT>
bool ElementFactory<eT>::Init(size_t _capacity, size_t _sKeyCnt) {

    size_t now = capacity_;
    size_t wanted = _capacity;

    if(wanted == now)
        return true;

    if(wanted > now)
        return appendElement(now, wanted, _sKeyCnt);
    else
        return removeElement(now, wanted);
}

template <typename eT>
bool ElementFactory<eT>::Init(size_t _capacity) {

    size_t now = capacity_;
    size_t wanted = _capacity;

    if(wanted == now)
        return true;

    if(wanted > now)
        return appendElement(now, wanted);
    else
        return removeElement(now, wanted);
}

template <typename eT>
bool ElementFactory<eT>::appendElement(size_t _from, size_t _to, size_t _sKeyCnt) {

    int index = static_cast<int>(_from);
    int to = static_cast<int>(_to);

    // 정상이라면, appendElement 가 불리우는 시점에는 firstEmptyElement_ 가 nullptr 입니다.
    eT * lastE = getLastLinkElement(firstEmptyElement_);

    while(to > index) {

        eT * e = new (std::nothrow) eT(index, _sKeyCnt);
        if(!e)
            return false;

        if(lastE)
            lastE->SetNext(e);
        else
            firstEmptyElement_ = e;

        lastE = e;
        index++;
        capacity_ = index;

        // std::cout << " # - Factory::Init firstE :" << (void *)firstEmptyElement_;
        // std::cout << " id: " << firstEmptyElement_->id ;
        // std::cout << " prev:" << (void *)(firstEmptyElement_->prev) << std::endl;
    }

    // std::cout << " #2 - Factory::Init Capa:" << capacity_ << std::endl;
    return true;
}

template <typename eT>
bool ElementFactory<eT>::appendElement(size_t _from, size_t _to) {

    int index = static_cast<int>(_from);
    int to = static_cast<int>(_to);

    eT * lastE = getLastLinkElement(firstEmptyElement_);

    while(to > index) {

        eT * e = new (std::nothrow) eT(index);
        if(!e)
            return false;

        if(lastE)
            lastE->SetNext(e);
        else
            firstEmptyElement_ = e;

        lastE = e;
        index++;
        capacity_ = index;

        // // std::cout << " # - Factory::Init firstE :" << (void *)firstEmptyElement_;
        // // std::cout << " id: " << firstEmptyElement_->id ;
        // // std::cout << " prev:" << (void *)(firstEmptyElement_->prev) << std::endl;
    }

    // std::cout << " #2 - Factory::appendElement Capa:" << capacity_ << std::endl;
    return true;
}

template <typename eT>
bool ElementFactory<eT>::removeElement(size_t _from, size_t _to) {

    // std::cout << "------- ElementFactory::removeElement from: " << _from << " to:" << _to << std::endl;
    eT * pE = firstEmptyElement_;
    eT * pNext = nullptr;

    while(pE) {
        pNext = static_cast<eT *>(pE->next);

        if(pE->id >= _to && pE->id < _from) {
            pE->ConnectBothEnds();

            if(pE == firstEmptyElement_)
                firstEmptyElement_ = pNext;

            delete pE;
            capacity_--;
        }

        pE = pNext;
    }

    // std::cout << " #3 - Factory::removeElement Capa:" << capacity_ << std::endl;
    return true;
}


template <typename eT>
ElementFactory<eT>::~ElementFactory() {
    clear();
}

template <typename eT>
void ElementFactory<eT>::clear() {
    // // std::cout << "-------------------" << std::endl;

    // // std::cout << "# clear - start pE :" << (void *)firstEmptyElement_ ;
    // if(firstEmptyElement_)
    //     // std::cout << " next :" << (void *)firstEmptyElement_->next;
    // // std::cout << std::endl;

    Element * pE = firstEmptyElement_;
    Element * pNext = nullptr;

    while(pE != nullptr) {
        // // std::cout << "# clear pE :" << (void *)pE << " next :" << (void *)pE->next << std::endl;
        pNext = pE->next;
        delete pE;
        pE = pNext;

        capacity_--;
    }

    firstEmptyElement_ = nullptr;
}


template <typename eT>
eT * ElementFactory<eT>::Alloc() {
    if(firstEmptyElement_ == nullptr)
        return firstEmptyElement_;

    eT * e = firstEmptyElement_;

    firstEmptyElement_ = dynamic_cast<eT *>(e->next);
    e->ConnectBothEnds();

    e->Clean();
    size_++;

    // // std::cout << " # - Factory::Alloc firstE :" << (void *)firstEmptyElement_;
    // if(firstEmptyElement_) {
    //   // std::cout << " id: " << firstEmptyElement_->id ;
    //   // std::cout << " next:" << (void *)(firstEmptyElement_->next);
    //   // std::cout << " prev:" << (void *)(firstEmptyElement_->prev);
    //}
    //// std::cout << std::endl;

    return e;
}

template <typename eT>
eT * ElementFactory<eT>::Alloc(size_t _index) {
    if(firstEmptyElement_ == nullptr)
        return firstEmptyElement_;

    Element * pE = firstEmptyElement_;
    // I_LOG("-OTElement::Alloc() index [%zu] E id[%zu]", _index, pE->id);

    while(pE) {
        if(pE->id == _index)
            break;
        pE = pE->next;
    }

    if(pE == nullptr)
        return (eT *)nullptr;

    eT * e =  dynamic_cast<eT *>(pE);

    if(e == firstEmptyElement_)
        firstEmptyElement_ = dynamic_cast<eT *>(e->next);

    e->ConnectBothEnds();

    e->Clean();
    size_++;

    return e;
}

template <typename eT>
void ElementFactory<eT>::Release(eT * _e) {
    if(!_e)
        return ;

    _e->ConnectBothEnds();
    // // std::cout << " # - Factory::Release WorkE :" << (void *)_e;
    //    // std::cout << " next:" << (void *)(_e->next);
    //    // std::cout << " prev:" << (void *)(_e->prev);
    //// std::cout << std::endl;

    if(firstEmptyElement_)
        firstEmptyElement_->SetPrev(_e);

    firstEmptyElement_ = _e;
    size_--;

    // // std::cout << " # - Factory::Release firstE :" << (void *)firstEmptyElement_;
    // if(firstEmptyElement_) {
    //    // std::cout << " id: " << firstEmptyElement_->id ;
    //    // std::cout << " next:" << (void *)(firstEmptyElement_->next);
    //    // std::cout << " prev:" << (void *)(firstEmptyElement_->prev);
    // }
    // // std::cout << std::endl;
}

}

#endif // OT_ELEMENT_HPP
