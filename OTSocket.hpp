/*
 * **************************************************************************
 *
 * OTSocket.hpp
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

#ifndef OT_SOCKET_HPP
#define OT_SOCKET_HPP

#include <iostream>
#include <string>
#include <poll.h>

#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>

///////////////////////////////////////////////////////////////////////////////
// CAppTimer //////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

class CAppTimer {
public:
    explicit CAppTimer() { period_ = 30; lastT_ = 0; }
    ~CAppTimer() {}

    void Init(int _period) { period_ = _period; }
    bool IsTimeOut(time_t _t = time(nullptr)) { return _t >= (lastT_ + period_); }
    void Update(time_t _t = time(nullptr)) { lastT_ = _t; }
    void Reset() { lastT_ = 0; }

private:
    int     period_;
    time_t  lastT_;
};

///////////////////////////////////////////////////////////////////////////////
// OTTCPActiveSocket //////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

class OTTCPActiveSocket {
public:
    explicit OTTCPActiveSocket();
    virtual ~OTTCPActiveSocket();

    const char * GetConnectAddress() { return addr_.c_str(); }
    void SetWait(int _milliseconds);
    void SetLinger();
    void Close();
    void Init(int _fd, struct sockaddr * _addr, socklen_t _addrlen);
    bool Send(char * _ptr, size_t _size);
    int Recv(char * _ptr, size_t _size);

    bool IsConnected() { return fd_ >= 0; }
    int GetSocket() { return fd_; }
    const char * GetError() { return strError_.c_str(); }

protected:
    int             fd_;
    int             milliSeconds_;

    std::string     addr_;
    struct pollfd   fds_;

    std::string     strError_;

};

///////////////////////////////////////////////////////////////////////////////
// OTTCPAccepter //////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

class OTTCPAccepter : public OTTCPActiveSocket {
public:
    explicit OTTCPAccepter() {}
    ~OTTCPAccepter() {}

    void Init(int _fd, struct sockaddr_in * _addr);
};


///////////////////////////////////////////////////////////////////////////////
// OTTCPClient ////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

class OTTCPClient : public OTTCPActiveSocket {
public:
    explicit OTTCPClient() {}
    ~OTTCPClient() {}

    bool Connect(std::string _ip, int _port);
    bool ConnectDualVersion(std::string _ip, int _port);

};


// Server
class OTTCPServer {
public:
    explicit OTTCPServer();
    ~OTTCPServer();

    bool Create(int _port);
    bool CreateV6(int _port);
    void SetWait(int _milliseconds);
    int  WaitClient(OTTCPAccepter ** _accepter);
    void Close();

private:
    bool ready(struct sockaddr * _addr, socklen_t _addrlen);

private:
    int     fd_;
    int     milliSeconds_;
    struct  pollfd  fds_;

};


#endif // OT_SOCKET_HPP
