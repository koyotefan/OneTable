/*
 * **************************************************************************
 *
 * OTSocket.cpp
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

#include <unistd.h>
#include <cstring>
#include <netinet/tcp.h>

#include "OTSocket.hpp"

OTTCPActiveSocket::OTTCPActiveSocket()
    : fd_(-1),
      milliSeconds_(1000) {

    memset(&fds_, 0, sizeof(fds_));
}

OTTCPActiveSocket::~OTTCPActiveSocket() {
    Close();
}

void OTTCPActiveSocket::Close() {
    if(fd_ >= 0) {
        close(fd_);
        fd_ = -1;
    }

    addr_ = "";
    memset(&fds_, 0, sizeof(fds_));
}

void OTTCPActiveSocket::Init(int _fd, struct sockaddr * _addr, socklen_t _addrlen) {
    fd_ = _fd;

    int     nRet = 0;
    char    hostname[256];
    char    svcname[64];
    if((nRet = getnameinfo(_addr,
                    _addrlen,
                    hostname,
                    sizeof(hostname),
                    svcname,
                    sizeof(svcname),
                    NI_NAMEREQD | NI_NUMERICHOST | NI_NUMERICSERV)) != 0) {
        // W_LOG
        // printf("getnameinfo() - fail [%d]\n", nRet);

    } else {
        addr_ = hostname;
    }

    // option
    struct timeval  to;
    to.tv_sec = 3;
    to.tv_usec = 0;

    if(setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &to, sizeof(to)) < 0) {
        // char buf[128];
        // printf("setsockopt() - SO_RCVTIMEO fail [%d:%s]\n",
        //    errno, strerror_r(errno, buf, sizeof(buf)));
    }

    if(setsockopt(fd_, SOL_SOCKET, SO_SNDTIMEO, &to, sizeof(to)) < 0) {
        // char buf[128];
        // printf("setsockopt() - SO_RCVTIMEO fail [%d:%s]\n",
        //    errno, strerror_r(errno, buf, sizeof(buf)));
    }

    int on = 1;

    if(setsockopt(fd_, SOL_SOCKET, SO_KEEPALIVE, &on, sizeof(on)) < 0) {
        // std::cout << "[setKeepAlive] SO_KEEPALIVE fail : " << strerror(errno) << std::endl;
        // return ;
    }

    int idle = 5; // 5 sec 

    if(setsockopt(fd_, SOL_TCP, TCP_KEEPIDLE, &idle, sizeof(idle)) < 0) {
        // std::cout << "[setKeepAlive] TCP_KEEPIDLE fail : " << strerror(errno) << std::endl;
        // return ;
    }

    int cnt = 3;

    if(setsockopt(fd_, SOL_TCP, TCP_KEEPCNT, &cnt, sizeof(cnt)) < 0) {
        // std::cout << "[setKeepAlive] TCP_KEEPCNT fail : " << strerror(errno) << std::endl;
        // return ;
    }

    int interval = 5;

    if(setsockopt(fd_, SOL_TCP, TCP_KEEPINTVL, &interval, sizeof(interval)) < 0) {
        // std::cout << "[setKeepAlive] TCP_KEEPINTVL fail : " << strerror(errno) << std::endl;
        // return ;
    }

    memset(&fds_, 0, sizeof(fds_));
    fds_.fd     = fd_;
    fds_.events = POLLIN;
}

void OTTCPActiveSocket::SetWait(int _milliSeconds) {
    if(_milliSeconds >= 0)
        milliSeconds_ = _milliSeconds;
}

void OTTCPActiveSocket::SetLinger() {

    if(fd_ < 0)
        return ;

    struct linger st = {1, 1};
    if(setsockopt(fd_, SOL_SOCKET, SO_LINGER, &st, sizeof(st)) < 0) {
        // char buf[128];
        // printf("setsockopt() - SO_LINGER fail [%d:%s]\n",
        //    errno, strerror_r(errno, buf, sizeof(buf)));
    }
}

bool OTTCPActiveSocket::Send(char * _ptr, size_t _size) {

    if(fd_ < 0)
        return false;

    bool    bRetry = true;
    size_t sendt = 0;

    while(_size > sendt) {
        ssize_t sendn = send(fd_, _ptr + sendt, _size - sendt, 0);

        if(sendn <= 0) {
            char buf[128];
            char temp[256];

            snprintf(temp, sizeof(temp), "send() fail [%zd] [%d:%s]\n",
                    sendn, errno, strerror_r(errno, buf, sizeof(buf)));
            strError_ = temp;

            if(sendn < 0 && errno == 11 && bRetry) {
                bRetry = false;
                poll(nullptr, 0, 5);
                continue;
            }

            return false;
        }

        sendt += sendn;
    }

    return true;
}

int OTTCPActiveSocket::Recv(char * _ptr, size_t _size) {
    if(fd_ < 0)
        return -1;

    struct pollfd l_fds;
    memcpy(&l_fds, &fds_, sizeof(fds_));

    int ret = poll(&fds_, 1, milliSeconds_);
    if(ret <= 0) {
        if(ret < 0) {
            char buf[128];
            char temp[256];

            snprintf(temp, sizeof(temp), "poll() fail [%d] [%d:%s]\n",
                ret, errno, strerror_r(errno, buf, sizeof(buf)));
            strError_ = temp;
        }
        return ret;
    }

    bool    bRetry = true;
    size_t  readt = 0;
    while(_size > readt) {
        ssize_t readn = recv(fd_, _ptr + readt, _size - readt, 0);

        if(readn <= 0) {
            char buf[128];
            char temp[256];

            sprintf(temp, "recv() fail [%zu] [%zd] [%d:%s]\n",
                readt, readn, errno, strerror_r(errno, buf, sizeof(buf)));
            strError_ = temp;

            if(readn < 0 && errno == 11 && bRetry) {
                bRetry = false;
                poll(nullptr, 0, 5);
                continue;
            }

            return -1;
        }

        readt += readn;
    }

    return (int)readt;
}

void OTTCPAccepter::Init(int _fd, struct sockaddr_in * _addr) {

    if(_fd < 0)
        return ;

    OTTCPActiveSocket::Init(_fd, (struct sockaddr *)_addr, sizeof(struct sockaddr_in));
}

bool OTTCPClient::ConnectDualVersion(std::string _ip, int _port) {
    struct addrinfo hints;
    memset(&hints, 0, sizeof(struct addrinfo));

    hints.ai_flags = AI_NUMERICSERV;
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = 0;

    struct addrinfo * res = nullptr;
    std::string strPort = std::to_string(_port);

    int ret = getaddrinfo(_ip.c_str(), strPort.c_str(), &hints, &res);
    if(ret != 0) {
        // printf("getaddrinfo() fail [%d:%s]\n",
        //     errno, gai_strerror(ret));
        return false;
    }

    int fd;
    if((fd = socket(res->ai_family, res->ai_socktype, res->ai_protocol)) < 0) {
        // char buf[128];
        // printf("socket() fail [%d:%s] [%u:%u:%u]\n",
        //    errno, strerror_r(errno, buf, sizeof(buf)),
        //    res->ai_family, res->ai_socktype, res->ai_protocol);

        freeaddrinfo(res);
        return false;
    }

    if(connect(fd, res->ai_addr, res->ai_addrlen) < 0)
    {
        // char buf[128];
        //printf("connect() [%s] fail [%d:%s]\n",
        //    _ip.c_str(), errno, strerror_r(errno, buf, sizeof(buf)));

        close(fd);
        freeaddrinfo(res);
        return false;
    }

    OTTCPActiveSocket::Init(fd, res->ai_addr, res->ai_addrlen);
    freeaddrinfo(res);

    return true;
}

bool OTTCPClient::Connect(std::string _ip, int _port) {
    struct sockaddr_in  addr;
    memset(&addr, 0, sizeof(struct sockaddr_in));

    int fd;

    if((fd = socket(PF_INET, SOCK_STREAM, 0)) < 0) {
        // char buf[128];
        // printf("socket() fail [%d:%s]\n",
        //    errno, strerror_r(errno, buf, sizeof(buf)));

        return false;
    }

    addr.sin_family = AF_INET;
    addr.sin_port = htons((short)_port);

    struct in_addr      in;

    if((in.s_addr = inet_addr(_ip.c_str())) != (unsigned int)-1)
        addr.sin_addr.s_addr = in.s_addr;
    else
    {
        // char buf[128];
        // printf("inet_addr() fail [%d:%s]\n",
        //    errno, strerror_r(errno, buf, sizeof(buf)));

        close(fd);
        return false;
    }

    if(connect(fd, (struct sockaddr *)&addr, sizeof(struct sockaddr_in)) < 0)
    {
        // char buf[128];
        // printf("connect() [%s] fail [%d:%s]\n",
        //    _ip.c_str(), errno, strerror_r(errno, buf, sizeof(buf)));

        close(fd);
        return false;
    }

    OTTCPActiveSocket::Init(fd, (struct sockaddr *)&addr, sizeof(addr));

    return true;
}

OTTCPServer::OTTCPServer()
    : fd_(-1),
      milliSeconds_(1000) {

    memset(&fds_, 0, sizeof(fds_));
}

OTTCPServer::~OTTCPServer() {
    Close();
}

void OTTCPServer::Close() {
    if(fd_ >= 0) {
        close(fd_);
        fd_ = -1;
    }

    memset(&fds_, 0, sizeof(fds_));
}

int OTTCPServer::WaitClient(OTTCPAccepter ** _accepter) {

    if(fd_ < 0)
        return -1;

    struct pollfd   l_fds;
    memcpy(&l_fds, &fds_, sizeof(fds_));

    int ret = poll(&fds_, 1, milliSeconds_);

    switch(ret) {
    case 0:
        return 0;
    case -1:
        // char buf[128];
        // printf("poll() fail [%d:%s]\n",
        //    errno, strerror_r(errno, buf, sizeof(buf)));

        Close();
        return -1;
    default:
        break;
    }

    if(fds_.revents != POLLIN)
    {
        // printf("poll() unexpected event [%d]\n", fds_.revents);

        Close();
        return -1;
    }

    struct sockaddr_in  addr;
    memset(&addr, 0, sizeof(addr));

    socklen_t           addrLen = 0;

    int conn = accept(fd_, (struct sockaddr *)&addr, &addrLen);

    if(conn < 0) {
        // char buf[128];
        // printf("accept() fail [%d:%s]\n",
        //    errno, strerror_r(errno, buf, sizeof(buf)));

        return 0;
    }

    if(*_accepter == nullptr)
        *_accepter = new (std::nothrow) OTTCPAccepter();

    if(*_accepter == nullptr) {
        close(conn);
        return 0;
    }

    (*_accepter)->Init(conn, &addr);
    return 1;
}

void OTTCPServer::SetWait(int _milliseconds) {
    if(_milliseconds >= 0)
        milliSeconds_ = _milliseconds;
}

bool OTTCPServer::Create(int _port) {

    struct sockaddr_in  addr;
    memset(&addr, 0, sizeof(struct sockaddr_in));

    if((fd_ = socket(PF_INET, SOCK_STREAM, 0)) < 0) {
        // char buf[128];
        // printf("socket() fail [%d:%s]\n",
        //    errno, strerror_r(errno, buf, sizeof(buf)));

        Close();
        return false;
    }

    int nval = 1;
    if(setsockopt(fd_,
                  SOL_SOCKET,
                  SO_REUSEADDR,
                  &nval,
                  sizeof(nval)) < 0)
    {
        // char buf[128];
        // printf("setsockopt() SO_REUSEADDR fail [%d:%s]\n",
        //    errno, strerror_r(errno, buf, sizeof(buf)));

        Close();
        return false;
    }

    addr.sin_family = AF_INET;
    addr.sin_port = htons((short)_port);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);

    return ready((struct sockaddr *)&addr, sizeof(addr));
}

bool OTTCPServer::CreateV6(int _port) {

    struct sockaddr_in6  addr;
    memset(&addr, 0, sizeof(struct sockaddr_in6));

    if((fd_ = socket(PF_INET6, SOCK_STREAM, 0)) < 0) {
        // char buf[128];
        // printf("socket() fail [%d:%s]\n",
        //    errno, strerror_r(errno, buf, sizeof(buf)));

        Close();
        return false;
    }

    int nval = 1;
    if(setsockopt(fd_,
                  SOL_SOCKET,
                  SO_REUSEADDR,
                  &nval,
                  sizeof(nval)) < 0)
    {
        // char buf[128];
        // printf("setsockopt() SO_REUSEADDR fail [%d:%s]\n",
        //    errno, strerror_r(errno, buf, sizeof(buf)));

        Close();
        return false;
    }

    addr.sin6_family = AF_INET6;
    addr.sin6_port = htons((short)_port);
    addr.sin6_addr = in6addr_any;

    return ready((struct sockaddr *)&addr, sizeof(addr));
}

bool OTTCPServer::ready(struct sockaddr * _addr, socklen_t _addrlen) {

    if(bind(fd_, _addr, _addrlen) < 0)
    {
        // char buf[128];
        // printf("bind() fail [%d:%s]\n",
        //    errno, strerror_r(errno, buf, sizeof(buf)));

        Close();
        return false;
    }

    if(listen(fd_, 1) < 0)
    {
        // char buf[128];
        // printf("listen() fail [%d:%s]",
        //    errno, strerror_r(errno, buf, sizeof(buf)));

        Close();
        return false;
    }

    memset(&fds_, 0, sizeof(fds_));
    fds_.fd      = fd_;
    fds_.events  = POLLIN;

    return true;
}
