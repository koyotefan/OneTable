/*
 * **************************************************************************
 *
 * OTDefine.hpp
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

#ifndef OT_DEFINE_HPP
#define OT_DEFINE_HPP

#include <iostream>
#include <string>
#include <unordered_map>
#include <map>
#include <time.h>
#include <mutex>
#include <memory>

#ifdef __OT_DEBUG
    #define I_LOG(msg, ...)     printf(msg "\n", ## __VA_ARGS__)
    #define D_LOG(msg, ...)     printf(msg "\n", ## __VA_ARGS__)
    #define W_LOG(msg, ...)     printf(msg "\n", ## __VA_ARGS__)
    #define E_LOG(msg, ...)     printf(msg "\n", ## __VA_ARGS__)
#endif

/*-
#else
    using namespace std;
	#include "NDFServiceLog.hpp"
#endif
-*/


namespace nsOneTable {

    enum class KeyType : int {
        STR = 0,
        BIN = 1,
        INT = 2
    };

    const int MAX_MULTI_KEY_CNT = 16;
    //const int MAX_SIZE_PER_PAGE = 64 * 1024;
    const int MAX_SIZE_PER_PAGE = 64 * 1024 * 1024;

}

#endif // OT_DEFINE_HPP
