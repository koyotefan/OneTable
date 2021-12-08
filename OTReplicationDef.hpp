
#ifndef OT_REPLICATION_DEF_HPP
#define OT_REPLICATION_DEF_HPP

namespace nsOneTable {

    template <typename dataT>
    struct stHistory {
        size_t  tid;
        bool    bInsert;
        dataT   d;

        stHistory() {}
        stHistory(size_t _tid, bool _bInsert, const dataT & _d) {
          tid = _tid;
          bInsert = _bInsert;
          memcpy(&d, &_d, sizeof(dataT));
        }

        stHistory<dataT> & operator=(const stHistory<dataT> & _st) {
            if(this != &_st) {
                tid = _st.tid;
                bInsert = _st.bInsert;
                memcpy(&d, &_st.d, sizeof(dataT));
            }

            return *this;
        }
    }  __attribute__((packed));


    template <typename dataT>
    struct stSyncData {
        size_t  tid;
        bool    bMyTid;
        dataT   d;

        stSyncData() {}
        stSyncData(size_t _tid, bool _bMyTid, const dataT & _d) {
          tid = _tid;
          bMyTid = _bMyTid;
          memcpy(&d, &_d, sizeof(dataT));
        }

        stSyncData<dataT> & operator=(const stSyncData<dataT> & _st) {
            if(this != &_st) {
                tid = _st.tid;
                bMyTid = _st.bMyTid;
                memcpy(&d, &_st.d, sizeof(dataT));
            }

            return *this;
        }
    }  __attribute__((packed));


    enum class ReplicationCmd : size_t {
        ReqPush     = 0,
        OkPush      = 1,
        DoPush      = 2,
        MustSync    = 3,
        ReqSync     = 4,
        OkSync      = 5,
        DoSync      = 6,
        EndSync     = 7,
        NotReady    = 8,
        OkReady     = 9,
        ReqResume   = 10,
        OkResume    = 11,
        ReqDelete   = 12,
        OkDelete    = 13
    };

    struct stReplicationCmd {
        ReplicationCmd cmd;
        size_t  dataCnt;
        size_t  sentTid;
        size_t  recvTid;
    };

    enum class ReplMemberType : int {
        MUST = 0,
        OPTION = 1
    };


}

#endif // OT_REPLICATION_DEF_HPP
