
#include <iostream>
#include <sstream>

#include <time.h>
#include <poll.h>
#include <memory>

#include "OneTable.hpp"
#include "OTReplication.hpp"

#include <vector>

#include "StructDef.hpp"

void insert(std::shared_ptr<nsOneTable::OneTable<stPacket>>   _pTbl,
            std::string _id,
            std::string _data) {

    stPacket st;
    st.id = std::stoi(_id);
    st.ctime = time(nullptr);
    ::strcpy(st.data, _data.c_str());

    if(_pTbl->Put(st) == false)
        std::cout << "[main] insert Error.. Key:" << st.id << std::endl;
    else
        std::cout << "[main] insert Ok.. Key:" << st.id << std::endl;
}

void del(std::shared_ptr<nsOneTable::OneTable<stPacket>>   _pTbl,
        std::string _id) {

    unsigned int id = std::stoi(_id);

    if(_pTbl->Del(&id, sizeof(id)) == false)
        std::cout << "[main] del Error.. Key:" << id << std::endl;
    else
        std::cout << "[main] del Ok.. Key:" << id << std::endl;
}

void get(std::shared_ptr<nsOneTable::OneTable<stPacket>>   _pTbl,
        std::string _id) {

    stPacket st;
    st.id = std::stoi(_id);

    if(_pTbl->Get(&st.id, sizeof(st.id), st) == false) {
        std::cout << "[main] get Error.. Key:" << st.id << std::endl;
    } else {
        std::cout << "[main] get Ok.. Key:" << st.id << std::endl;
        std::cout << "[main] get Ok.. time:" << st.ctime << std::endl;
        std::cout << "[main] get Ok.. data:" << st.data<< std::endl;
    }
}

void doProcess(std::shared_ptr<nsOneTable::OneTable<stPacket>>   _pTbl) {

    std::vector<std::string>    vWords;

    while(true) {
        std::cout << "[main] INSERT/DELETE/GET [id] [DATA]\n" << std::endl;
        std::string     input;
        getline(std::cin, input);

        std::cout << "[main] :" << input << std::endl;

        std::string     word;
        for(std::stringstream stream(input); (stream >> word) ; )
            vWords.push_back(word);

        if(vWords.size() != 3 && vWords.size() != 2) {
            std::cout << "[main] Invalid Cmd\n";
            vWords.clear();
            continue;
        }

        if(vWords[0].compare("INSERT") == 0)
            insert(_pTbl, vWords[1], vWords[2]);
        else if(vWords[0].compare("DELETE") == 0)
            del(_pTbl, vWords[1]);
        else if(vWords[0].compare("GET") == 0)
            get(_pTbl, vWords[1]);

        vWords.clear();
    }
}

int main(int argc, char * argv[]) {

    if(argc != 3) {
        std::cout << "Invalid argument" << std::endl;
        std::cout << "[USAGE] ./OT [DATA DIR] [PEERIP]" << std::endl;
        return 0;
    }

    std::string     path = argv[1];

    //std::shared_ptr<nsOneTable::OneTable<stPacket>>
    //    pTbl(new nsOneTable::OneTable<stPacket>("Test", "/home/jhchoi/SCEF_ETC/DATA"));

    std::shared_ptr<nsOneTable::OneTable<stPacket>>     pTbl;

    pTbl = std::shared_ptr<nsOneTable::OneTable<stPacket>>(
                new nsOneTable::OneTable<stPacket>("Test", path));

    pTbl->DefinePK(offsetof(stPacket, stPacket::id),
                sizeof(stPacket::id),
                nsOneTable::KeyType::INT);
    pTbl->DefineSK(0,
                offsetof(stPacket, stPacket::ctime),
                sizeof(stPacket::ctime),
                nsOneTable::KeyType::INT);

    if(pTbl->Init(10000) == false) {
        std::cout << "[main] OneTable Init() fail" << std::endl;
        return 0;
    }

    //std::shared_ptr<nsOneTable::OTReplication<stPacket>>
    //    pRepl(new nsOneTable::OTReplication<stPacket>(pTbl));

    std::shared_ptr<nsOneTable::OTReplication<stPacket>>     pRepl;
    pRepl = std::shared_ptr<nsOneTable::OTReplication<stPacket>>(
                new nsOneTable::OTReplication<stPacket>(pTbl));

    pRepl->SetPeer(argv[2], argv[2], 10058, nsOneTable::ReplMemberType::OPTION);
    pRepl->SlaveIsDisabledToGetCommand();

    if(pRepl->Init() == false) {
        std::cout << "[main] OTReplication Init() fail" << std::endl;
        return 0;
    }

    while(true) {
        if(pRepl->IsReady() == true)
            break;
        poll(nullptr, 0, 1000);
        std::cout << "----- " << std::endl;
    }

    std::cout << "[main] Ready Ok" << std::endl;

    doProcess(pTbl);

}
