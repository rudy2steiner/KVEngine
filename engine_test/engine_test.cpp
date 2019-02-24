//
// Created by root on 19-2-24.
//

#include <iostream>
#include <string>
#include "rand_string.h"
#include <cstring>
#include <chrono>
#include "engine_test.h"
#include <stdlib.h>
#include <cassert>
const uint64_t valLen=4*1024*1024-8;
using  namespace std;
using  namespace polar_race;
const  size_t VALUE_SIZE=4097;
const  size_t KEY_SIZE=9;
char   *valBuf=new char[VALUE_SIZE];


void EngineTest::Write(Engine* engine, threadsafe_vector<char *>& keys, unsigned numWrite)
{
    RandomString stringGenerator;
    //str copy
    unsigned  offset;
    for (unsigned i = 0; i < numWrite; ++i) {
        char* key=new char[KEY_SIZE];
        char* val=new char[VALUE_SIZE];

        strcpy(val,valBuf);
        cout<<val<<endl;
        stringGenerator.randString(key,8);
        offset=i%(VALUE_SIZE-9);
        memcpy(&val[8],key,8);
        std::cout<<"key:"<<key<<",offset:"<<offset<<",value:"<<val<<std::endl;
        RetCode retCode=engine->Write(key, val);
        if(retCode==kSucc) {
            keys.add(key);
        }else{
            std::cout<<"Write failed,key "<<key<<",value "<<val<<std::endl;
        }
    }
}

const string key_from_value(const string &val)
{
    //cout<<val<<endl;
    return val.substr(8,8);
}
void EngineTest::RandomRead(Engine* engine, const threadsafe_vector<char *>& keys, unsigned numRead)
{
    size_t keySize= keys.size();
    for (unsigned i = 0; i < numRead; ++i) {
        auto& key = keys[rand()%keySize];
        string val;
        engine->Read(key, &val);
        const string keyFromVal= key_from_value(val);
        cout<<"key from value:"<<keyFromVal<<endl;
        if (strcmp(key,keyFromVal.c_str())!=0) {
            std::cout<<"value:"<<val<<std::endl;
            std::cout << "Random Read error: key "<<key<<" and expect key " << key_from_value(val)<<std::endl;
            exit(-1);
        }
    }
}
void test(const std::string& dir, unsigned numThreads, unsigned numWrite)
{
    Engine *engine = NULL;
    RetCode ret = Engine::Open(dir.c_str(), &engine);
    assert(ret == kSucc);

    threadsafe_vector<std::string> keys;
    keys.reserve(numThreads * numWrite);
    delete engine;
}
int main(){
    std::string dir("/tmp/polar");
    RandomString rs;
    rs.randString(valBuf,VALUE_SIZE-1);
    cout<<"original:"<<valBuf<<endl;
    uint64_t numWrite=2;
    // open engine
    Engine *engine = NULL;
    EngineTest engineTest;
    RetCode ret = Engine::Open(dir.c_str(), &engine);
    assert(ret == kSucc);
    threadsafe_vector<char *> keys;
    keys.reserve(numWrite);
    auto writeStart = chrono::high_resolution_clock::now();
    engineTest.Write(engine,keys,numWrite);
    for(int i=0;i<keys.size();i++){
//        rs.randString(key,8);
//        memcpy(&cchars[4],key,8); // copy into offset
        cout<<keys[i]<<endl;

    }
    engineTest.RandomRead(engine,keys,numWrite);
    auto writeEnd = chrono::high_resolution_clock::now();
    std::cout << "Sequential read takes: "
              << chrono::duration<double,milli>(writeEnd - writeStart).count()
              << " milliseconds" <<endl;
    delete engine;
}

