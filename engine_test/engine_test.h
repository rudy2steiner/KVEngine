//
// Created by root on 19-2-24.
//
#ifndef ENGINE_ENGINE_TEST_H
#define ENGINE_ENGINE_TEST_H
#include <mutex>
#include <vector>
#include "include/engine.h"
using namespace polar_race;
template <typename T>
class threadsafe_vector : public std::vector<T>
{
public:
    void add(const T& val)
    {
        std::lock_guard<std::mutex> lock(mMutex);
        this->push_back(val);
    }

    void add(T&& val)
    {
        std::lock_guard<std::mutex> lock(mMutex);
        this->emplace_back(val);
    }

private:
    mutable std::mutex mMutex;
};

class EngineTest{

public:
    void Write(Engine* engine, threadsafe_vector<char *>& keys, unsigned numWrite);
    void RandomRead(Engine* engine, const threadsafe_vector<char *>& keys, unsigned numRead);

};



#endif //ENGINE_ENGINE_TEST_H
