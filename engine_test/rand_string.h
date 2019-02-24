//
// Created by root on 19-2-24.
//

#ifndef ENGINE_RAND_STRING_H
#define ENGINE_RAND_STRING_H
#include <string>
const size_t TABLE_SIZE=100;
class RandomString{
private:
    const char table[TABLE_SIZE]="0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

public:
     void randString(char *s,int len);
};
#endif //ENGINE_RAND_STRING_H
