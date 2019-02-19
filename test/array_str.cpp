//
// Created by System Administrator on 2019-02-19.
//

#include <iostream>
#include <string>
#include "../include/polar_string.h"

using namespace std;
using namespace polar_race;
int main(){

    PolarString ps("abcd",4);
    char a[]="abcdcc";
    const char *b;
    string c="bjljlkj";
    b=c.data();
    cout<< a<<endl;
    cout<<b<<endl;
    cout<<ps.ToString()<<endl;
    const int min_len=strlen(a)<strlen(b)?strlen(a):strlen(b);
    cout<<memcmp(b,a,min_len)<<endl;
    return 0;
}
