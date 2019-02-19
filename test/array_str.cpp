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
    string c="jjljlkj";
    b=c.data();
    cout<< a<<endl;
    cout<<b<<endl;
    cout<<ps.ToString()<<endl;
    return 0;
}
