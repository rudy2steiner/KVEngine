//
// Created by root on 19-2-24.
//

//
// Created by root on 19-2-24.
//

#include <iostream>
#include <string>
#include "rand_string.h"
#include <cstring>
#include <stdio.h>
const uint64_t valLen=4*1024*1024-8;
using  namespace std;
int main(){
    RandomString rs;
    char *chars=new char[4097];
    char *key=new char[9];
    rs.randString(chars,4096);
    rs.randString(key,8);
    cout<<"original:"<<chars<<endl;
    uint64_t offset=12348;
    const char *offsetChar=to_string(offset).c_str();
    cout<<offsetChar<<",size:"<< strlen(offsetChar)<<endl;
    memcpy(&chars[5],offsetChar,8);
    cout<<chars[0]<<","<<key<<endl;
    cout<<"changed:"<<chars<<endl;
    cout<<key<<endl;
    string *s;
    *s=string(key,8);
    cout<<s<<endl;
//    string s(chars);
//    cout<<s<<",lenght:"<<s.size()<<endl;
//    const char *cString;
//    int b=1237;
//    int *a=&b;
//    cout<<strlen(cString)<<" size:"<<sizeof(cString)<<","<<sizeof(a)<<endl;
//    uint64_t key=0;
//    uint64_t kk=1128;
//    string sstring="abcdefghijk";
//    char *sChar=new char[sstring.size()+1];
//    const char* constChar=sstring.c_str();
//
//    cout<<"src:"<<sstring<<endl;
////    cout<<"modified:"<<copystring<<endl;
//    strcpy(sChar,sstring.c_str());
//    cout<<"5 char address:"<<++sChar<<endl;
//    cout<<"sChar address:"<<static_cast<void *>(sChar)<<endl;
//    memcpy(++sChar,&kk, sizeof(kk));
//    string valuestring(sChar);
//    cout<<"modified str:"<<valuestring<<endl;
//    string copystring(sstring);
//    copystring[2]='z';
    //cout<<s<<endl;
//    for(;key<UINT64_MAX;key++ ){
//      uint64_t loc=  key%valLen;
//      string val=s;
//      //val[loc]
//    }

}