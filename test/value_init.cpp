//
// Created by root on 19-2-16.
//

#include <iostream>
#include <stdint.h>
#include <cstring>
using namespace std;
int main(){

    int a(12);
    int b={a};
    std::cout<< a+b<<std::endl;

    uint32_t yams[3];
    yams[0]=2,yams[1]=2,yams[2]=3;
    cout<< sizeof(yams)<<endl;
    cout<<"sum:"<<yams[0]+yams[1]+yams[2]<<endl;
    const int SIZE=150;
    char name1[SIZE]="c++ is hard to understand!what's your name:";
    char name2[SIZE];
    cout<<"i'm busy,"<<name1;
    cin>>name2;
    cout<<"well,your name has ";
    cout<<strlen(name2)<<" letters and is stored in an array of "<< sizeof(name2)<<" bytes \n";
    return 0;
}