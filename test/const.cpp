//
// Created by System Administrator on 2019-02-19.
//


#include <iostream>

using namespace std;
int main(){


    int age=28;
    const int *pt=&age;
    cout<<*pt<<endl;
    cout<<++age<<endl;
    cout<<*pt<<endl;
    *pt=39;
    cout<<*pt<<endl;
    return 0;
}
