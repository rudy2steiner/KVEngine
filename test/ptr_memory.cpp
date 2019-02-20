//
// Created by System Administrator on 2019-02-20.
//

#include <iostream>

using namespace std;

int main(){

    int *a=new int;  // anonymous int visit,new memory allocation
    *a=1000;
    struct b{
        int a;
        double b;
    } integer={1,0.0001};
    cout<<*a<<endl;
    delete a; // memory ptr
    cout<<integer.a<<" "<<integer.b<<endl;
    return 0;
}