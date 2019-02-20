//
// Created by root on 19-2-20.
//
#include <iostream>
using  namespace std;
template <typename T>
void Swap(T &a,T &b);
struct job{
    char name[40];
    double  salray;
    int floor;
};
template<> void Swap<job>(job &j1,job &j2);
void Show(job &j);

int main(){

    int i=10;
    int j=20;
    cout<<"i , j "<<i<<","<<j<<endl;
    Swap(i,j);
    cout<<"i , j "<<i<<","<<j<<endl;
    double a=101.0;
    double b=203.0;
    cout<<"a , b "<<a<<","<<b<<endl;
    Swap(a,b);
    cout<<"a , b "<<a<<","<<b<<endl;
    job sue={"sue",29.0,2};
    job clean={"clean",27.0,5};
    Show(sue);
    Show(clean);
    Swap(sue,clean);
    Show(sue);
    Show(clean);
    return 0;
}

template <typename T>
void   Swap(T &a,T &b){
    T temp=a;
    a=b;
    b=temp;
}

template<> void Swap<job>(job &j1,job &j2){

    double  t1;
    int t2;
    t1=j1.salray;
    j1.salray=j2.salray;
    j2.salray=t1;

    t2=j1.floor;
    j1.floor=j2.floor;
    j2.floor=t2;
}

void Show(job &j){
    cout<<"name:"<<j.name<<endl;
    cout<<"salary:"<<j.salray<<endl;
    cout<<"floor:"<<j.floor<<endl;
}

