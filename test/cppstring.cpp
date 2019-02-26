//
// Created by root on 19-2-16.
//


#include <iostream>
#include <string>
inline double square(double x){ return x*x;}
int main(){

    using namespace std;
    char charr1[20];
    char charr2[]="jaguar";
    const char *charr3="sdfsdfsd";
    string str1;
    string str2="panther";
    str1=str2;
    cout<<"str1:"<<str1<<endl;
    cout<<"Enter a kind of feline：";
    cin>>charr1;
    cout<<"Enter another kind of feline:";
    cin>>str1;
    cout<<"there are some felines:\n";
    cout<<charr1<<" "<<charr2<<" "
        <<str1<<" "<< str2<<"\n";
    cout<<charr3<<endl;
    double  c=13.0;
    cout<<square(c)<<endl;
    return 0;
}
