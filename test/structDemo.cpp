//
// Created by root on 19-2-16.
//

#include <iostream>

using namespace std;

struct inflatable{
    char name[20];
    float volumn;
    double price;
};

int main(){

    inflatable a={"wangjin"};
    inflatable b=a;
    strcpy(b.name,"djkjfdkj");
    cout<<a.name<<endl;
    cout<<b.name<<endl;
    cout<<a.name<<endl;

    enum spectrum{red,orange,yellow,green,blue,vilote,indigo,ultraviolet};
    spectrum  color(red);
    cout<<color<<endl;
    int *c=new int;
    delete c;
    int *psome=new int[10];
    delete [] psome;
    return 0;
}
