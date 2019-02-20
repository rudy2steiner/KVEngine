//
// Created by System Administrator on 2019-02-20.
//

#include <iostream>

using namespace std;
const  int ArSize=80;
char * left(const char * str,int n=1);

int main(){


    cout<<"input str and sub len:";
    char chars[ArSize];
    int len;
    cin>>chars;
    cin>>len;
    char *a=left(chars);
    cout<<a<<endl;
    a=left(chars,len);
    cout<<a<<endl;
    return 0;
}

char * left(const char * str,int n){
    if(n<0)
        n=0;
    char * p=new char[n+1];
    int i;
    for(i=0;i<n&&str[i];i++){
        p[i]=str[i];
    }
    while(i<=n)
        p[i++]='\0';
    return p;

}