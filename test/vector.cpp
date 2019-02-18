//
// Created by root on 19-2-16.
//

#include <iostream>
#include <vector>
#include <array>
#include <ctime>
using namespace std;

int main(){
    vector<double> vi;
    double al[4]={1.2,2.3,3.6,4.8};
    vector<double> v2(4);
    v2[0]=1.0/3;
    v2[1]=1.0/4;
    vi=v2;
    array<double,4> a3={1.1,2.1,2.3,3.1};
    array<double,4> a4;
    a4=a3;

    cout<<vi[0]<<endl;
    for(int i=0;i<5;i++){
        cout<<"C++ knows loops\n";
    }
    string word;
    cout<<"input a word:";
    cin>>word;
    for(int i=word.size()-1;i>=0;i--){
        cout<<word[i];
    }
    cout<<endl;
    clock_t start=clock();
//    while(clock()-start<600){ cout<<"wait";}
    for(double a:a3){
        cout<<a<<endl;
    }
    char ch;
    cin.get(ch);
    int count;
    while(cin.fail()== false){
        cout<<ch;
        count++;
        cin.get(ch);
    }
    cout<<"read "<<count<<"char "<<endl;
    return 0;

}