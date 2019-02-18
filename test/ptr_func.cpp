//
// Created by root on 19-2-18.
//

#include <iostream>
double betsy(int);
double pam(int);
void estimate(int lines,double (*pf)(int));
using namespace std;

int  main(){

    int code;
    cout<<"How many lines of the code your  needs?";
    cin>>code;
    cout<<"betsy's estimate :\n";
    estimate(code,betsy);
    cout<<"pam's estimate :\n";
    estimate(code,pam);
    return 0;
}
double betsy(int lns){
    return 0.05*lns;
}

double pam(int lns){
    return  0.03*lns+0.0004*lns*lns;
}
void estimate(int lines, double (*pf)(int)){
     cout<<lines<< " lines will take";
    cout<<(*pf)(lines)<< " hours\n";
}