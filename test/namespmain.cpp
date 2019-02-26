//
// Created by root on 19-2-21.
//


#include <iostream>
#include "namesp.h"
void other(void);
void another(void);
# g++ namesp.o namespmain.cpp -o namespmain

int main(void){

     using debts::Debt;
    using  debts::showDebt;
    Debt golf={{"Beeny","Goatsniff"},1222.0};
    showDebt(golf);
    other();
    another();

    return 0;
}

void other(void){
    using std::cout;
    using  std::endl;
    using namespace debts;
    Person dg={"Doodles","Glister"};
    showPerson(dg);
    cout<<endl;
    Debt zippy[3];
    int i;
    for(i=0;i<3;i++){
        getDebt(zippy[i]);
    }
    for(i=0;i<3;i++){
        showDebt(zippy[i]);
    }
    cout<<"Total debt:$"<<sumDebts(zippy,3)<<endl;
    return;
}

void another(void){

    using  pers::Person;
    Person collector={"Milo","Rightshift"};
    pers::showPerson(collector);
    std::cout<<std::endl;
}