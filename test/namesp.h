//
// Created by root on 19-2-21.
//

#ifndef ENGINE_NAMESP_H
#define ENGINE_NAMESP_H

#include <string>
namespace pers{

    struct  Person{
        std::string fname;
        std::string lname;
    };
    void getPerson(Person &);
    void showPerson(const Person &);
}
namespace debts{

    using namespace pers;
    struct Debt{
        Person name;
        double amout;
    };
    void getDebt(Debt &);
    void showDebt(const Debt &);
    double sumDebts(const Debt ar[],int n);
}
#endif //ENGINE_NAMESP_H
