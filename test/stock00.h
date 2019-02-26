//
// Created by root on 19-2-22.
//

#ifndef ENGINE_STOCK00_H
#define ENGINE_STOCK00_H

#include <string>

class Stock{
private:
    std::string company;
    long shares;
    double share_val;
    double total_val;
    void set_tot(){total_val=shares*share_val;}

public:
    Stock();
    Stock(std::string &co,long n=0,double pr=0.0);
    ~Stock();
    void acquire(const std::string &co,long n, double pr);
    void buy(long num, double price);
    void sell(long nem, double price);
    void update(double price);
    void show() const ; // not modify object
};
#endif //ENGINE_STOCK00_H
