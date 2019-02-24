//
// Created by root on 19-2-24.
//
#include "rand_string.h"
#include <stdlib.h>
#include <cstring>
#include <iostream>

void RandomString::randString(char *s,int len){
   size_t size= sizeof(table);
   size_t strLen=strlen(table);
   //std::cout<<size<<","<<strLen<<std::endl;
   for(int i=0;i<len;i++){
       s[i]=table[rand()%strLen];
   }
   s[len]=0;
}
