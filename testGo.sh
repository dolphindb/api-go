#!/bin/bash

#cd api-go/ firstly

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$(pwd)/src

res=/hdd/hdd1/testing/api/result/api-go/output.txt

rm -f $res
rm -f test.res


#rm -f ct.res
#go test -v ./test/Constant_test.go >> ct.res
#awk 'BEGIN{sum=0;sum1=0}{s+=gsub(/PASS/,"&");sum+=gsub(/RUN/,"&"1)}END{print sum,s}' ct.res   >> test.res

go test -v ./test/Constant_test.go  | awk 'BEGIN{sum=0;sum1=0}{s+=gsub(/--- PASS/,"&");sum+=gsub(/RUN/,"&"1)}END{print sum,s}'   >> test.res
go test -v ./test/DBConnection_test.go  | awk 'BEGIN{sum=0;sum1=0}{s+=gsub(/--- PASS/,"&");sum+=gsub(/RUN/,"&"1)}END{print sum,s}'   >> test.res
go test -v ./test/Set_test.go | awk 'BEGIN{sum=0;sum1=0}{s+=gsub(/--- PASS/,"&");sum+=gsub(/RUN/,"&"1)}END{print sum,s}'   >> test.res
go test -v ./test/Table_test.go  | awk 'BEGIN{sum=0;sum1=0}{s+=gsub(/--- PASS/,"&");sum+=gsub(/RUN/,"&"1)}END{print sum,s}'   >> test.res
go test -v ./test/Vector_test.go  | awk 'BEGIN{sum=0;sum1=0}{s+=gsub(/--- PASS/,"&");sum+=gsub(/RUN/,"&"1)}END{print sum,s}'   >> test.res


cat test.res | awk 'BEGIN{sum=0;sum1=0}{ sum+=$0;sum1+=$2}END{print "total",sum, "passed ",sum1,"failed ",sum-sum1}' >>$res


