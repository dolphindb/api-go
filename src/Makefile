CFLAGS = -D_GLIBCXX_USE_CXX11_ABI=0 -std=c++11 -fPIC -DLINUX
INCLUDES= -I./include
DOLPHINDBAPI_LIBRARIES_DIRS = -L./bin/linux_x64
LIBSSL_DEFAULT_PATH = /usr/lib/x86_64-linux-gnu/libssl.so.1.0.0
LIBSSL = -lssl
ifneq ("$(wildcard $(LIBSSL_DEFAULT_PATH))", "")
	LIBSSL = $(LIBSSL_DEFAULT_PATH)
endif

LIBCRYPTO_DEFAULT_PATH = /usr/lib/x86_64-linux-gnu/libcrypto.so.1.0.0
LIBCRYPTO = -lcrypto
ifneq ("$(wildcard $(LIBCRYPTO_DEFAULT_PATH))", "")
	LIBCRYPTO = $(LIBCRYPTO_DEFAULT_PATH)
endif

libwrapper.so: wrapper.o
	gcc $(CFLAGS) $(INCLUDES) $(DOLPHINDBAPI_LIBRARIES_DIRS) -shared -o libwrapper.so wrapper.o -lDolphinDBAPI $(LIBSSL) $(LIBCRYPTO) -luuid -lpthread
wrapper.o: include/wrapper.cpp
	gcc $(CFLAGS) $(INCLUDES) -c include/wrapper.cpp -o wrapper.o

clean:
	rm *.o
	rm libwrapper.so