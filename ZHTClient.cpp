#include <string>
#include <cstring>
#include <iostream>
#include <sstream>
#include <fstream>

#include "zht_util.h"
#include "meta.pb.h"

class ZHTClient {
public:
	int REPLICATION_TYPE; //serverside or client side. -1:error
	int NUM_REPLICAS; //-1:error
	int protocolType; //1:1TCP; 2:UDP; 3.... Reserved.  -1:error
	vector<struct HostEntity> memberList;
	ZHTClient();

	int initialize(string configFilePath, string memberListFilePath);
	struct HostEntity str2Host(string str);
	int insert(string key, string value); //following functions should only know string, address where to can be calculated.
	int sendPackage(string pkg);
	int get(string key, string &returnValue);
	int remove(string key);

};

ZHTClient::ZHTClient() { // default all invalid value, so the client must be initialized to set the variables.
	//Since constructor can't return anything, we must have an initialization function that can return possible error message.
	this->NUM_REPLICAS = -1;
	this->REPLICATION_TYPE = -1;
	this->protocolType = -1;
}

int ZHTClient::initialize(string configFilePath, string memberListFilePath) {

	//read cfg file
	this->memberList = getMembership(memberListFilePath);

	FILE *fp;
	char line[100], *key, *svalue;
	int ivalue;

	fp = fopen(configFilePath.c_str(), "r");
	if (fp == NULL) {
		cout << "Error opening the config file." << endl;
		return -1;
	}
	while (fgets(line, 100, fp) != NULL) {
		key = strtok(line, "=");
		svalue = strtok(NULL, "=");
		ivalue = atoi(svalue);

		if ((strcmp(key, "REPLICATION_TYPE")) == 0) {
			this->REPLICATION_TYPE = ivalue;
		} //other config options follow this way(if).

		else if ((strcmp(key, "NUM_REPLICAS")) == 0) {
			this->NUM_REPLICAS = ivalue + 1; //note: +1 is must
		} else {
			cout << "Config file is not correct." << endl;
			return -2;
		}

	}
	return 0;

}

//transfer a key to a host index where it should go
struct HostEntity ZHTClient::str2Host(string str) {
	Package pkg;
	pkg.ParseFromString(str);
	int index = myhash(pkg.virtualpath().c_str(), this->memberList.size());
	struct HostEntity host = this->memberList.at(index);

	return host;
}

//send a plain string to destination, receive return state.
int ZHTClient::insert(string key, string value) {

    Package package, package_ret;
    package.set_virtualpath(key); //as key
    package.set_isdir(true);
    package.set_replicano(5); //orginal--Note: never let it be nagative!!!
    package.set_operation(3); // 3 for insert, 1 for look up, 2 for remove
    package.set_realfullpath(value);
    string pkg = package.SerializeAsString();
    return sendPackage(pkg);
}

int ZHTClient::sendPackage(string pkg) {
    int sock = -1;
    struct HostEntity dest = this->str2Host(pkg);
    int ret = simpleSend(pkg, dest, sock);
    d3_closeConnection(sock);
    return ret;
}

int ZHTClient::get(string key, string &returnValue) {

    Package package, package_ret;
    package.set_virtualpath(key); //as key
    package.set_isdir(false); //this is crucial: true for not breaking the string.
    package.set_replicano(5); //orginal--Note: never let it be nagative!!!
    package.set_operation(1); // 3 for insert, 1 for look up, 2 for remove
	string msg = package.SerializeAsString();

	int sock = -1;
	struct HostEntity dest = this->str2Host(msg);

	int ret = simpleSend(msg, dest, sock);
	char buff[MAX_MSG_SIZE]; //MAX_MSG_SIZE
	memset(buff, 0, sizeof(buff));

	//this only work for TCP. UDP need to make a new one so accept returns from server.
	if (ret == msg.length()) {
		int rcv_size = -1;
		if (TRANS_PROTOCOL == USE_TCP) {

			rcv_size = d3_recv_data(sock, buff, MAX_MSG_SIZE, 0); //MAX_MSG_SIZE

		} else if (TRANS_PROTOCOL == USE_UDP) {
			srand(getpid() + clock());
			sockaddr_in tmp_sockaddr;

			memset(&tmp_sockaddr, 0, sizeof(sockaddr_in));
			//receive lookup result
			rcv_size = d3_svr_recv(sock, buff, MAX_MSG_SIZE, 0, &tmp_sockaddr);
		}
		if (rcv_size < 0) {
			cout << "Lookup received error." << endl;
			return rcv_size;
		} else {
			returnValue.assign(buff);
		}
	}
	d3_closeConnection(sock);

	return ret;
}

int ZHTClient::remove(string key) {
	int sock = -1;

    Package package, package_ret;
	package.set_virtualpath(randomString(50)); //as key
	package.set_isdir(false); //this is crucial: true for not breaking the string.
	package.set_replicano(5); //orginal--Note: never let it be nagative!!!
	package.set_operation(2); // 3 for insert, 1 for look up, 2 for remove
	string msg = package.SerializeAsString();

	struct HostEntity dest = this->str2Host(msg);
	int ret = simpleSend(msg, dest, sock);
	d3_closeConnection(sock);
	return ret;
}
