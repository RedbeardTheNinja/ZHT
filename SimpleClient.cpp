#include <iostream>
#include <iomanip>
#include "ZHTClient.cpp"

using namespace std;

int main(int argc, const char* argv[]) {

    if (argc != 3) {
        cout<<"Usage: ./SimpleClient <memberListFilePath> <configFilePath>"<<endl;
        return -1;
    }

    ZHTClient client;
    string memberList(argv[1]);
    string cfgFile(argv[2]);

    if (client.initialize(cfgFile, memberList) != 0) {
        cout << "Crap! ZHTClient initialization failed, program exits." << endl;
        return -1;
    }

    string pointerToString;
    string key("key");

    int ret = client.insert(key, "Hello World!");
    if (ret != 0) {
        cout << "Error Inserting: " << strerror(ret) << endl;
        return -1;
    } else {
        cout << "Inserted value" << endl;
    }


    ret = client.get(key, pointerToString);
    if (ret != 0) {
        cout << "Error On Lookup: " << strerror(ret) << endl;
        return -1;
    } else {
        cout << "Found value '" << pointerToString << "'\n";
    }

    ret = client.remove(key);
    if (ret != 0) {
        cout << "Error On Remove: " << strerror(ret) << endl;
    } else {
        cout << "Removed value" << endl;
    }

    return 0;
}
