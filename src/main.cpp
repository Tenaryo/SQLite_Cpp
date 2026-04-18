#include "database.hpp"
#include <iostream>

int main(int argc, char* argv[]) {
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;

    if (argc != 3) {
        std::cerr << "Expected two arguments" << std::endl;
        return 1;
    }

    auto db = Database::open(argv[1]);
    if (!db) {
        std::cerr << db.error() << std::endl;
        return 1;
    }

    handle_command(*db, argv[2], std::cout);
}
