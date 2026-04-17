#include "database.hpp"
#include <iostream>

int main(int argc, char* argv[]) {
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;

    if (argc != 3) {
        std::cerr << "Expected two arguments" << std::endl;
        return 1;
    }

    std::string_view database_file_path = argv[1];
    std::string_view command = argv[2];

    if (command == ".dbinfo") {
        auto db = Database::open(database_file_path);
        if (!db) {
            std::cerr << db.error() << std::endl;
            return 1;
        }
        std::cout << "database page size: " << db->page_size() << '\n'
                  << "number of tables: " << db->num_tables() << '\n';
    }

    return 0;
}
