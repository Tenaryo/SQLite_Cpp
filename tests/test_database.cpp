#include "database.hpp"
#include <algorithm>
#include <cassert>
#include <cstdlib>

auto main() -> int {
    {
        auto db = Database::open("sample.db");
        assert(db.has_value());
        assert(db->page_size() == 4096);
        assert(db->num_tables() == 3);
    }

    {
        auto db = Database::open("nonexistent.db");
        assert(!db.has_value());
    }

    {
        auto db = Database::open("sample.db");
        assert(db.has_value());
        auto names = db->table_names();
        assert(names.size() == 2);
        assert(std::ranges::equal(names, std::array{"apples", "oranges"}));
    }

    return EXIT_SUCCESS;
}
