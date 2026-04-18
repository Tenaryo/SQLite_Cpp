#include "database.hpp"
#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <sstream>
#include <string>

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

    {
        auto db = Database::open("sample.db");
        assert(db.has_value());
        assert(db->rootpage("apples") == 2);
        assert(db->rootpage("oranges") == 4);
        assert(db->rootpage("sqlite_sequence") == 3);
    }

    {
        auto db = Database::open("sample.db");
        assert(db.has_value());
        assert(db->count_rows(2) == 4);
    }

    {
        auto db = Database::open("sample.db");
        assert(db.has_value());
        std::ostringstream out;
        handle_command(*db, "SELECT COUNT(*) FROM apples", out);
        assert(out.str() == "4\n");
    }

    {
        auto sel = parse_select("SELECT name FROM apples");
        assert(sel.has_value());
        assert(sel->columns.size() == 1);
        assert(sel->columns[0] == "name");
        assert(sel->table == "apples");
        assert(!sel->is_count);
    }

    {
        auto sel = parse_select("SELECT color FROM oranges");
        assert(sel.has_value());
        assert(sel->columns.size() == 1);
        assert(sel->columns[0] == "color");
        assert(sel->table == "oranges");
        assert(!sel->is_count);
    }

    {
        auto sel = parse_select("SELECT name, color FROM apples");
        assert(sel.has_value());
        assert(sel->columns.size() == 2);
        assert(sel->columns[0] == "name");
        assert(sel->columns[1] == "color");
        assert(sel->table == "apples");
        assert(!sel->is_count);
    }

    {
        auto sel = parse_select("SELECT COUNT(*) FROM apples");
        assert(sel.has_value());
        assert(sel->is_count);
        assert(sel->table == "apples");
    }

    {
        auto db = Database::open("sample.db");
        assert(db.has_value());
        auto cols = db->parse_create_table("CREATE TABLE apples\n(\n\tid integer primary key "
                                           "autoincrement,\n\tname text,\n\tcolor text\n)");
        assert(cols.size() == 3);
        assert(cols[0] == "id");
        assert(cols[1] == "name");
        assert(cols[2] == "color");
    }

    {
        auto db = Database::open("sample.db");
        assert(db.has_value());
        auto cols = db->parse_create_table("CREATE TABLE oranges\n(\n\tid integer primary key "
                                           "autoincrement,\n\tname text,\n\tdescription text\n)");
        assert(cols.size() == 3);
        assert(cols[0] == "id");
        assert(cols[1] == "name");
        assert(cols[2] == "description");
    }

    {
        auto db = Database::open("sample.db");
        assert(db.has_value());
        auto vals = db->read_column_values(2, 1);
        assert(vals.size() == 4);
        assert(std::ranges::any_of(vals, [](auto v) { return v == "Granny Smith"; }));
        assert(std::ranges::any_of(vals, [](auto v) { return v == "Fuji"; }));
        assert(std::ranges::any_of(vals, [](auto v) { return v == "Honeycrisp"; }));
        assert(std::ranges::any_of(vals, [](auto v) { return v == "Golden Delicious"; }));
    }

    {
        auto db = Database::open("sample.db");
        assert(db.has_value());
        auto vals = db->read_column_values(2, 2);
        assert(vals.size() == 4);
        assert(std::ranges::any_of(vals, [](auto v) { return v == "Light Green"; }));
        assert(std::ranges::any_of(vals, [](auto v) { return v == "Red"; }));
        assert(std::ranges::any_of(vals, [](auto v) { return v == "Blush Red"; }));
        assert(std::ranges::any_of(vals, [](auto v) { return v == "Yellow"; }));
    }

    {
        auto db = Database::open("sample.db");
        assert(db.has_value());
        std::ostringstream out;
        handle_command(*db, "SELECT name FROM apples", out);
        auto output = out.str();
        assert(output.find("Granny Smith") != std::string::npos);
        assert(output.find("Fuji") != std::string::npos);
        assert(output.find("Honeycrisp") != std::string::npos);
        assert(output.find("Golden Delicious") != std::string::npos);
    }

    {
        auto db = Database::open("sample.db");
        assert(db.has_value());
        std::ostringstream out;
        handle_command(*db, "SELECT name, color FROM apples", out);
        auto output = out.str();
        assert(output.find("Granny Smith|Light Green") != std::string::npos);
        assert(output.find("Fuji|Red") != std::string::npos);
        assert(output.find("Honeycrisp|Blush Red") != std::string::npos);
        assert(output.find("Golden Delicious|Yellow") != std::string::npos);
    }

    return EXIT_SUCCESS;
}
