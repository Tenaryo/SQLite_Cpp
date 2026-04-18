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
        const auto idx = std::vector<size_t>{1};
        auto rows = db->read_columns_values(2, idx);
        assert(rows.size() == 4);
        assert(std::ranges::any_of(rows, [](auto& r) { return r[0] == "Granny Smith"; }));
        assert(std::ranges::any_of(rows, [](auto& r) { return r[0] == "Fuji"; }));
        assert(std::ranges::any_of(rows, [](auto& r) { return r[0] == "Honeycrisp"; }));
        assert(std::ranges::any_of(rows, [](auto& r) { return r[0] == "Golden Delicious"; }));
    }

    {
        auto db = Database::open("sample.db");
        assert(db.has_value());
        const auto idx = std::vector<size_t>{2};
        auto rows = db->read_columns_values(2, idx);
        assert(rows.size() == 4);
        assert(std::ranges::any_of(rows, [](auto& r) { return r[0] == "Light Green"; }));
        assert(std::ranges::any_of(rows, [](auto& r) { return r[0] == "Red"; }));
        assert(std::ranges::any_of(rows, [](auto& r) { return r[0] == "Blush Red"; }));
        assert(std::ranges::any_of(rows, [](auto& r) { return r[0] == "Yellow"; }));
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

    {
        auto sel = parse_select("SELECT name, color FROM apples WHERE color = 'Yellow'");
        assert(sel.has_value());
        assert(sel->columns.size() == 2);
        assert(sel->columns[0] == "name");
        assert(sel->columns[1] == "color");
        assert(sel->table == "apples");
        assert(!sel->is_count);
        assert(sel->where.has_value());
        assert(sel->where->column == "color");
        assert(sel->where->value == "Yellow");
    }

    {
        auto sel = parse_select("SELECT name FROM apples");
        assert(sel.has_value());
        assert(!sel->where.has_value());
    }

    {
        auto db = Database::open("sample.db");
        assert(db.has_value());
        std::ostringstream out;
        handle_command(*db, "SELECT name, color FROM apples WHERE color = 'Yellow'", out);
        assert(out.str() == "Golden Delicious|Yellow\n");
    }

    {
        auto db = Database::open("sample.db");
        assert(db.has_value());
        std::ostringstream out;
        handle_command(*db, "SELECT name, color FROM apples WHERE color = 'Purple'", out);
        assert(out.str().empty());
    }

    {
        auto db = Database::open("sample.db");
        assert(db.has_value());
        std::ostringstream out;
        handle_command(*db, "SELECT name FROM apples WHERE color = 'Yellow'", out);
        assert(out.str() == "Golden Delicious\n");
    }

    {
        auto db = Database::open("superheroes.db");
        assert(db.has_value());
        std::ostringstream out;
        handle_command(*db, "SELECT id, name FROM superheroes WHERE eye_color = 'Pink Eyes'", out);
        assert(out.str() == "297|Stealth (New Earth)\n"
                            "790|Tobias Whale (New Earth)\n"
                            "1085|Felicity (New Earth)\n"
                            "2729|Thrust (New Earth)\n"
                            "3289|Angora Lapin (New Earth)\n"
                            "3913|Matris Ater Clementia (New Earth)\n");
    }

    {
        auto db = Database::open("superheroes.db");
        assert(db.has_value());
        std::ostringstream out;
        handle_command(*db, "SELECT COUNT(*) FROM superheroes", out);
        assert(out.str() == "6895\n");
    }

    {
        auto db = Database::open("superheroes.db");
        assert(db.has_value());
        std::ostringstream out;
        handle_command(*db, "SELECT id FROM superheroes", out);
        auto output = out.str();
        auto count = std::ranges::count(output, '\n');
        assert(count == 6895);
    }

    return EXIT_SUCCESS;
}
