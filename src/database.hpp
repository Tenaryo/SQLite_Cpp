#pragma once

#include <algorithm>
#include <array>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <format>
#include <fstream>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

struct WhereClause {
    std::string_view column;
    std::string_view value;
};

struct ColumnFilter {
    size_t column_index;
    std::string_view value;
};

struct SelectStatement {
    std::vector<std::string_view> columns;
    std::string_view table;
    bool is_count = false;
    std::optional<WhereClause> where;
};

auto parse_select(std::string_view sql) -> std::expected<SelectStatement, std::string> {
    auto to_lower = [](char c) {
        return static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    };

    auto find_ci = [&](std::string_view haystack, std::string_view needle) -> size_t {
        if (haystack.size() < needle.size())
            return std::string_view::npos;
        for (size_t i = 0; i <= haystack.size() - needle.size(); ++i) {
            bool match = true;
            for (size_t j = 0; j < needle.size(); ++j) {
                if (to_lower(haystack[i + j]) != to_lower(needle[j])) {
                    match = false;
                    break;
                }
            }
            if (match)
                return i;
        }
        return std::string_view::npos;
    };

    auto from_pos = find_ci(sql, "FROM");
    if (from_pos == std::string_view::npos)
        return std::unexpected("Invalid SELECT: missing FROM");

    auto after_select = sql.find_first_not_of(' ', 6);
    auto col_region = sql.substr(after_select, from_pos - after_select);
    while (!col_region.empty() && col_region.back() == ' ')
        col_region.remove_suffix(1);

    bool is_count = find_ci(col_region, "COUNT(*)") != std::string_view::npos;

    std::vector<std::string_view> columns;
    if (!is_count) {
        size_t pos = 0;
        while (pos < col_region.size()) {
            auto comma = col_region.find(',', pos);
            auto end = comma == std::string_view::npos ? col_region.size() : comma;
            auto col = col_region.substr(pos, end - pos);
            while (!col.empty() && col.front() == ' ')
                col.remove_prefix(1);
            while (!col.empty() && col.back() == ' ')
                col.remove_suffix(1);
            columns.push_back(col);
            pos = end + 1;
        }
    }

    auto tbl_start = sql.find_first_not_of(' ', from_pos + 4);

    auto remaining = sql.substr(tbl_start);

    std::optional<WhereClause> where;
    auto where_pos = find_ci(remaining, "WHERE");
    std::string_view table_name;
    if (where_pos != std::string_view::npos) {
        table_name = remaining.substr(0, where_pos);
        while (!table_name.empty() && table_name.back() == ' ')
            table_name.remove_suffix(1);

        auto after_where = remaining.find_first_not_of(' ', where_pos + 5);
        auto condition = remaining.substr(after_where);

        auto eq_pos = condition.find('=');
        if (eq_pos != std::string_view::npos) {
            auto col = condition.substr(0, eq_pos);
            while (!col.empty() && col.back() == ' ')
                col.remove_suffix(1);

            auto val = condition.substr(eq_pos + 1);
            while (!val.empty() && val.front() == ' ')
                val.remove_prefix(1);
            while (!val.empty() && val.back() == ' ')
                val.remove_suffix(1);
            if (val.size() >= 2 && val.front() == '\'' && val.back() == '\'')
                val = val.substr(1, val.size() - 2);

            where = WhereClause{.column = col, .value = val};
        }
    } else {
        table_name = remaining;
    }

    return SelectStatement{
        .columns = std::move(columns),
        .table = table_name,
        .is_count = is_count,
        .where = std::move(where),
    };
}

class Database {
    std::vector<std::byte> data_;
    uint32_t page_size_;
    uint32_t num_tables_;

    struct SchemaEntry {
        std::string_view type;
        std::string_view name;
        uint32_t rootpage;
        std::string_view sql;
    };

    std::vector<SchemaEntry> schema_;

    explicit Database(std::vector<std::byte> data)
        : data_(std::move(data)), page_size_(read_u16_be(16)), num_tables_(read_u16_be(103)) {
        for (auto offset : cell_offsets())
            schema_.push_back(parse_schema_entry(offset));
    }

    auto read_u16_be(size_t offset) const noexcept -> uint16_t {
        return static_cast<uint16_t>(data_[offset]) << 8 | static_cast<uint16_t>(data_[offset + 1]);
    }

    auto read_u32_be(size_t offset) const noexcept -> uint32_t {
        return static_cast<uint32_t>(data_[offset]) << 24 |
               static_cast<uint32_t>(data_[offset + 1]) << 16 |
               static_cast<uint32_t>(data_[offset + 2]) << 8 |
               static_cast<uint32_t>(data_[offset + 3]);
    }

    static constexpr uint8_t kTableInterior = 0x05;
    static constexpr uint8_t kTableLeaf = 0x0D;
    static constexpr uint8_t kIndexInterior = 0x02;
    static constexpr uint8_t kIndexLeaf = 0x0A;

    struct VarintResult {
        uint64_t value;
        size_t consumed;
    };

    auto read_varint(size_t offset) const noexcept -> VarintResult {
        uint64_t value = 0;
        for (int i = 0; i < 9; ++i) {
            auto byte = static_cast<uint8_t>(data_[offset + static_cast<size_t>(i)]);
            if (i == 8) {
                value = (value << 8) | byte;
                return {value, 9};
            }
            value = (value << 7) | static_cast<uint64_t>(byte & 0x7F);
            if ((byte & 0x80) == 0)
                return {value, static_cast<size_t>(i) + 1};
        }
        __builtin_unreachable();
    }

    static auto serial_type_size(uint64_t serial_type) noexcept -> size_t {
        if (serial_type <= 4)
            return std::array{0, 1, 2, 3, 4}[serial_type];
        if (serial_type == 5)
            return 6;
        if (serial_type == 6 || serial_type == 7)
            return 8;
        if (serial_type >= 12 && serial_type % 2 == 0)
            return static_cast<size_t>((serial_type - 12) / 2);
        if (serial_type >= 13 && serial_type % 2 == 1)
            return static_cast<size_t>((serial_type - 13) / 2);
        return 0;
    }

    auto cell_offsets() const -> std::vector<uint16_t> {
        static constexpr size_t kCellPtrArrayStart = 108;
        std::vector<uint16_t> offsets;
        offsets.reserve(num_tables_);
        for (uint32_t i = 0; i < num_tables_; ++i)
            offsets.push_back(read_u16_be(kCellPtrArrayStart + i * 2));
        return offsets;
    }

    auto parse_schema_entry(size_t cell_offset) const -> SchemaEntry {
        auto pos = cell_offset;
        pos += read_varint(pos).consumed;
        pos += read_varint(pos).consumed;

        auto header_start = pos;
        auto header_size_vr = read_varint(pos);
        pos += header_size_vr.consumed;

        static constexpr int kNumFields = 5;
        uint64_t serial_types[kNumFields];
        for (int i = 0; i < kNumFields; ++i) {
            auto vr = read_varint(pos);
            serial_types[i] = vr.value;
            pos += vr.consumed;
        }

        auto body = header_start + header_size_vr.value;
        size_t off = 0;

        auto read_text = [&](int idx) -> std::string_view {
            auto sz = serial_type_size(serial_types[idx]);
            auto sv =
                std::string_view{reinterpret_cast<const char*>(data_.data() + body + off), sz};
            off += sz;
            return sv;
        };

        auto read_uint = [&](int idx) -> uint32_t {
            auto sz = serial_type_size(serial_types[idx]);
            uint32_t val = 0;
            for (size_t j = 0; j < sz; ++j)
                val = (val << 8) | static_cast<uint8_t>(data_[body + off + j]);
            off += sz;
            return val;
        };

        auto type = read_text(0);
        auto name = read_text(1);
        read_text(2);
        auto rootpage = serial_types[3] == 0 ? 0u : read_uint(3);
        auto sql = serial_types[4] == 0 ? std::string_view{} : read_text(4);

        return {.type = type, .name = name, .rootpage = rootpage, .sql = sql};
    }

    auto parse_index_cell(size_t cell_offset) const -> std::pair<std::string_view, uint64_t> {
        auto pos = cell_offset;
        pos += read_varint(pos).consumed;

        auto header_start = pos;
        auto header_size_vr = read_varint(pos);
        pos += header_size_vr.consumed;

        auto st0 = read_varint(pos);
        pos += st0.consumed;
        auto st1 = read_varint(pos);
        pos += st1.consumed;

        auto body = header_start + header_size_vr.value;
        auto sz0 = serial_type_size(st0.value);
        auto col_val = std::string_view{reinterpret_cast<const char*>(data_.data() + body), sz0};

        auto sz1 = serial_type_size(st1.value);
        uint64_t rowid = 0;
        for (size_t j = 0; j < sz1; ++j)
            rowid = (rowid << 8) | static_cast<uint8_t>(data_[body + sz0 + j]);

        return {col_val, rowid};
    }

    auto parse_row_columns(size_t pos,
                           std::span<const size_t> column_indices,
                           uint64_t rowid) const -> std::vector<std::string> {
        auto header_start = pos;
        auto header_size_vr = read_varint(pos);
        pos += header_size_vr.consumed;
        auto header_end = header_start + header_size_vr.value;

        auto body_base = header_start + header_size_vr.value;
        size_t body_offset = 0;

        size_t max_col = column_indices.empty() ? 0 : *std::ranges::max_element(column_indices);
        std::vector<std::string> row(column_indices.size());

        for (size_t col = 0; pos < header_end && col <= max_col; ++col) {
            auto vr = read_varint(pos);
            pos += vr.consumed;
            auto sz = serial_type_size(vr.value);

            auto resolve = [&]() -> std::string {
                if (vr.value == 0)
                    return std::to_string(rowid);
                if (vr.value == 8)
                    return "0";
                if (vr.value == 9)
                    return "1";
                if (vr.value >= 1 && vr.value <= 6) {
                    uint64_t ival = 0;
                    for (size_t j = 0; j < sz; ++j)
                        ival =
                            (ival << 8) | static_cast<uint8_t>(data_[body_base + body_offset + j]);
                    return std::to_string(ival);
                }
                if (vr.value >= 13 && vr.value % 2 == 1) {
                    return std::string{
                        reinterpret_cast<const char*>(data_.data() + body_base + body_offset), sz};
                }
                return {};
            };

            auto val = resolve();
            auto it = std::ranges::find(column_indices, col);
            if (it != column_indices.end()) {
                auto idx = static_cast<size_t>(it - column_indices.begin());
                row[idx] = std::move(val);
            }
            body_offset += sz;
        }
        return row;
    }
  public:
    auto index_search(uint32_t page_number,
                      std::string_view target) const -> std::vector<uint64_t> {
        auto page_offset = static_cast<size_t>(page_number - 1) * page_size_;
        auto type = static_cast<uint8_t>(data_[page_offset]);

        if (type == kIndexLeaf) {
            auto num_cells = read_u16_be(page_offset + 3);
            std::vector<uint64_t> rowids;
            for (uint32_t i = 0; i < num_cells; ++i) {
                auto cell_ptr = page_offset + read_u16_be(page_offset + 8 + i * 2);
                auto [val, rowid] = parse_index_cell(cell_ptr);
                if (val == target) {
                    rowids.push_back(rowid);
                } else if (val > target) {
                    break;
                }
            }
            return rowids;
        }

        if (type == kIndexInterior) {
            auto num_cells = read_u16_be(page_offset + 3);
            auto right_child = read_u32_be(page_offset + 8);
            std::vector<uint64_t> rowids;

            for (uint32_t i = 0; i < num_cells; ++i) {
                auto cell_ptr = page_offset + read_u16_be(page_offset + 12 + i * 2);
                auto left_child = read_u32_be(cell_ptr);
                auto [val, rowid] = parse_index_cell(cell_ptr + 4);

                if (val < target) {
                    continue;
                } else if (val == target) {
                    rowids.push_back(rowid);
                    auto child_rowids = index_search(left_child, target);
                    for (auto r : child_rowids)
                        rowids.push_back(r);
                } else {
                    auto child_rowids = index_search(left_child, target);
                    for (auto r : child_rowids)
                        rowids.push_back(r);
                    return rowids;
                }
            }
            auto child_rowids = index_search(right_child, target);
            for (auto r : child_rowids)
                rowids.push_back(r);
            return rowids;
        }

        return {};
    }

    auto read_row_by_rowid(uint32_t page_number,
                           uint64_t target_rowid,
                           std::span<const size_t> column_indices) const
        -> std::optional<std::vector<std::string>> {
        auto page_offset = static_cast<size_t>(page_number - 1) * page_size_;
        auto type = static_cast<uint8_t>(data_[page_offset]);

        if (type == kTableInterior) {
            auto num_cells = read_u16_be(page_offset + 3);
            auto right_child = read_u32_be(page_offset + 8);

            for (uint32_t i = 0; i < num_cells; ++i) {
                auto cell_ptr = page_offset + read_u16_be(page_offset + 12 + i * 2);
                auto child = read_u32_be(cell_ptr);
                auto key_vr = read_varint(cell_ptr + 4);

                if (target_rowid <= key_vr.value) {
                    return read_row_by_rowid(child, target_rowid, column_indices);
                }
            }
            return read_row_by_rowid(right_child, target_rowid, column_indices);
        }

        auto num_cells = read_u16_be(page_offset + 3);
        for (uint32_t i = 0; i < num_cells; ++i) {
            auto cell_ptr = page_offset + read_u16_be(page_offset + 8 + i * 2);
            auto pos = cell_ptr;
            pos += read_varint(pos).consumed;
            auto rowid_vr = read_varint(pos);
            pos += rowid_vr.consumed;

            if (rowid_vr.value == target_rowid) {
                return parse_row_columns(pos, column_indices, rowid_vr.value);
            }
            if (rowid_vr.value > target_rowid) {
                break;
            }
        }
        return std::nullopt;
    }

    static auto open(std::string_view path) -> std::expected<Database, std::string> {
        std::ifstream file(std::string(path), std::ios::binary);
        if (!file)
            return std::unexpected("Failed to open database file");

        file.seekg(0, std::ios::end);
        auto size = file.tellg();
        file.seekg(0);

        std::vector<std::byte> buf(static_cast<size_t>(size));
        file.read(reinterpret_cast<char*>(buf.data()), size);
        if (!file)
            return std::unexpected("Failed to read database file");

        return Database(std::move(buf));
    }

    auto page_size() const noexcept -> uint32_t { return page_size_; }
    auto num_tables() const noexcept -> uint32_t { return num_tables_; }

    auto table_names() const -> std::vector<std::string_view> {
        std::vector<std::string_view> names;
        for (const auto& entry : schema_)
            if (!entry.name.starts_with("sqlite_"))
                names.push_back(entry.name);
        std::ranges::sort(names);
        return names;
    }

    auto rootpage(std::string_view table_name) const -> std::expected<uint32_t, std::string> {
        for (const auto& entry : schema_)
            if (std::ranges::equal(entry.name, table_name, [](char a, char b) {
                    return std::tolower(a) == std::tolower(b);
                }))
                return entry.rootpage;
        return std::unexpected(std::format("Error: no such table: {}", table_name));
    }

    auto index_rootpage(std::string_view table_name, std::string_view column_name) const
        -> std::expected<uint32_t, std::string> {
        for (const auto& entry : schema_) {
            if (entry.type != "index")
                continue;
            if (entry.sql.find(table_name) == std::string_view::npos)
                continue;
            auto to_lower = [](char c) {
                return static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
            };
            bool found = false;
            auto& sql = entry.sql;
            for (size_t i = 0; i + column_name.size() <= sql.size(); ++i) {
                bool match = true;
                for (size_t j = 0; j < column_name.size(); ++j) {
                    if (to_lower(sql[i + j]) != to_lower(column_name[j])) {
                        match = false;
                        break;
                    }
                }
                if (match) {
                    found = true;
                    break;
                }
            }
            if (found)
                return entry.rootpage;
        }
        return std::unexpected(std::format("No index for {}.{}", table_name, column_name));
    }

    auto count_rows(uint32_t page_number) const -> uint32_t {
        auto page_offset = static_cast<size_t>(page_number - 1) * page_size_;
        auto type = static_cast<uint8_t>(data_[page_offset]);

        if (type == kTableLeaf)
            return read_u16_be(page_offset + 3);

        auto num_cells = read_u16_be(page_offset + 3);
        auto right_child = read_u32_be(page_offset + 8);
        uint32_t total = count_rows(right_child);
        for (uint32_t i = 0; i < num_cells; ++i) {
            auto cell_ptr = page_offset + read_u16_be(page_offset + 12 + i * 2);
            auto child = read_u32_be(cell_ptr);
            total += count_rows(child);
        }
        return total;
    }

    auto table_sql(std::string_view table_name) const -> std::string_view {
        for (const auto& entry : schema_)
            if (std::ranges::equal(entry.name, table_name, [](char a, char b) {
                    return std::tolower(a) == std::tolower(b);
                }))
                return entry.sql;
        return {};
    }

    auto parse_create_table(std::string_view sql) const -> std::vector<std::string_view> {
        auto paren_open = sql.find('(');
        auto paren_close = sql.rfind(')');
        auto body = sql.substr(paren_open + 1, paren_close - paren_open - 1);

        std::vector<std::string_view> columns;
        size_t pos = 0;
        while (pos < body.size()) {
            auto comma = body.find(',', pos);
            auto end = comma == std::string_view::npos ? body.size() : comma;
            auto def = body.substr(pos, end - pos);

            auto trim = [](std::string_view s) {
                while (!s.empty() && (s.front() == ' ' || s.front() == '\t' || s.front() == '\n' ||
                                      s.front() == '\r'))
                    s.remove_prefix(1);
                while (!s.empty() && (s.back() == ' ' || s.back() == '\t' || s.back() == '\n' ||
                                      s.back() == '\r'))
                    s.remove_suffix(1);
                return s;
            };

            auto trimmed = trim(def);
            auto space = trimmed.find_first_of(" \t\n\r");
            if (space != std::string_view::npos)
                columns.push_back(trimmed.substr(0, space));

            pos = end + 1;
        }
        return columns;
    }

    auto read_columns_values(uint32_t page_number,
                             std::span<const size_t> column_indices,
                             const ColumnFilter* filter = nullptr) const
        -> std::vector<std::vector<std::string>> {
        auto page_offset = static_cast<size_t>(page_number - 1) * page_size_;
        auto type = static_cast<uint8_t>(data_[page_offset]);

        if (type == kTableInterior) {
            auto num_cells = read_u16_be(page_offset + 3);
            auto right_child = read_u32_be(page_offset + 8);

            std::vector<std::vector<std::string>> rows;
            for (uint32_t i = 0; i < num_cells; ++i) {
                auto cell_ptr = page_offset + read_u16_be(page_offset + 12 + i * 2);
                auto child = read_u32_be(cell_ptr);
                auto child_rows = read_columns_values(child, column_indices, filter);
                for (auto& r : child_rows)
                    rows.push_back(std::move(r));
            }
            auto right_rows = read_columns_values(right_child, column_indices, filter);
            for (auto& r : right_rows)
                rows.push_back(std::move(r));
            return rows;
        }

        auto num_cells = read_u16_be(page_offset + 3);

        size_t max_col = column_indices.empty() ? 0 : *std::ranges::max_element(column_indices);
        if (filter)
            max_col = std::max(max_col, filter->column_index);

        std::vector<std::vector<std::string>> rows;
        rows.reserve(num_cells);

        for (uint32_t i = 0; i < num_cells; ++i) {
            auto cell_ptr = page_offset + read_u16_be(page_offset + 8 + i * 2);

            auto pos = cell_ptr;
            pos += read_varint(pos).consumed;
            auto rowid_vr = read_varint(pos);
            pos += rowid_vr.consumed;

            auto header_start = pos;
            auto header_size_vr = read_varint(pos);
            pos += header_size_vr.consumed;
            auto header_end = header_start + header_size_vr.value;

            auto body_base = header_start + header_size_vr.value;
            size_t body_offset = 0;

            std::vector<std::string> row(column_indices.size());
            std::string filter_val;

            for (size_t col = 0; pos < header_end && col <= max_col; ++col) {
                auto vr = read_varint(pos);
                pos += vr.consumed;
                auto sz = serial_type_size(vr.value);

                auto resolve = [&]() -> std::string {
                    if (vr.value == 0)
                        return std::to_string(rowid_vr.value);
                    if (vr.value == 8)
                        return "0";
                    if (vr.value == 9)
                        return "1";
                    if (vr.value >= 1 && vr.value <= 6) {
                        uint64_t ival = 0;
                        for (size_t j = 0; j < sz; ++j)
                            ival = (ival << 8) |
                                   static_cast<uint8_t>(data_[body_base + body_offset + j]);
                        return std::to_string(ival);
                    }
                    if (vr.value >= 13 && vr.value % 2 == 1) {
                        return std::string{
                            reinterpret_cast<const char*>(data_.data() + body_base + body_offset),
                            sz};
                    }
                    return {};
                };

                auto val = resolve();

                auto it = std::ranges::find(column_indices, col);
                if (it != column_indices.end()) {
                    auto idx = static_cast<size_t>(it - column_indices.begin());
                    row[idx] = std::move(val);
                    if (filter && col == filter->column_index)
                        filter_val = row[idx];
                } else if (filter && col == filter->column_index) {
                    filter_val = std::move(val);
                }

                body_offset += sz;
            }

            if (filter && filter_val != filter->value)
                continue;
            rows.push_back(std::move(row));
        }
        return rows;
    }
};

auto handle_command(const Database& db, std::string_view command, std::ostream& out) -> void {
    if (command.starts_with('.')) {
        if (command == ".dbinfo") {
            out << "database page size: " << db.page_size() << '\n'
                << "number of tables: " << db.num_tables() << '\n';
        } else if (command == ".tables") {
            for (const auto& name : db.table_names())
                out << name << ' ';
            out << '\n';
        }
    } else {
        auto sel = parse_select(command);
        if (!sel) {
            out << sel.error() << '\n';
            return;
        }
        auto rp = db.rootpage(sel->table);
        if (!rp) {
            out << rp.error() << '\n';
            return;
        }
        if (sel->is_count) {
            out << db.count_rows(*rp) << '\n';
        } else {
            auto sql = db.table_sql(sel->table);
            auto table_cols = db.parse_create_table(sql);

            std::vector<size_t> col_indices;
            col_indices.reserve(sel->columns.size());
            for (const auto& col_name : sel->columns) {
                for (size_t i = 0; i < table_cols.size(); ++i) {
                    if (std::ranges::equal(table_cols[i], col_name, [](char a, char b) {
                            return std::tolower(a) == std::tolower(b);
                        })) {
                        col_indices.push_back(i);
                        break;
                    }
                }
            }

            ColumnFilter filter_storage{0, {}};
            const ColumnFilter* filter = nullptr;
            if (sel->where) {
                for (size_t i = 0; i < table_cols.size(); ++i) {
                    if (std::ranges::equal(table_cols[i], sel->where->column, [](char a, char b) {
                            return std::tolower(a) == std::tolower(b);
                        })) {
                        filter_storage.column_index = i;
                        filter_storage.value = sel->where->value;
                        filter = &filter_storage;
                        break;
                    }
                }
            }

            if (sel->where) {
                auto idx_rp = db.index_rootpage(sel->table, sel->where->column);
                if (idx_rp) {
                    auto rowids = db.index_search(*idx_rp, sel->where->value);
                    std::ranges::sort(rowids);
                    for (auto rid : rowids) {
                        auto row = db.read_row_by_rowid(*rp, rid, col_indices);
                        if (row) {
                            for (size_t i = 0; i < row->size(); ++i) {
                                if (i > 0)
                                    out << '|';
                                out << (*row)[i];
                            }
                            out << '\n';
                        }
                    }
                    return;
                }
            }

            auto rows = db.read_columns_values(*rp, col_indices, filter);
            for (const auto& row : rows) {
                for (size_t i = 0; i < row.size(); ++i) {
                    if (i > 0)
                        out << '|';
                    out << row[i];
                }
                out << '\n';
            }
        }
    }
}
