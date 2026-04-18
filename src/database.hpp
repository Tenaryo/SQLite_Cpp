#pragma once

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <fstream>
#include <string>
#include <string_view>
#include <vector>

class Database {
    std::vector<std::byte> data_;
    uint32_t page_size_;
    uint32_t num_tables_;

    explicit Database(std::vector<std::byte> data)
        : data_(std::move(data)), page_size_(read_u16_be(16)), num_tables_(read_u16_be(103)) {}

    auto read_u16_be(size_t offset) const noexcept -> uint16_t {
        return static_cast<uint16_t>(data_[offset]) << 8 | static_cast<uint16_t>(data_[offset + 1]);
    }

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

    auto parse_table_name(size_t cell_offset) const -> std::string_view {
        auto pos = cell_offset;
        pos += read_varint(pos).consumed;
        pos += read_varint(pos).consumed;

        auto header_start = pos;
        auto header_size_vr = read_varint(pos);
        pos += header_size_vr.consumed;

        uint64_t serial_types[3];
        for (int i = 0; i < 3; ++i) {
            auto vr = read_varint(pos);
            serial_types[i] = vr.value;
            pos += vr.consumed;
        }

        auto body_start = header_start + header_size_vr.value;
        size_t body_offset = 0;
        for (int i = 0; i < 2; ++i)
            body_offset += serial_type_size(serial_types[i]);

        auto tbl_name_size = serial_type_size(serial_types[2]);
        return {reinterpret_cast<const char*>(data_.data() + body_start + body_offset),
                tbl_name_size};
    }
  public:
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
        names.reserve(num_tables_);
        for (auto offset : cell_offsets()) {
            auto name = parse_table_name(offset);
            if (!name.starts_with("sqlite_"))
                names.push_back(name);
        }
        std::ranges::sort(names);
        return names;
    }
};

auto handle_command(const Database& db, std::string_view command, std::ostream& out) -> void {
    if (command == ".dbinfo") {
        out << "database page size: " << db.page_size() << '\n'
            << "number of tables: " << db.num_tables() << '\n';
    } else if (command == ".tables") {
        for (const auto& name : db.table_names())
            out << name << ' ';
        out << '\n';
    }
}
