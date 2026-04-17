#pragma once

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
};
