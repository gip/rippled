#pragma once

#include <xrpl/beast/utility/Journal.h>
#include <xrpl/config/BasicConfig.h>
#include <xrpl/json/json_value.h>

#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>

namespace xrpl {

// An optional, application-owned durable outbox with its own HTTP worker.
class LedgerExporter
{
    struct Impl;
    std::unique_ptr<Impl> impl_;

public:
    LedgerExporter(
        Section const& config,
        std::filesystem::path const& dataDir,
        std::string const& sslVerifyDir,
        std::string const& sslVerifyFile,
        beast::Journal journal);

    ~LedgerExporter();

    LedgerExporter(LedgerExporter const&) = delete;
    LedgerExporter&
    operator=(LedgerExporter const&) = delete;

    [[nodiscard]] bool
    enabled() const noexcept;

    void
    start() noexcept;

    void
    stop() noexcept;

    // Success means the complete payload is durably queued, not delivered.
    bool
    enqueue(std::uint32_t ledger, std::string const& hash, json::Value const& payload) noexcept;

    void
    reportGap(std::uint32_t ledger, std::string const& hash, std::string const& reason) noexcept;

    [[nodiscard]] json::Value
    getStatus() const;
};

}  // namespace xrpl
