#pragma once

#include <xrpl/basics/Blob.h>
#include <xrpl/basics/base_uint.h>
#include <xrpl/json/json_value.h>
#include <xrpl/protocol/STLedgerEntry.h>
#include <xrpl/protocol/STTx.h>
#include <xrpl/protocol/Serializer.h>
#include <xrpl/protocol/TER.h>

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <vector>

namespace xrpl {

class ReadView;

// Shared by speculative views. Recording failures must not affect execution.
struct LedgerChangeStatus
{
    bool failed = false;
};

struct LedgerEntrySnapshot
{
    Blob data;
    json::Value json;
};

class StateChangeSet
{
    struct Change
    {
        std::optional<LedgerEntrySnapshot> before;
        std::optional<LedgerEntrySnapshot> after;
        std::uint16_t type = 0;
    };

    std::shared_ptr<LedgerChangeStatus> status_;
    std::map<uint256, Change> changes_;

public:
    explicit StateChangeSet(std::shared_ptr<LedgerChangeStatus> status) noexcept;

    // Preserve the first before value and the last after value for each key.
    void
    record(
        uint256 const& key,
        SLE::const_pointer const& before,
        SLE::const_pointer const& after) noexcept;

    [[nodiscard]] json::Value
    getJson() const;
};

struct TransactionChanges
{
    uint256 hash;
    std::uint32_t index;
    TER result;
    std::optional<uint256> parentBatchId;
    json::Value transaction;
    Blob transactionData;
    Blob metadataData;
    StateChangeSet changes;

    [[nodiscard]] json::Value
    getJson() const;
};

struct LedgerChanges
{
    std::shared_ptr<LedgerChangeStatus> status = std::make_shared<LedgerChangeStatus>();
    StateChangeSet pre{status};
    std::vector<TransactionChanges> transactions;
    StateChangeSet post{status};

    [[nodiscard]] json::Value
    getJson(ReadView const& parent, ReadView const& ledger, std::uint32_t networkId);
};

}  // namespace xrpl
