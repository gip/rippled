#include <xrpl/ledger/LedgerChanges.h>

#include <xrpl/basics/strHex.h>
#include <xrpl/ledger/ReadView.h>
#include <xrpl/protocol/TxMeta.h>

#include <algorithm>
#include <stdexcept>
#include <utility>

namespace xrpl {

namespace {

std::optional<LedgerEntrySnapshot>
snapshot(SLE::const_pointer const& sle)
{
    if (!sle)
        return std::nullopt;
    Serializer s;
    sle->add(s);
    return LedgerEntrySnapshot{s.modData(), sle->getJson()};
}

json::Value
snapshotJson(std::optional<LedgerEntrySnapshot> const& value)
{
    if (!value)
        return json::Value{};
    json::Value result{json::ValueType::Object};
    result["json"] = value->json;
    result["blob"] = strHex(value->data);
    return result;
}

}  // namespace

StateChangeSet::StateChangeSet(std::shared_ptr<LedgerChangeStatus> status) noexcept
    : status_(std::move(status))
{
}

void
StateChangeSet::record(
    uint256 const& key,
    SLE::const_pointer const& before,
    SLE::const_pointer const& after) noexcept
{
    if (status_->failed)
        return;
    try
    {
        auto [it, inserted] = changes_.try_emplace(key);
        if (inserted)
            it->second.before = snapshot(before);
        it->second.after = snapshot(after);
        if (after || before)
            it->second.type = (after ? after : before)->getType();
    }
    catch (...)
    {
        status_->failed = true;
        changes_.clear();
    }
}

json::Value
StateChangeSet::getJson() const
{
    json::Value result{json::ValueType::Array};
    for (auto const& [key, change] : changes_)
    {
        if ((!change.before && !change.after) ||
            (change.before && change.after && change.before->data == change.after->data))
            continue;
        json::Value item{json::ValueType::Object};
        item["key"] = to_string(key);
        item["entry_type"] = change.type;
        item["operation"] = !change.before ? "insert" : !change.after ? "delete" : "modify";
        item["before"] = snapshotJson(change.before);
        item["after"] = snapshotJson(change.after);
        result.append(std::move(item));
    }
    return result;
}

json::Value
TransactionChanges::getJson() const
{
    json::Value resultJson{json::ValueType::Object};
    resultJson["hash"] = to_string(hash);
    resultJson["TransactionIndex"] = index;
    resultJson["result"] = transToken(result);
    if (parentBatchId)
        resultJson["parent_batch_id"] = to_string(*parentBatchId);
    resultJson["transaction"] = transaction;
    resultJson["transaction_blob"] = strHex(transactionData);
    resultJson["metadata"] = metadata;
    resultJson["metadata_blob"] = strHex(metadataData);
    resultJson["changes"] = changes.getJson();
    return resultJson;
}

json::Value
LedgerChanges::getJson(ReadView const& parent, ReadView const& ledger, std::uint32_t networkId)
{
    if (status->failed)
        throw std::runtime_error("ledger change capture failed");
    std::ranges::sort(transactions, {}, &TransactionChanges::index);
    std::size_t count = 0;
    for ([[maybe_unused]] auto const& tx : ledger.txs)
        ++count;
    if (count != transactions.size())
        throw std::runtime_error("ledger change capture is incomplete");
    for (std::size_t i = 0; i < transactions.size(); ++i)
        if (transactions[i].index != i)
            throw std::runtime_error("ledger change capture has invalid transaction order");

    auto const& header = ledger.header();
    json::Value result{json::ValueType::Object};
    result["schema_version"] = 1;
    result["network_id"] = networkId;
    result["ledger_index"] = header.seq;
    result["ledger_hash"] = to_string(header.hash);
    result["parent_ledger_hash"] = to_string(header.parentHash);
    result["close_time"] = json::Value::UInt(header.closeTime.time_since_epoch().count());
    result["parent_state_root"] = to_string(parent.header().accountHash);
    result["state_root"] = to_string(header.accountHash);
    result["stage"] = "accepted";
    result["pre_transaction_changes"] = pre.getJson();
    auto& txs = result["transactions"] = json::ValueType::Array;
    for (auto const& tx : transactions)
        txs.append(tx.getJson());
    result["post_transaction_changes"] = post.getJson();
    return result;
}

}  // namespace xrpl
