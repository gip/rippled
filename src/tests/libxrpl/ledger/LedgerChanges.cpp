#include <xrpl/ledger/LedgerChanges.h>

#include <xrpl/basics/StringUtilities.h>
#include <xrpl/basics/strHex.h>
#include <xrpl/ledger/ApplyViewImpl.h>
#include <xrpl/ledger/Ledger.h>
#include <xrpl/ledger/LedgerTiming.h>
#include <xrpl/ledger/OpenView.h>
#include <xrpl/protocol/Feature.h>
#include <xrpl/protocol/Indexes.h>
#include <xrpl/protocol/STArray.h>
#include <xrpl/protocol/SeqProxy.h>
#include <xrpl/protocol/TxFlags.h>
#include <xrpl/protocol_autogen/transactions/Batch.h>
#include <xrpl/protocol_autogen/transactions/Payment.h>
#include <xrpl/tx/apply.h>

#include <gtest/gtest.h>
#include <helpers/Account.h>
#include <helpers/TestServiceRegistry.h>
#include <helpers/TxTest.h>

#include <chrono>
#include <memory>
#include <unordered_set>

namespace xrpl::test {
namespace {

class LedgerChangesTest : public ::testing::Test
{
protected:
    TestServiceRegistry registry;
    Account const alice{"export_alice"};
    Account const bob{"export_bob"};
    // Rules retains a reference to its presets; they must outlive the ledgers.
    std::unordered_set<uint256, beast::Uhash<>> features;
    std::shared_ptr<Ledger> parent;
    std::shared_ptr<Ledger> built;
    std::unique_ptr<OpenView> view;
    LedgerChanges changes;
    NetClock::time_point const now{std::chrono::seconds{100}};

    void
    SetUp() override
    {
        auto const all = allFeatures();
        foreachFeature(all, [&](uint256 const& f) { features.insert(f); });
        parent = std::make_shared<Ledger>(
            kCreateGenesis,
            Rules{features},
            Fees{XRPAmount{10}, XRPAmount{10000000}, XRPAmount{2000000}},
            std::vector<uint256>{features.begin(), features.end()},
            registry.getNodeFamily());
        built = std::make_shared<Ledger>(*parent, now);
        view = std::make_unique<OpenView>(built.get());
        view->enableChangeCapture(changes.status);
    }

    std::shared_ptr<STTx const>
    payment(std::uint32_t seq, Account const& destination, XRPAmount amount = XRP(100))
    {
        return transactions::PaymentBuilder{
            Account::kMaster, destination, amount, seq, XRPAmount{10}}
            .build(Account::kMaster.pk(), Account::kMaster.sk())
            .getSTTx();
    }

    ApplyResult
    submit(std::shared_ptr<STTx const> const& tx, ApplyFlags flags = TapNone)
    {
        return apply(registry, *view, *tx, flags, registry.getJournal("apply"));
    }

    json::Value
    finish()
    {
        view->apply(*built);
        changes.transactions = view->takeTransactionChanges();
        built->setChangeCapture(&changes.post, changes.status);
        built->updateSkipList();
        built->setChangeCapture(nullptr);
        built->setAccepted(now, kLedgerGenesisTimeResolution, true);
        return changes.getJson(*parent, *built, 0);
    }

    void
    verifyReplay(json::Value const& payload)
    {
        Ledger replay(*parent, now);
        auto applyChanges = [&](json::Value const& entries) {
            for (auto const& entry : entries)
            {
                uint256 key;
                ASSERT_TRUE(key.parseHex(entry["key"].asString()));
                auto const existing = replay.read(keylet::unchecked(key));
                if (entry["before"].isNull())
                    ASSERT_FALSE(existing);
                else
                {
                    ASSERT_TRUE(existing);
                    Serializer original;
                    existing->add(original);
                    ASSERT_EQ(strHex(original.peekData()), entry["before"]["blob"].asString());
                }
                if (entry["after"].isNull())
                    replay.rawErase(key);
                else
                {
                    auto const bytes = strUnHex(entry["after"]["blob"].asString());
                    ASSERT_TRUE(bytes);
                    SerialIter sit(makeSlice(*bytes));
                    auto sle = std::make_shared<SLE>(sit, key);
                    if (existing)
                        replay.rawReplace(sle);
                    else
                        replay.rawInsert(sle);
                }
            }
        };
        applyChanges(payload["pre_transaction_changes"]);
        for (auto const& transaction : payload["transactions"])
            applyChanges(transaction["changes"]);
        applyChanges(payload["post_transaction_changes"]);
        EXPECT_EQ(
            to_string(replay.stateMap().getHash().asUInt256()), payload["state_root"].asString());
    }
};

TEST_F(LedgerChangesTest, OrderedTransactionsAndFullEntriesReproduceRoot)
{
    auto const first = payment(1, alice);
    auto const second = payment(2, alice, XRP(5));
    ASSERT_EQ(submit(first).ter, tesSUCCESS);
    ASSERT_EQ(submit(second).ter, tesSUCCESS);
    auto const payload = finish();
    ASSERT_EQ(payload["transactions"].size(), 2);
    EXPECT_EQ(payload["transactions"][0u]["TransactionIndex"].asUInt(), 0);
    EXPECT_EQ(payload["transactions"][1]["TransactionIndex"].asUInt(), 1);
    verifyReplay(payload);

    Ledger reference(*parent, now);
    OpenView unrecorded(&reference);
    for (auto const& tx : {first, second})
        ASSERT_EQ(
            apply(registry, unrecorded, *tx, TapNone, registry.getJournal("apply")).ter,
            tesSUCCESS);
    unrecorded.apply(reference);
    reference.updateSkipList();
    reference.setAccepted(now, kLedgerGenesisTimeResolution, true);
    EXPECT_EQ(reference.header().hash, built->header().hash);
}

TEST_F(LedgerChangesTest, FeeClaimAndRejectedAttempt)
{
    auto const rejected = submit(payment(2, alice));
    EXPECT_FALSE(rejected.applied);
    auto const feeClaim = submit(payment(1, alice, XRPAmount{1}));
    ASSERT_TRUE(feeClaim.applied);
    ASSERT_EQ(feeClaim.ter, tecNO_DST_INSUF_XRP);
    auto const payload = finish();
    ASSERT_EQ(payload["transactions"].size(), 1);
    EXPECT_EQ(payload["transactions"][0u]["result"].asString(), "tecNO_DST_INSUF_XRP");
    EXPECT_EQ(payload["transactions"][0u]["changes"].size(), 1);
    verifyReplay(payload);
}

TEST_F(LedgerChangesTest, EmptyLedgerStillExportsSkipList)
{
    auto const payload = finish();
    EXPECT_TRUE(payload["transactions"].size() == 0);
    EXPECT_FALSE(payload["post_transaction_changes"].size() == 0);
    verifyReplay(payload);
}

TEST_F(LedgerChangesTest, DirectChangesCreateModifyDeleteAndNoOps)
{
    built->setChangeCapture(&changes.pre, changes.status);
    auto negative = std::make_shared<SLE>(keylet::negativeUNL());
    negative->setFieldVL(sfValidatorToDisable, alice.pk().slice());
    built->rawInsert(negative);
    built->rawReplace(negative);  // A duplicate write is coalesced.
    auto temporary = std::make_shared<SLE>(keylet::skip(1000));
    temporary->setFieldV256(sfHashes, STVector256{std::vector<uint256>{parent->header().hash}});
    built->rawInsert(temporary);
    built->rawErase(temporary);  // A net no-op is omitted.
    built->setChangeCapture(nullptr);
    auto const payload = finish();
    EXPECT_EQ(payload["pre_transaction_changes"].size(), 1);
    verifyReplay(payload);
}

TEST_F(LedgerChangesTest, SkipListBoundaryAndNegativeUnlTransition)
{
    auto negative = std::make_shared<SLE>(keylet::negativeUNL());
    negative->setFieldVL(sfValidatorToDisable, alice.pk().slice());
    // Set up the parent containing a pending Negative UNL transition.
    auto mutableParent = std::make_shared<Ledger>(*parent, now);
    LedgerHeader header = mutableParent->header();
    header.seq = 256;
    mutableParent->setLedgerInfo(header);
    parent = std::move(mutableParent);
    parent->rawInsert(negative);
    parent->setImmutable();
    built = std::make_shared<Ledger>(*parent, now);
    view = std::make_unique<OpenView>(built.get());
    view->enableChangeCapture(changes.status);
    built->setChangeCapture(&changes.pre, changes.status);
    built->updateNegativeUNL();
    built->setChangeCapture(nullptr);
    auto const payload = finish();
    EXPECT_EQ(payload["pre_transaction_changes"].size(), 1);
    EXPECT_EQ(payload["post_transaction_changes"].size(), 2);
    verifyReplay(payload);
}

TEST_F(LedgerChangesTest, MetadataOnlyOwnerThreadingAndDeletion)
{
    ASSERT_EQ(submit(payment(1, alice)).ter, tesSUCCESS);
    auto const offerKey = keylet::offer(alice, SeqProxy::rawSequence(50));
    auto offer = std::make_shared<SLE>(offerKey);
    offer->setAccountID(sfAccount, alice);
    offer->setFieldU32(sfSequence, 50);
    {
        ApplyViewImpl sandbox(view.get(), TapNone);
        sandbox.insert(offer);
        auto tx = payment(2, bob);
        ASSERT_TRUE(sandbox.apply(*view, *tx, tesSUCCESS, {}, false, registry.getJournal("apply")));
    }
    {
        ApplyViewImpl sandbox(view.get(), TapNone);
        auto current = sandbox.peek(offerKey);
        ASSERT_TRUE(current);
        sandbox.erase(current);
        auto tx = payment(3, bob);
        ASSERT_TRUE(sandbox.apply(*view, *tx, tesSUCCESS, {}, false, registry.getJournal("apply")));
    }
    auto const payload = finish();
    ASSERT_EQ(payload["transactions"].size(), 3);
    // The owner account is threaded although only the offer was in items_.
    EXPECT_EQ(payload["transactions"][1]["changes"].size(), 2);
    EXPECT_EQ(payload["transactions"][2]["changes"].size(), 2);
    verifyReplay(payload);
}

TEST_F(LedgerChangesTest, DryRunDoesNotRecord)
{
    auto const result = submit(payment(1, alice), TapDryRun);
    EXPECT_FALSE(result.applied);
    auto const payload = finish();
    EXPECT_TRUE(payload["transactions"].size() == 0);
    verifyReplay(payload);
}

TEST_F(LedgerChangesTest, RecordingFailureDoesNotChangeLedger)
{
    auto const tx = payment(1, alice);
    Ledger reference(*parent, now);
    OpenView referenceView(&reference);
    auto const expected =
        apply(registry, referenceView, *tx, TapNone, registry.getJournal("apply"));
    changes.status->failed = true;
    auto const actual = submit(tx);
    EXPECT_EQ(actual.ter, expected.ter);
    EXPECT_EQ(actual.applied, expected.applied);
    referenceView.apply(reference);
    view->apply(*built);
    reference.updateSkipList();
    built->updateSkipList();
    reference.setAccepted(now, kLedgerGenesisTimeResolution, true);
    built->setAccepted(now, kLedgerGenesisTimeResolution, true);
    EXPECT_EQ(built->header().hash, reference.header().hash);
    EXPECT_THROW((void)changes.getJson(*parent, *built, 0), std::runtime_error);
}

TEST_F(LedgerChangesTest, IncompleteCaptureCannotProducePayload)
{
    ASSERT_EQ(submit(payment(1, alice)).ter, tesSUCCESS);
    // Inject a missing journal record. Committed ledger effects remain intact.
    auto const discarded = view->takeTransactionChanges();
    ASSERT_EQ(discarded.size(), 1);
    EXPECT_THROW((void)finish(), std::runtime_error);
    EXPECT_TRUE(built->exists(keylet::account(alice)));
}

TEST_F(LedgerChangesTest, AllBatchModesFollowCommitAndRollback)
{
    struct Scenario
    {
        std::uint32_t flags;
        bool failFirst;
        bool failSecond;
        unsigned count;
    };
    for (auto const& scenario :
         {Scenario{tfAllOrNothing, false, false, 3},
          Scenario{tfAllOrNothing, false, true, 1},
          Scenario{tfIndependent, false, true, 3},
          Scenario{tfIndependent, false, false, 3},
          Scenario{tfUntilFailure, true, false, 2},
          Scenario{tfUntilFailure, false, true, 3},
          Scenario{tfOnlyOne, false, false, 2},
          Scenario{tfOnlyOne, true, false, 3}})
    {
        SCOPED_TRACE(scenario.flags);
        built = std::make_shared<Ledger>(*parent, now);
        view = std::make_unique<OpenView>(built.get());
        changes = LedgerChanges{};
        view->enableChangeCapture(changes.status);
        STArray raw(sfRawTransactions);
        for (unsigned i = 0; i < 2; ++i)
        {
            auto const fail = i == 0 ? scenario.failFirst : scenario.failSecond;
            transactions::PaymentBuilder inner{
                Account::kMaster,
                i == 0 ? alice : bob,
                fail ? XRPAmount{1} : XRP(100),
                2 + i,
                XRPAmount{0}};
            inner.setFlags(tfInnerBatchTxn);
            STObject object = inner.getSTObject();
            object.setFieldVL(sfSigningPubKey, Blob{});
            object.setFName(sfRawTransaction);
            raw.pushBack(std::move(object));
        }
        auto const batch = transactions::BatchBuilder{Account::kMaster, raw, 1, XRPAmount{40}}
                               .setFlags(scenario.flags)
                               .build(Account::kMaster.pk(), Account::kMaster.sk())
                               .getSTTx();
        ASSERT_EQ(
            applyTransaction(registry, *view, *batch, false, TapNone, registry.getJournal("apply")),
            ApplyTransactionResult::Success);
        auto const payload = finish();
        ASSERT_EQ(payload["transactions"].size(), scenario.count);
        for (unsigned i = 1; i < scenario.count; ++i)
            EXPECT_EQ(
                payload["transactions"][i]["parent_batch_id"].asString(),
                to_string(batch->getTransactionID()));
        verifyReplay(payload);
    }
}

}  // namespace
}  // namespace xrpl::test
