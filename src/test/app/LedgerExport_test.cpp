#include <test/jtx/Env.h>
#include <test/jtx/amount.h>
#include <test/jtx/envconfig.h>
#include <test/jtx/pay.h>

#include <xrpld/app/main/Application.h>
#include <xrpld/core/Config.h>

#include <xrpl/basics/FileUtilities.h>
#include <xrpl/basics/StringUtilities.h>
#include <xrpl/beast/unit_test/suite.h>
#include <xrpl/json/json_reader.h>
#include <xrpl/ledger/Ledger.h>
#include <xrpl/net/LedgerExporter.h>
#include <xrpl/rdb/SociDB.h>

#include <soci/soci.h>

#include <filesystem>
#include <memory>
#include <string>

namespace xrpl::test {

// Application/consensus fixtures are available only in the legacy test binary.
class LedgerExport_test : public beast::unit_test::Suite
{
    void
    testClose(bool exhaust)
    {
        testcase(exhaust ? "Export failure preserves the ledger" : "Ledger close and admin status");
        auto const directory =
            uniqueRandomPath(std::filesystem::temp_directory_path(), "xrpl-ledger-export-");
        std::filesystem::create_directories(directory);
        struct Cleanup
        {
            std::filesystem::path path;
            ~Cleanup()
            {
                std::error_code ec;
                std::filesystem::remove_all(path, ec);
            }
        } cleanup{directory};

        auto const config = [&] {
            return jtx::envconfig([&](std::unique_ptr<Config> cfg) {
                auto& section = cfg->section("ledger_export");
                section.set("url", "http://127.0.0.1:1/ledgers");
                section.set("outbox_path", (directory / "outbox.sqlite").string());
                if (exhaust)
                    section.set("max_pending_bytes", "1");
                return cfg;
            });
        };
        jtx::Env env(*this, config());
        auto* exporter = env.app().getLedgerExporter();
        if (!BEAST_EXPECT(exporter && exporter->enabled()))
            return;
        // Transport is covered by gtest. Keep the integration payload queued.
        exporter->stop();
        auto const parent = env.closed();
        jtx::Account const alice{"export_integration_alice"};
        env(jtx::pay(env.master, alice, jtx::XRP(1000)));
        BEAST_EXPECT(env.close());
        auto const ledger = env.closed();
        auto const info = env.rpc("server_info")["result"]["info"]["ledger_export"];
        BEAST_EXPECT(info["enabled"].asBool());
        BEAST_EXPECT(info["pending_ledgers"].asUInt() == (exhaust ? 0 : 1));
        BEAST_EXPECT(info["gap_count"].asUInt() == (exhaust ? 1 : 0));

        if (exhaust)
        {
            jtx::Env reference(*this);
            reference(jtx::pay(reference.master, alice, jtx::XRP(1000)));
            BEAST_EXPECT(reference.close());
            BEAST_EXPECT(ledger->header().accountHash == reference.closed()->header().accountHash);
            BEAST_EXPECT(ledger->header().txHash == reference.closed()->header().txHash);
            return;
        }

        soci::session sql;
        open(sql, "sqlite", (directory / "outbox.sqlite").string());
        std::string raw;
        sql << "SELECT Payload FROM Outbox ORDER BY Id LIMIT 1", soci::into(raw);
        json::Reader reader;
        json::Value payload;
        if (!BEAST_EXPECT(reader.parse(raw, payload)))
            return;
        BEAST_EXPECT(payload["ledger_hash"].asString() == to_string(ledger->header().hash));
        BEAST_EXPECT(payload["parent_ledger_hash"].asString() == to_string(parent->header().hash));
        BEAST_EXPECT(payload["transactions"].size() == 1);
        BEAST_EXPECT(payload["transactions"][0u]["TransactionIndex"].asUInt() == 0);

        Ledger replay(dynamic_cast<Ledger const&>(*parent), ledger->header().closeTime);
        auto applyChanges = [&](json::Value const& changes) {
            for (auto const& change : changes)
            {
                uint256 key;
                if (!BEAST_EXPECT(key.parseHex(change["key"].asString())))
                    return;
                if (change["after"].isNull())
                    replay.rawErase(key);
                else
                {
                    auto const bytes = strUnHex(change["after"]["blob"].asString());
                    if (!BEAST_EXPECT(bytes.has_value()))
                        return;
                    SerialIter sit(makeSlice(*bytes));
                    auto sle = std::make_shared<SLE>(sit, key);
                    if (replay.exists(keylet::unchecked(key)))
                        replay.rawReplace(sle);
                    else
                        replay.rawInsert(sle);
                }
            }
        };
        applyChanges(payload["pre_transaction_changes"]);
        for (auto const& tx : payload["transactions"])
            applyChanges(tx["changes"]);
        applyChanges(payload["post_transaction_changes"]);
        BEAST_EXPECT(replay.stateMap().getHash().asUInt256() == ledger->header().accountHash);
    }

public:
    void
    run() override
    {
        testClose(false);
        testClose(true);
    }
};

BEAST_DEFINE_TESTSUITE(LedgerExport, app, xrpl);

}  // namespace xrpl::test
