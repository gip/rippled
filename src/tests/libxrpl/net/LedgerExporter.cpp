#include <xrpl/net/LedgerExporter.h>

#include <xrpl/basics/FileUtilities.h>
#include <xrpl/json/to_string.h>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>

#include <gtest/gtest.h>
#include <helpers/TestSink.h>
#include <openssl/pem.h>
#include <openssl/x509v3.h>

#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

namespace xrpl::test {
namespace {

namespace asio = boost::asio;
namespace http = boost::beast::http;
using Tcp = asio::ip::tcp;
using namespace std::chrono_literals;

class Receiver
{
    asio::io_context io_;
    Tcp::acceptor acceptor_{io_, {asio::ip::make_address("127.0.0.1"), 0}};
    std::vector<unsigned> responses_;
    std::shared_ptr<asio::ssl::context> tls_;
    mutable std::mutex mutex_;
    std::condition_variable changed_;
    std::vector<http::request<http::string_body>> requests_;
    unsigned connections_ = 0;
    std::thread worker_;

    template <class Stream>
    asio::awaitable<void>
    receive(Stream& socket)
    {
        boost::beast::flat_buffer buffer;
        http::request<http::string_body> request;
        co_await http::async_read(socket, buffer, request, asio::use_awaitable);
        unsigned status;
        {
            std::lock_guard lock(mutex_);
            auto const index = requests_.size();
            status = index < responses_.size() ? responses_[index] : 200;
            requests_.push_back(std::move(request));
            changed_.notify_all();
        }
        if (status == 1)
        {
            // Hold the connection without responding to exercise the deadline.
            asio::steady_timer timer(io_);
            timer.expires_after(20s);
            co_await timer.async_wait(asio::use_awaitable);
        }
        else if (status != 0)
        {
            // Zero simulates committing data then losing the acknowledgement.
            http::response<http::empty_body> response{http::status(status), 11};
            response.keep_alive(false);
            response.prepare_payload();
            co_await http::async_write(socket, response, asio::use_awaitable);
        }
    }

    asio::awaitable<void>
    run()
    {
        try
        {
            while (true)
            {
                auto socket = co_await acceptor_.async_accept(asio::use_awaitable);
                {
                    std::lock_guard lock(mutex_);
                    ++connections_;
                    changed_.notify_all();
                }
                try
                {
                    if (tls_)
                    {
                        asio::ssl::stream<Tcp::socket&> stream(socket, *tls_);
                        co_await stream.async_handshake(
                            asio::ssl::stream_base::server, asio::use_awaitable);
                        co_await receive(stream);
                    }
                    else
                        co_await receive(socket);
                }
                catch (...)
                {
                    // Certificate rejection is expected in the TLS tests.
                }
            }
        }
        catch (...)
        {
        }
    }

public:
    explicit Receiver(
        std::vector<unsigned> responses = {},
        std::shared_ptr<asio::ssl::context> tls = {})
        : responses_(std::move(responses)), tls_(std::move(tls))
    {
        asio::co_spawn(io_, run(), asio::detached);
        worker_ = std::thread([this] { io_.run(); });
    }

    ~Receiver()
    {
        io_.stop();
        worker_.join();
    }

    std::string
    url() const
    {
        return "http://127.0.0.1:" + std::to_string(acceptor_.local_endpoint().port()) +
            "/ledgers?source=test";
    }

    bool
    waitFor(std::size_t count)
    {
        std::unique_lock lock(mutex_);
        return changed_.wait_for(lock, 8s, [&] { return requests_.size() >= count; });
    }

    bool
    waitForConnection()
    {
        std::unique_lock lock(mutex_);
        return changed_.wait_for(lock, 5s, [&] { return connections_ != 0; });
    }

    auto
    requests() const
    {
        std::lock_guard lock(mutex_);
        return requests_;
    }
};

class LedgerExporterTest : public ::testing::Test
{
protected:
    std::filesystem::path const directory =
        uniqueRandomPath(std::filesystem::temp_directory_path(), "xrpl-export-");
    beast::Journal journal{TestSink::instance()};

    void
    SetUp() override
    {
        std::filesystem::create_directories(directory);
    }

    void
    TearDown() override
    {
        std::error_code ec;
        std::filesystem::remove_all(directory, ec);
    }

    Section
    config(std::string const& url)
    {
        Section section;
        section.set("url", url);
        return section;
    }

    json::Value
    payload(unsigned ledger)
    {
        json::Value value{json::ValueType::Object};
        value["ledger_index"] = ledger;
        return value;
    }

    bool
    drained(LedgerExporter& exporter)
    {
        auto const deadline = std::chrono::steady_clock::now() + 5s;
        do
        {
            auto const status = exporter.getStatus();
            if (status["storage_available"].asBool() && status["pending_ledgers"].asUInt() == 0)
                return true;
            std::this_thread::sleep_for(10ms);
        } while (std::chrono::steady_clock::now() < deadline);
        return false;
    }

    bool
    failed(LedgerExporter& exporter, std::chrono::seconds timeout = 5s)
    {
        auto const deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline)
        {
            if (!exporter.getStatus()["last_error"].asString().empty())
                return true;
            std::this_thread::sleep_for(10ms);
        }
        return false;
    }

    std::shared_ptr<asio::ssl::context>
    certificate()
    {
        auto context = std::make_shared<asio::ssl::context>(asio::ssl::context::tls_server);
        std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)> key(
            EVP_PKEY_Q_keygen(nullptr, nullptr, "EC", "P-256"), EVP_PKEY_free);
        std::unique_ptr<X509, decltype(&X509_free)> cert(X509_new(), X509_free);
        if (!key || !cert || !X509_set_version(cert.get(), 2) ||
            !ASN1_INTEGER_set(X509_get_serialNumber(cert.get()), 1) ||
            !X509_gmtime_adj(X509_getm_notBefore(cert.get()), -3600) ||
            !X509_gmtime_adj(X509_getm_notAfter(cert.get()), 86400) ||
            !X509_set_pubkey(cert.get(), key.get()))
            throw std::runtime_error("Cannot create test certificate");
        auto* subject = X509_get_subject_name(cert.get());
        if (!X509_NAME_add_entry_by_txt(
                subject,
                "CN",
                MBSTRING_ASC,
                reinterpret_cast<unsigned char const*>("localhost"),
                -1,
                -1,
                0) ||
            !X509_set_issuer_name(cert.get(), subject))
            throw std::runtime_error("Cannot set test certificate subject");
        X509V3_CTX extensions;
        X509V3_set_ctx(&extensions, cert.get(), cert.get(), nullptr, nullptr, 0);
        for (auto const& [nid, value] :
             {std::pair{NID_basic_constraints, "critical,CA:TRUE"},
              std::pair{NID_subject_alt_name, "DNS:localhost"}})
        {
            std::unique_ptr<X509_EXTENSION, decltype(&X509_EXTENSION_free)> extension(
                X509V3_EXT_conf_nid(nullptr, &extensions, nid, value), X509_EXTENSION_free);
            if (!extension || !X509_add_ext(cert.get(), extension.get(), -1))
                throw std::runtime_error("Cannot extend test certificate");
        }
        if (!X509_sign(cert.get(), key.get(), EVP_sha256()) ||
            !SSL_CTX_use_certificate(context->native_handle(), cert.get()) ||
            !SSL_CTX_use_PrivateKey(context->native_handle(), key.get()))
            throw std::runtime_error("Cannot sign test certificate");
        std::unique_ptr<BIO, decltype(&BIO_free)> pem(BIO_new(BIO_s_mem()), BIO_free);
        if (!pem || !PEM_write_bio_X509(pem.get(), cert.get()))
            throw std::runtime_error("Cannot serialize test certificate");
        char* data = nullptr;
        auto const length = BIO_get_mem_data(pem.get(), &data);
        std::ofstream(directory / "ca.pem").write(data, length);
        return context;
    }
};

TEST_F(LedgerExporterTest, DisabledWithoutUrl)
{
    LedgerExporter exporter(Section{}, directory, "", "", journal);
    EXPECT_FALSE(exporter.enabled());
    EXPECT_FALSE(exporter.getStatus()["enabled"].asBool());
    EXPECT_TRUE(std::filesystem::is_empty(directory));
}

TEST_F(LedgerExporterTest, DurableOrderedRetryAndAuthentication)
{
    Receiver receiver({503, 200, 202});
    auto section = config(receiver.url());
    section.set("bearer_token", "test-token");
    LedgerExporter exporter(section, directory, "", "", journal);
    ASSERT_TRUE(exporter.enqueue(10, "hash10", payload(10)));
    ASSERT_TRUE(exporter.enqueue(11, "hash11", payload(11)));
    ASSERT_TRUE(exporter.enqueue(10, "hash10", payload(10)));  // Pending duplicate.
    EXPECT_EQ(exporter.getStatus()["pending_ledgers"].asUInt(), 2);
    exporter.start();
    ASSERT_TRUE(receiver.waitFor(3));
    ASSERT_TRUE(drained(exporter));
    auto const requests = receiver.requests();
    ASSERT_EQ(requests.size(), 3);
    EXPECT_EQ(requests[0u]["Idempotency-Key"], "hash10");
    EXPECT_EQ(requests[1]["Idempotency-Key"], "hash10");
    EXPECT_EQ(requests[2]["Idempotency-Key"], "hash11");
    EXPECT_EQ(requests[0u].method(), http::verb::post);
    EXPECT_EQ(requests[0u].target(), "/ledgers?source=test");
    EXPECT_EQ(requests[0u][http::field::content_type], "application/json");
    EXPECT_EQ(requests[0u][http::field::authorization], "Bearer test-token");
    EXPECT_EQ(requests[0u].body(), json::to_string(payload(10)));
    auto const status = exporter.getStatus();
    EXPECT_EQ(status["last_acknowledged_hash"].asString(), "hash11");
    EXPECT_EQ(status["last_acknowledged_ledger"].asString(), "11");
    EXPECT_EQ(json::to_string(status).find("test-token"), std::string::npos);
}

TEST_F(LedgerExporterTest, PendingPayloadsSurviveRestart)
{
    Receiver receiver;
    auto const section = config(receiver.url());
    {
        LedgerExporter exporter(section, directory, "", "", journal);
        ASSERT_TRUE(exporter.enqueue(10, "hash10", payload(10)));
        ASSERT_TRUE(exporter.enqueue(11, "hash11", payload(11)));
    }
    {
        LedgerExporter exporter(section, directory, "", "", journal);
        ASSERT_EQ(exporter.getStatus()["pending_ledgers"].asUInt(), 2);
        exporter.start();
        ASSERT_TRUE(receiver.waitFor(2));
        ASSERT_TRUE(drained(exporter));
    }
    LedgerExporter exporter(section, directory, "", "", journal);
    EXPECT_EQ(exporter.getStatus()["pending_ledgers"].asUInt(), 0);
    EXPECT_EQ(exporter.getStatus()["last_acknowledged_hash"].asString(), "hash11");
}

TEST_F(LedgerExporterTest, LostAcknowledgementAllowsDuplicateDelivery)
{
    Receiver receiver({0, 200});
    auto const section = config(receiver.url());
    {
        LedgerExporter exporter(section, directory, "", "", journal);
        ASSERT_TRUE(exporter.enqueue(10, "hash10", payload(10)));
        exporter.start();
        ASSERT_TRUE(receiver.waitFor(1));
        exporter.stop();
    }
    LedgerExporter exporter(section, directory, "", "", journal);
    ASSERT_EQ(exporter.getStatus()["pending_ledgers"].asUInt(), 1);
    exporter.start();
    ASSERT_TRUE(receiver.waitFor(2));
    ASSERT_TRUE(drained(exporter));
    auto const requests = receiver.requests();
    EXPECT_EQ(requests[0u]["Idempotency-Key"], requests[1]["Idempotency-Key"]);
    EXPECT_EQ(requests[0u].body(), requests[1].body());
}

TEST_F(LedgerExporterTest, QueueExhaustionRetainsPendingPayloadAndReportsGap)
{
    auto section = config("http://127.0.0.1:1/ledgers");
    section.set("max_pending_bytes", std::to_string(json::to_string(payload(10)).size()));
    {
        LedgerExporter exporter(section, directory, "", "", journal);
        ASSERT_TRUE(exporter.enqueue(10, "hash10", payload(10)));
        EXPECT_FALSE(exporter.enqueue(11, "hash11", payload(11)));
        auto const status = exporter.getStatus();
        EXPECT_EQ(status["pending_ledgers"].asUInt(), 1);
        EXPECT_EQ(status["gap_count"].asUInt(), 1);
        ASSERT_EQ(status["gaps"].size(), 1);
        EXPECT_EQ(status["gaps"][0u]["ledger_index"].asUInt(), 11);
    }
    LedgerExporter restarted(section, directory, "", "", journal);
    EXPECT_EQ(restarted.getStatus()["pending_ledgers"].asUInt(), 1);
    EXPECT_EQ(restarted.getStatus()["gap_count"].asUInt(), 1);
}

TEST_F(LedgerExporterTest, StorageFailureDoesNotThrowAndCanRecover)
{
    auto const blocker = directory / "blocker";
    std::ofstream(blocker) << "not a directory";
    auto section = config("http://127.0.0.1:1/ledgers");
    section.set("outbox_path", (blocker / "outbox.sqlite").string());
    LedgerExporter exporter(section, directory, "", "", journal);
    EXPECT_TRUE(exporter.enabled());
    EXPECT_FALSE(exporter.enqueue(10, "hash10", payload(10)));
    auto const status = exporter.getStatus();
    EXPECT_FALSE(status["storage_available"].asBool());
    ASSERT_EQ(status["gaps"].size(), 1);
    std::filesystem::remove(blocker);
    EXPECT_TRUE(exporter.enqueue(11, "hash11", payload(11)));
    EXPECT_EQ(exporter.getStatus()["pending_ledgers"].asUInt(), 1);
}

TEST_F(LedgerExporterTest, InvalidConfigurationAndHeaderInjectionAreRejected)
{
    for (auto const* url : {"file:///tmp/outbox", "http://user:pass@localhost/", "http://host/#x"})
        EXPECT_THROW(
            LedgerExporter(config(url), directory, "", "", journal), std::invalid_argument);
    auto section = config("http://localhost/");
    section.set("bearer_token", "secret\r\nInjected: header");
    EXPECT_THROW(LedgerExporter(section, directory, "", "", journal), std::invalid_argument);
}

TEST_F(LedgerExporterTest, HttpsVerifiesTrustAndHostname)
{
    unsigned scenario = 0;
    for (auto const& [trusted, matching] :
         {std::pair{true, true}, std::pair{true, false}, std::pair{false, true}})
    {
        Receiver receiver({}, certificate());
        auto url = receiver.url();
        url.replace(0, 4, "https");
        if (matching)
            url.replace(url.find("127.0.0.1"), 9, "localhost");
        auto section = config(url);
        section.set("outbox_path", "tls-" + std::to_string(scenario++) + ".sqlite");
        LedgerExporter exporter(
            section, directory, "", trusted ? (directory / "ca.pem").string() : "", journal);
        ASSERT_TRUE(exporter.enqueue(10, "hash10", payload(10)));
        exporter.start();
        ASSERT_TRUE(receiver.waitForConnection());
        if (trusted && matching)
        {
            ASSERT_TRUE(receiver.waitFor(1));
            EXPECT_TRUE(drained(exporter));
        }
        else
        {
            EXPECT_TRUE(failed(exporter));
            EXPECT_TRUE(receiver.requests().empty());
            EXPECT_EQ(exporter.getStatus()["pending_ledgers"].asUInt(), 1);
        }
    }
}

TEST_F(LedgerExporterTest, RequestTimeoutRetainsPayload)
{
    Receiver receiver({1});
    LedgerExporter exporter(config(receiver.url()), directory, "", "", journal);
    ASSERT_TRUE(exporter.enqueue(10, "hash10", payload(10)));
    exporter.start();
    ASSERT_TRUE(receiver.waitFor(1));
    EXPECT_TRUE(failed(exporter, 12s));
    EXPECT_EQ(exporter.getStatus()["pending_ledgers"].asUInt(), 1);
}

}  // namespace
}  // namespace xrpl::test
