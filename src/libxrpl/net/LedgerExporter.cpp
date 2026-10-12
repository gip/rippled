#include <xrpl/net/LedgerExporter.h>

#include <xrpl/basics/Log.h>
#include <xrpl/basics/StringUtilities.h>
#include <xrpl/json/to_string.h>
#include <xrpl/net/HTTPClientSSLContext.h>
#include <xrpl/rdb/DatabaseCon.h>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>

#include <soci/soci.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <map>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <thread>
#include <utility>

namespace xrpl {

namespace asio = boost::asio;
namespace http = boost::beast::http;
using Tcp = asio::ip::tcp;
using namespace std::chrono_literals;

struct LedgerExporter::Impl
{
    struct Pending
    {
        long long id = 0;
        std::uint32_t ledger = 0;
        std::string hash;
        std::string payload;
    };

    // Owned by the request and deadline handler, including after cancellation.
    struct Request
    {
        Tcp::resolver resolver;
        Tcp::socket socket;
        std::optional<asio::ssl::stream<Tcp::socket&>> tls;
        asio::steady_timer deadline;
        asio::cancellation_signal cancellation;
        bool timedOut = false;

        explicit Request(asio::io_context& io) : resolver(io), socket(io), deadline(io)
        {
        }
    };

    ParsedUrl url_;
    std::string token_;
    std::filesystem::path path_;
    std::string verifyDir_;
    std::string verifyFile_;
    long long maxBytes_;
    beast::Journal journal_;

    mutable std::mutex mutex_;
    std::string lastError_;
    // Keep recent gaps visible even if the database cannot be written.
    std::map<std::string, std::pair<std::uint32_t, std::string>> volatileGaps_;
    std::unique_ptr<DatabaseCon> db_;
    std::optional<HTTPClientSSLContext> ssl_;
    asio::io_context io_;
    asio::steady_timer wake_{io_};
    std::thread worker_;
    bool spawned_ = false;

    Impl(
        Section const& config,
        std::filesystem::path const& dataDir,
        std::string const& verifyDir,
        std::string const& verifyFile,
        beast::Journal journal)
        : token_(config.valueOr<std::string>("bearer_token", ""))
        , path_(config.valueOr<std::string>("outbox_path", "ledger_export.sqlite"))
        , verifyDir_(verifyDir)
        , verifyFile_(verifyFile)
        , maxBytes_(config.valueOr<long long>("max_pending_bytes", 1024LL * 1024 * 1024))
        , journal_(journal)
    {
        auto const url = config.valueOr<std::string>("url", "");
        auto const control = [](unsigned char c) { return c <= 32 || c == 127; };
        if (!parseUrl(url_, url) || (url_.scheme != "http" && url_.scheme != "https") ||
            url_.domain.empty() || !url_.username.empty() || !url_.password.empty() ||
            url.find('#') != std::string::npos || std::ranges::any_of(url, control))
            throw std::invalid_argument("Invalid [ledger_export] URL");
        if (std::ranges::any_of(token_, control))
            throw std::invalid_argument("Invalid [ledger_export] bearer_token");
        if (maxBytes_ <= 0)
            throw std::invalid_argument("Invalid [ledger_export] max_pending_bytes");
        if (path_.is_relative())
        {
            if (dataDir.empty())
                throw std::invalid_argument("[ledger_export] requires a persistent outbox_path");
            path_ = dataDir / path_;
        }
        if (url_.path.empty())
            url_.path = "/";
        if (!url_.port)
            url_.port = url_.scheme == "https" ? 443 : 80;

        try
        {
            openDatabase();
        }
        catch (...)
        {
            lastError_ = "outbox storage unavailable";
            JLOG(journal_.error()) << "Ledger export: " << lastError_;
        }
    }

    // All callers hold mutex_, except the constructor before publication.
    void
    openDatabase()
    {
        if (db_)
            return;
        std::filesystem::create_directories(path_.parent_path());
        static std::array<std::string, 4> const kPragmas{
            "PRAGMA journal_mode=WAL;",
            "PRAGMA synchronous=FULL;",
            "PRAGMA busy_timeout=1000;",
            "PRAGMA wal_autocheckpoint=1000;"};
        static std::array<char const*, 3> const kSchema{
            "CREATE TABLE IF NOT EXISTS Outbox ("
            "Id INTEGER PRIMARY KEY AUTOINCREMENT, Hash TEXT NOT NULL UNIQUE, "
            "Ledger INTEGER NOT NULL, Payload TEXT NOT NULL, Bytes INTEGER NOT NULL);",
            "CREATE TABLE IF NOT EXISTS ExportState (Name TEXT PRIMARY KEY, Value TEXT NOT NULL);",
            "CREATE TABLE IF NOT EXISTS Gaps ("
            "Hash TEXT PRIMARY KEY, Ledger INTEGER NOT NULL, Reason TEXT NOT NULL);"};
        db_ = std::make_unique<DatabaseCon>(
            path_.parent_path(), path_.filename().string(), kPragmas, kSchema, journal_);
        // Persist gaps observed while storage was unavailable once it recovers.
        if (!volatileGaps_.empty())
        {
            auto& sql = db_->getSession();
            soci::transaction transaction(sql);
            for (auto const& [hash, gap] : volatileGaps_)
                sql << "INSERT OR REPLACE INTO Gaps(Hash,Ledger,Reason) VALUES(:h,:l,:r)",
                    soci::use(hash), soci::use(gap.first), soci::use(gap.second);
            transaction.commit();
            volatileGaps_.clear();
        }
    }

    void
    error(std::string const& reason)
    {
        std::lock_guard lock(mutex_);
        lastError_ = reason;
        JLOG(journal_.error()) << "Ledger export: " << reason;
        try
        {
            if (db_)
                db_->getSession()
                    << "INSERT OR REPLACE INTO ExportState(Name,Value) VALUES('last_error',:r)",
                    soci::use(reason);
        }
        catch (...)
        {
        }
    }

    void
    gap(std::uint32_t ledger, std::string const& hash, std::string_view reason) noexcept
    {
        try
        {
            // Allocate diagnostics inside the guard, including after bad_alloc.
            std::string const message(reason);
            std::lock_guard lock(mutex_);
            lastError_ = message;
            volatileGaps_[hash] = {ledger, message};
            // Status is bounded; durable gap history is retained in SQLite.
            if (volatileGaps_.size() > 100)
                volatileGaps_.erase(volatileGaps_.begin());
            JLOG(journal_.error())
                << "Ledger export gap at " << ledger << " " << hash << ": " << message;
            try
            {
                openDatabase();
                auto& sql = db_->getSession();
                soci::transaction transaction(sql);
                sql << "INSERT OR REPLACE INTO Gaps(Hash,Ledger,Reason) VALUES(:h,:l,:r)",
                    soci::use(hash), soci::use(ledger), soci::use(message);
                sql << "INSERT OR REPLACE INTO ExportState(Name,Value) VALUES('last_error',:r)",
                    soci::use(message);
                transaction.commit();
                volatileGaps_.erase(hash);
            }
            catch (...)
            {
                // The in-memory status and log still report the gap.
            }
        }
        catch (...)
        {
            // Export reporting is never allowed to escape into consensus.
        }
    }

    bool
    enqueue(std::uint32_t ledger, std::string const& hash, json::Value const& value) noexcept
    {
        try
        {
            auto const payload = json::to_string(value);
            {
                std::lock_guard lock(mutex_);
                openDatabase();
                auto& sql = db_->getSession();
                soci::transaction transaction(sql);
                int existing = 0;
                sql << "SELECT COUNT(*) FROM Outbox WHERE Hash=:h", soci::use(hash),
                    soci::into(existing);
                if (existing)
                    return true;
                long long pending = 0;
                sql << "SELECT COALESCE(SUM(Bytes),0) FROM Outbox", soci::into(pending);
                if (payload.size() > static_cast<std::uint64_t>(maxBytes_) ||
                    pending > maxBytes_ - static_cast<long long>(payload.size()))
                    throw std::length_error("outbox capacity exceeded");
                auto const bytes = static_cast<long long>(payload.size());
                sql << "INSERT INTO Outbox(Hash,Ledger,Payload,Bytes) VALUES(:h,:l,:p,:b)",
                    soci::use(hash), soci::use(ledger), soci::use(payload), soci::use(bytes);
                transaction.commit();
            }
            // Polling also finds queued payloads if notification cannot allocate.
            try
            {
                asio::post(io_, [this] { wake_.cancel(); });
            }
            catch (...)
            {
            }
            return true;
        }
        catch (std::length_error const&)
        {
            gap(ledger, hash, "outbox capacity exceeded");
        }
        catch (...)
        {
            gap(ledger, hash, "outbox persistence failed");
        }
        return false;
    }

    std::optional<Pending>
    next()
    {
        std::lock_guard lock(mutex_);
        openDatabase();
        Pending p;
        soci::indicator indicator;
        db_->getSession() << "SELECT Id,Ledger,Hash,Payload FROM Outbox ORDER BY Id LIMIT 1",
            soci::into(p.id, indicator), soci::into(p.ledger), soci::into(p.hash),
            soci::into(p.payload);
        if (!db_->getSession().got_data())
            return std::nullopt;
        return p;
    }

    void
    acknowledge(Pending const& p)
    {
        std::lock_guard lock(mutex_);
        auto& sql = db_->getSession();
        soci::transaction transaction(sql);
        auto const ledger = std::to_string(p.ledger);
        sql << "INSERT OR REPLACE INTO ExportState(Name,Value) VALUES('last_acknowledged_hash',:h)",
            soci::use(p.hash);
        sql << "INSERT OR REPLACE INTO ExportState(Name,Value) "
               "VALUES('last_acknowledged_ledger',:l)",
            soci::use(ledger);
        sql << "DELETE FROM Outbox WHERE Id=:i", soci::use(p.id);
        transaction.commit();
        JLOG(journal_.debug()) << "Ledger export acknowledged " << p.ledger << " " << p.hash;
    }

    template <class Stream>
    asio::awaitable<unsigned>
    exchange(Stream& stream, Pending const& p, Request& operation)
    {
        http::request<http::string_body> request{http::verb::post, url_.path, 11};
        auto const host =
            url_.domain.find(':') == std::string::npos ? url_.domain : "[" + url_.domain + "]";
        request.set(http::field::host, host + ":" + std::to_string(*url_.port));
        request.set(http::field::content_type, "application/json");
        request.set("Idempotency-Key", p.hash);
        if (!token_.empty())
            request.set(http::field::authorization, "Bearer " + token_);
        request.keep_alive(false);
        request.body() = p.payload;
        request.prepare_payload();
        co_await http::async_write(
            stream,
            request,
            asio::bind_cancellation_slot(operation.cancellation.slot(), asio::use_awaitable));
        boost::beast::flat_buffer buffer;
        for (unsigned i = 0; i < 8; ++i)
        {
            http::response_parser<http::empty_body> response;
            response.header_limit(32768);
            response.skip(true);
            co_await http::async_read_header(
                stream,
                buffer,
                response,
                asio::bind_cancellation_slot(operation.cancellation.slot(), asio::use_awaitable));
            auto const status = response.get().result_int();
            if (status >= 200)
                co_return status;
        }
        throw std::runtime_error("too many informational HTTP responses");
    }

    asio::awaitable<unsigned>
    post(Pending const& p)
    {
        auto request = std::make_shared<Request>(io_);
        request->deadline.expires_after(10s);
        request->deadline.async_wait([request](boost::system::error_code const& ec) {
            if (!ec)
            {
                request->timedOut = true;
                request->cancellation.emit(asio::cancellation_type::terminal);
                request->resolver.cancel();
                boost::system::error_code ignored;
                request->socket.close(ignored);
            }
        });
        try
        {
            if (url_.scheme == "https")
            {
                if (!ssl_)
                    ssl_.emplace(
                        verifyDir_, verifyFile_, true, journal_, asio::ssl::context::tls_client);
                request->tls.emplace(request->socket, ssl_->context());
                if (auto const ec = ssl_->preConnectVerify(*request->tls, url_.domain))
                    throw boost::system::system_error(ec);
                if (auto const ec = ssl_->postConnectVerify(*request->tls, url_.domain))
                    throw boost::system::system_error(ec);
            }
            auto const endpoints = co_await request->resolver.async_resolve(
                url_.domain, std::to_string(*url_.port), asio::use_awaitable);
            if (request->timedOut)
                throw std::runtime_error("request timed out");
            co_await asio::async_connect(
                request->socket,
                endpoints,
                asio::bind_cancellation_slot(request->cancellation.slot(), asio::use_awaitable));
            if (request->timedOut)
                throw std::runtime_error("request timed out");
            unsigned status;
            if (request->tls)
            {
                co_await request->tls->async_handshake(
                    asio::ssl::stream_base::client,
                    asio::bind_cancellation_slot(
                        request->cancellation.slot(), asio::use_awaitable));
                if (request->timedOut)
                    throw std::runtime_error("request timed out");
                status = co_await exchange(*request->tls, p, *request);
            }
            else
                status = co_await exchange(request->socket, p, *request);
            request->deadline.cancel();
            co_return status;
        }
        catch (...)
        {
            request->deadline.cancel();
            throw;
        }
    }

    asio::awaitable<void>
    run()
    {
        unsigned delay = 1;
        while (!io_.stopped())
        {
            bool failed = false;
            try
            {
                if (auto const p = next())
                {
                    auto const status = co_await post(*p);
                    if (status >= 200 && status < 300)
                    {
                        acknowledge(*p);
                        delay = 1;
                        continue;
                    }
                    error("HTTP status " + std::to_string(status));
                    failed = true;
                }
            }
            catch (...)
            {
                error("delivery or outbox operation failed");
                failed = true;
            }
            wake_.expires_after(std::chrono::seconds(failed ? delay : 1));
            // New ledgers may wake idle polling, but must not bypass retry delay.
            asio::steady_timer retry(io_);
            auto& timer = failed ? retry : wake_;
            if (failed)
            {
                retry.expires_after(std::chrono::seconds(delay));
                delay = std::min(60U, delay * 2);
            }
            boost::system::error_code ec;
            co_await timer.async_wait(asio::redirect_error(asio::use_awaitable, ec));
        }
    }

    json::Value
    status() const
    {
        std::lock_guard lock(mutex_);
        json::Value result{json::ValueType::Object};
        result["enabled"] = true;
        result["last_error"] = lastError_;
        auto& gaps = result["gaps"] = json::ValueType::Array;
        std::map<std::string, std::pair<std::uint32_t, std::string>> recent = volatileGaps_;
        try
        {
            if (!db_)
                throw std::runtime_error("no outbox");
            auto& sql = db_->getSession();
            long long count = 0;
            long long bytes = 0;
            long long gapCount = 0;
            sql << "SELECT COUNT(*), COALESCE(SUM(Bytes),0) FROM Outbox", soci::into(count),
                soci::into(bytes);
            sql << "SELECT COUNT(*) FROM Gaps", soci::into(gapCount);
            result["pending_ledgers"] = static_cast<double>(count);
            result["pending_bytes"] = static_cast<double>(bytes);
            result["gap_count"] = static_cast<double>(gapCount);
            soci::rowset<soci::row> rows = (sql.prepare << "SELECT Name,Value FROM ExportState");
            for (auto const& row : rows)
                if (auto const name = row.get<std::string>(0);
                    name != "last_error" || lastError_.empty())
                    result[name] = row.get<std::string>(1);
            soci::rowset<soci::row> gapRows =
                (sql.prepare
                 << "SELECT Hash,Ledger,Reason FROM Gaps ORDER BY rowid DESC LIMIT 100");
            for (auto const& row : gapRows)
                recent[row.get<std::string>(0)] = {
                    static_cast<std::uint32_t>(row.get<int>(1)), row.get<std::string>(2)};
            result["storage_available"] = true;
        }
        catch (...)
        {
            result["storage_available"] = false;
        }
        unsigned emitted = 0;
        for (auto const& [hash, gap] : recent)
        {
            if (emitted++ == 100)
                break;
            json::Value value{json::ValueType::Object};
            value["ledger_index"] = gap.first;
            value["ledger_hash"] = hash;
            value["reason"] = gap.second;
            gaps.append(std::move(value));
        }
        return result;
    }
};

LedgerExporter::LedgerExporter(
    Section const& config,
    std::filesystem::path const& dataDir,
    std::string const& sslVerifyDir,
    std::string const& sslVerifyFile,
    beast::Journal journal)
{
    if (auto const url = config.get<std::string>("url"); url && !url->empty())
        impl_ = std::make_unique<Impl>(config, dataDir, sslVerifyDir, sslVerifyFile, journal);
}

LedgerExporter::~LedgerExporter()
{
    stop();
}

bool
LedgerExporter::enabled() const noexcept
{
    return bool(impl_);
}

void
LedgerExporter::start() noexcept
{
    if (!impl_ || impl_->worker_.joinable())
        return;
    try
    {
        impl_->io_.restart();
        if (!impl_->spawned_)
        {
            asio::co_spawn(impl_->io_, impl_->run(), asio::detached);
            impl_->spawned_ = true;
        }
        impl_->worker_ = std::thread([this] {
            try
            {
                impl_->io_.run();
            }
            catch (...)
            {
                try
                {
                    impl_->error("HTTP worker failed");
                }
                catch (...)
                {
                }
            }
        });
    }
    catch (...)
    {
        try
        {
            impl_->error("HTTP worker could not start");
        }
        catch (...)
        {
        }
    }
}

void
LedgerExporter::stop() noexcept
{
    if (impl_)
    {
        impl_->io_.stop();
        if (impl_->worker_.joinable())
            impl_->worker_.join();
    }
}

bool
LedgerExporter::enqueue(
    std::uint32_t ledger,
    std::string const& hash,
    json::Value const& payload) noexcept
{
    return impl_ && impl_->enqueue(ledger, hash, payload);
}

void
LedgerExporter::reportGap(
    std::uint32_t ledger,
    std::string const& hash,
    std::string const& reason) noexcept
{
    if (impl_)
        impl_->gap(ledger, hash, reason);
}

json::Value
LedgerExporter::getStatus() const
{
    if (impl_)
        return impl_->status();
    json::Value result{json::ValueType::Object};
    result["enabled"] = false;
    return result;
}

}  // namespace xrpl
