#pragma once

#include <condition_variable>
#include <coroutine>
#include <exception>
#include <optional>
#include <queue>
#include <thread>
#include <vector>

#if defined(_WIN32)
#    ifndef WIN32_LEAN_AND_MEAN
#        define WIN32_LEAN_AND_MEAN
#    endif
#    ifndef NOMINMAX
#        define NOMINMAX
#    endif
#    include <WS2tcpip.h>
#    include <WinSock2.h>
#    include <Windows.h>
#    include <concurrent_queue.h>

#    include <MSWSock.h>

#    pragma comment(lib, "Ws2_32.lib")
#endif

namespace coro {

template <typename T>
class promise;
template <typename T>
class task_awaiter;
template <typename T>
class task;

template <>
class promise<void> {
public:
    struct final_awaiter {
        /// @brief
        ///   Always trigger @p await_suspend().
        constexpr auto await_ready() const noexcept -> bool {
            return false;
        }

        /// @brief
        ///   Finalize this coroutine and resume caller coroutine.
        auto await_suspend(std::coroutine_handle<promise<void>> coroutine) noexcept
            -> std::coroutine_handle<> {
            // Coroutine finalize, resume parent coroutine call.
            auto &promise = coroutine.promise();
            return promise.m_next ? promise.m_next : std::noop_coroutine();
        }

        /// @brief
        ///   Do nothing.
        auto await_resume() noexcept -> void {}
    };

public:
    /// @brief
    ///   Create task from promise.
    auto get_return_object() noexcept -> task<void>;

    /// @brief
    ///   Always suspend when coroutine is created.
    auto initial_suspend() noexcept -> std::suspend_always {
        return {};
    }

    /// @brief
    ///   Finalize this coroutine and resume caller coroutine.
    auto final_suspend() noexcept -> final_awaiter {
        return {};
    }

    /// @brief
    ///   Cache current exception.
    auto unhandled_exception() noexcept -> void {
        m_exception = std::current_exception();
    }

    /// @brief
    ///   Void task returns void.
    auto return_void() noexcept -> void {}

    /// @brief
    ///   Rethrow exception or get coroutine return value.
    auto result() const -> void {
        if (m_exception)
            std::rethrow_exception(m_exception);
    }

    friend class task_awaiter<void>;

private:
    std::coroutine_handle<> m_next{};
    std::exception_ptr      m_exception{};
};

template <std::movable T>
class promise<T> {
public:
    struct final_awaiter {
        /// @brief
        ///   Always trigger @p await_suspend().
        constexpr auto await_ready() const noexcept -> bool {
            return false;
        }

        /// @brief
        ///   Finalize this coroutine and resume caller coroutine.
        auto await_suspend(std::coroutine_handle<promise<T>> coroutine) noexcept
            -> std::coroutine_handle<> {
            // Coroutine finalize, resume parent coroutine call.
            auto &promise = coroutine.promise();
            return promise.m_next ? promise.m_next : std::noop_coroutine();
        }

        /// @brief
        ///   Do nothing.
        auto await_resume() noexcept -> void {}
    };

public:
    /// @brief
    ///   Create task from promise.
    auto get_return_object() noexcept -> task<T> {
        return task<T>{std::coroutine_handle<promise<T>>::from_promise(*this)};
    }

    /// @brief
    ///   Always suspend when coroutine is created.
    auto initial_suspend() noexcept -> std::suspend_always {
        return {};
    }

    /// @brief
    ///   Finalize this coroutine and resume caller coroutine.
    auto final_suspend() noexcept -> final_awaiter {
        return {};
    }

    /// @brief
    ///   Cache current exception.
    auto unhandled_exception() noexcept -> void {
        m_exception = std::current_exception();
    }

    /// @brief
    ///   Cache coroutine return value.
    auto return_value(T &&value) noexcept(std::is_nothrow_move_constructible_v<T>) -> void {
        m_result.emplace(std::move(value));
    }

    /// @brief
    ///   Rethrow coroutine exception or get return value.
    auto result() -> T & {
        if (m_exception != nullptr)
            std::rethrow_exception(m_exception);
        return m_result.value();
    }

    /// @brief
    ///   Rethrow coroutine exception or get return value.
    auto result() const -> const T & {
        if (m_exception != nullptr)
            std::rethrow_exception(m_exception);
        return m_result.value();
    }

    friend class task_awaiter<T>;

private:
    std::coroutine_handle<> m_next{};
    std::exception_ptr      m_exception{};
    std::optional<T>        m_result{};
};

template <>
class task_awaiter<void> {
public:
    using promise_type     = promise<void>;
    using coroutine_handle = std::coroutine_handle<promise_type>;

    /// @brief
    ///   Create an empty task awaiter.
    task_awaiter() noexcept = default;

    /// @brief
    ///   Create a new task awaiter for the specified coroutine.
    explicit task_awaiter(coroutine_handle coroutine) noexcept : m_coroutine(coroutine) {}

    /// @brief
    ///   Resume this coroutine immediately if this is an empty coroutine.
    auto await_ready() noexcept -> bool {
        return m_coroutine == nullptr || m_coroutine.done();
    }

    /// @brief
    ///   Cache caller coroutine handle and start this coroutine.
    auto await_suspend(std::coroutine_handle<> handle) noexcept -> std::coroutine_handle<> {
        // Cache caller coroutine handle in promise and start this coroutine.
        m_coroutine.promise().m_next = handle;
        return m_coroutine;
    }

    /// @brief
    ///   Rethrow coroutine exception if exists.
    auto await_resume() -> void {
        m_coroutine.promise().result();
    }

private:
    coroutine_handle m_coroutine{};
};

template <std::movable T>
class task_awaiter<T> {
public:
    using promise_type     = promise<T>;
    using coroutine_handle = std::coroutine_handle<promise_type>;

    /// @brief
    ///   Create an empty task awaiter.
    task_awaiter() noexcept = default;

    /// @brief
    ///   Create a new task awaiter for the specified coroutine.
    explicit task_awaiter(coroutine_handle coroutine) noexcept : m_coroutine(coroutine) {}

    /// @brief
    ///   Resume this coroutine immediately if this is an empty coroutine.
    auto await_ready() noexcept -> bool {
        return m_coroutine == nullptr || m_coroutine.done();
    }

    /// @brief
    ///   Cache caller coroutine handle and start this coroutine.
    auto await_suspend(std::coroutine_handle<> handle) noexcept -> std::coroutine_handle<> {
        // Cache caller coroutine handle in promise and start this coroutine.
        m_coroutine.promise().m_next = handle;
        return m_coroutine;
    }

    /// @brief
    ///   Get coroutine result or rethrow exception.
    auto await_resume() -> T {
        return std::move(m_coroutine.promise().result());
    }

private:
    coroutine_handle m_coroutine{};
};

template <>
class task<void> {
public:
    using promise_type     = promise<void>;
    using coroutine_handle = std::coroutine_handle<promise_type>;

    /// @brief
    ///   Create an empty task.
    task() noexcept = default;

    /// @brief
    ///   Create a new task from coroutine handle.
    explicit task(coroutine_handle coroutine) noexcept : m_coroutine(coroutine) {}

    /// @brief
    ///   Copy constructor of task is disabled.
    task(const task &) = delete;

    /// @brief
    ///   Copy assignment of task is disabled.
    auto operator=(const task &) = delete;

    /// @brief
    ///   Move constructor of task.
    ///
    /// @param other
    ///   The task to be moved. The moved task will be invalidated.
    task(task &&other) noexcept : m_coroutine(other.m_coroutine) {
        other.m_coroutine = nullptr;
    }

    /// @brief
    ///   Move assignment of task.
    ///
    /// @param other
    ///   The task to be moved. The moved task will be invalidated.
    auto operator=(task &&other) noexcept -> task & {
        if (m_coroutine != nullptr)
            m_coroutine.destroy();

        m_coroutine       = other.m_coroutine;
        other.m_coroutine = nullptr;
        return *this;
    }

    /// @brief
    ///   Destroy this task and the coroutine.
    ~task() noexcept {
        if (m_coroutine != nullptr)
            m_coroutine.destroy();
    }

    /// @brief
    ///   Checks if this task is finished.
    [[nodiscard]]
    auto done() const noexcept -> bool {
        return m_coroutine == nullptr || m_coroutine.done();
    }

    /// @brief
    ///   Resume this coroutine if suspended.
    auto resume() -> void {
        m_coroutine.resume();
    }

    /// @brief
    ///   Destroy this coroutine.
    auto destroy() noexcept -> void {
        if (m_coroutine == nullptr)
            return;
        m_coroutine.destroy();
        m_coroutine = nullptr;
    }

    /// @brief
    ///   Get promise of this coroutine.
    [[nodiscard]]
    auto promise() const noexcept -> promise_type & {
        return m_coroutine.promise();
    }

    /// @brief
    ///   Rethrow exception or get coroutine return value.
    auto result() const -> void {
        m_coroutine.promise().result();
    }

    auto operator()() -> void {
        this->resume();
    }

    /// @brief
    ///   Make task awaitable.
    auto operator co_await() const noexcept -> task_awaiter<void> {
        return task_awaiter<void>{m_coroutine};
    }

    /// @brief
    ///   Get coroutine handle of this task.
    auto handle() const noexcept -> coroutine_handle {
        return m_coroutine;
    }

private:
    coroutine_handle m_coroutine{};
};

template <std::movable T>
class task<T> {
public:
    using promise_type     = promise<T>;
    using coroutine_handle = std::coroutine_handle<promise_type>;

    /// @brief
    ///   Create an empty task.
    task() noexcept = default;

    /// @brief
    ///   Create a new task from coroutine handle.
    explicit task(coroutine_handle coroutine) noexcept : m_coroutine(coroutine) {}

    /// @brief
    ///   Copy constructor of task is disabled.
    task(const task &) = delete;

    /// @brief
    ///   Copy assignment of task is disabled.
    auto operator=(const task &) = delete;

    /// @brief
    ///   Move constructor of task.
    ///
    /// @param other
    ///   The task to be moved. The moved task will be invalidated.
    task(task &&other) noexcept : m_coroutine(other.m_coroutine) {
        other.m_coroutine = nullptr;
    }

    /// @brief
    ///   Move assignment of task.
    ///
    /// @param other
    ///   The task to be moved. The moved task will be invalidated.
    auto operator=(task &&other) noexcept -> task & {
        if (m_coroutine != nullptr)
            m_coroutine.destroy();

        m_coroutine       = other.m_coroutine;
        other.m_coroutine = nullptr;
        return *this;
    }

    /// @brief
    ///   Destroy this task and the coroutine.
    ~task() noexcept {
        if (m_coroutine != nullptr)
            m_coroutine.destroy();
    }

    /// @brief
    ///   Checks if this task is finished.
    [[nodiscard]]
    auto done() const noexcept -> bool {
        return m_coroutine == nullptr || m_coroutine.done();
    }

    /// @brief
    ///   Resume this coroutine if suspended.
    auto resume() -> void {
        m_coroutine.resume();
    }

    /// @brief
    ///   Destroy this coroutine.
    auto destroy() noexcept -> void {
        if (m_coroutine == nullptr)
            return;
        m_coroutine.destroy();
        m_coroutine = nullptr;
    }

    /// @brief
    ///   Get promise of this coroutine.
    [[nodiscard]]
    auto promise() const noexcept -> promise_type & {
        return m_coroutine.promise();
    }

    /// @brief
    ///   Rethrow exception or get coroutine return value.
    auto result() const -> T & {
        return m_coroutine.promise().result();
    }

    /// @brief
    ///   Make task awaitable.
    auto operator co_await() const noexcept -> task_awaiter<T> {
        return task_awaiter<T>{m_coroutine};
    }

private:
    coroutine_handle m_coroutine{};
};

inline auto promise<void>::get_return_object() noexcept -> task<void> {
    return task<void>{std::coroutine_handle<promise<void>>::from_promise(*this)};
}

class thread_pool {
public:
    /// @brief
    ///   Create a new thread pool and start workers.
    explicit thread_pool(uint32_t worker_count = std::thread::hardware_concurrency()) {
        m_workers.reserve(worker_count);
        // Create workers.
        for (uint32_t i = 0; i < worker_count; ++i) {
            m_workers.emplace_back([this](std::stop_token stop_token) {
                std::coroutine_handle<> coroutine{};
                for (;;) {
                    if (stop_token.stop_requested())
                        break;

                    if (!this->m_tasks.try_pop(coroutine)) {
                        if (!SwitchToThread())
                            Sleep(1);
                        continue;
                    }

                    MemoryBarrier();
                    coroutine.resume();
                }
            });
        }
    }

    /// @brief
    ///   Stop all workers and destroy this thread pool.
    ~thread_pool() noexcept = default;

    /// @brief
    ///   Get number of worker threads in this thread pool.
    [[nodiscard]]
    auto worker_count() const noexcept -> uint32_t {
        return static_cast<uint32_t>(m_workers.size());
    }

    /// @brief
    ///   Schedule a coroutine in this thread pool.
    auto schedule(std::coroutine_handle<> coroutine) noexcept -> void {
        // Pend this coroutine to the end of task queue.
        m_tasks.push(coroutine);
    }

    /// @brief
    ///   Schedule current coroutine in this thread pool.
    auto schedule() {
        struct awaiter {
            thread_pool *m_thread_pool;

            constexpr auto await_ready() const noexcept -> bool {
                return false;
            }

            auto await_suspend(std::coroutine_handle<> coroutine) noexcept -> void {
                m_thread_pool->schedule(coroutine);
            }

            constexpr auto await_resume() noexcept -> void {}
        };

        return awaiter{this};
    }

private:
    struct auto_task {
        std::coroutine_handle<> coroutine;

        struct promise_type {
            auto get_return_object() noexcept -> auto_task {
                return auto_task{std::coroutine_handle<promise_type>::from_promise(*this)};
            }

            constexpr auto initial_suspend() const noexcept -> std::suspend_never {
                return {};
            }

            constexpr auto final_suspend() const noexcept -> std::suspend_never {
                return {};
            }

            auto unhandled_exception() noexcept -> void {
                std::terminate();
            }

            auto return_void() noexcept -> void {}
        };

        auto operator co_await() const noexcept {
            struct awaiter {
                constexpr auto await_ready() const noexcept -> bool {
                    return true;
                }

                constexpr auto await_suspend(std::coroutine_handle<>) noexcept -> void {}
                constexpr auto await_resume() const noexcept -> void {}
            };

            return awaiter{};
        }
    };

    template <typename Func, typename... Args>
    auto internal_schedule(Func &&func, Args &&...args) -> auto_task {
        if constexpr (std::is_rvalue_reference_v<Func &&>) {
            Func functor(std::move(func));
            co_await this->schedule();
            functor(std::forward<Args>(args)...);
        } else {
            co_await this->schedule();
            func(std::forward<Args>(args)...);
        }
    }

public:
    /// @brief
    ///   Pend a new task in this thread pool. The coroutine will be automatically destroyed once
    ///   finished.
    template <typename Func, typename... Args>
    auto schedule(Func &&func, Args &&...args) -> void {
        this->internal_schedule(std::forward<Func>(func), std::forward<Args>(args)...);
    }

private:
    std::vector<std::jthread>                              m_workers{};
    concurrency::concurrent_queue<std::coroutine_handle<>> m_tasks{};
};

class io_context {
public:
    /// @brief
    ///   Create a new async IO context.
    explicit io_context(thread_pool &task_ctx) : m_task_ctx(task_ctx), m_iocp(), m_stopped(true) {
        m_iocp = CreateIoCompletionPort(INVALID_HANDLE_VALUE, nullptr, 0, 0);
        if (m_iocp == nullptr)
            throw std::runtime_error("Failed to create IOCP.");
    }

    /// @brief
    ///   Copy constructor of IO context is disabled.
    io_context(const io_context &) = delete;

    /// @brief
    ///   Copy assignment of IO context is disabled.
    auto operator=(const io_context &) = delete;

    /// @brief
    ///   Destroy this IO context.
    ~io_context() noexcept {
        stop();
        CloseHandle(m_iocp);
    }

    /// @brief
    ///   Start handling IO events.
    auto run() -> void {
        BOOL        ret;
        DWORD       bytes;
        ULONG_PTR   completion_key;
        OVERLAPPED *overlapped;
        m_stopped.store(false, std::memory_order_relaxed);
        while (!m_stopped.load(std::memory_order_relaxed)) {
            ret = GetQueuedCompletionStatus(m_iocp, &bytes, &completion_key, &overlapped, INFINITE);
            if (m_stopped.load(std::memory_order_relaxed))
                break;

            if (!ret)
                continue;

            void *address   = reinterpret_cast<void *>(completion_key);
            auto  coroutine = std::coroutine_handle<>::from_address(address);
            m_task_ctx.schedule(coroutine);
        }
    }

    /// @brief
    ///   Stop handling IO events.
    auto stop() noexcept -> void {
        if (!m_stopped.exchange(true))
            PostQueuedCompletionStatus(m_iocp, 0, 0, nullptr);
    }

    /// @brief
    ///   Get task context of this IO context.
    [[nodiscard]]
    auto task_context() const noexcept -> thread_pool & {
        return m_task_ctx;
    }

    /// @brief
    ///   Start an async read task.
    auto read(HANDLE file, void *buffer, std::uint64_t offset, std::uint32_t size) noexcept {
        class awaiter {
        public:
            awaiter(HANDLE        iocp,
                    HANDLE        file,
                    void         *buffer,
                    std::uint64_t offset,
                    std::uint32_t size) noexcept
                : m_iocp(iocp),
                  m_file(file),
                  m_buffer(buffer),
                  m_overlapped(),
                  m_success(),
                  m_size(),
                  m_bytes_read() {
                LARGE_INTEGER off;
                off.QuadPart            = offset;
                m_overlapped.Offset     = off.LowPart;
                m_overlapped.OffsetHigh = off.HighPart;
            }

            constexpr auto await_ready() const noexcept -> bool {
                return false;
            }

            auto await_suspend(std::coroutine_handle<> coroutine) noexcept -> bool {
                // Register to IOCP.
                void     *addr = coroutine.address();
                ULONG_PTR key  = reinterpret_cast<ULONG_PTR>(addr);
                if (CreateIoCompletionPort(m_file, m_iocp, key, 0) != m_iocp)
                    return false;

                // Start overlapped IO task.
                BOOL  ret       = ReadFile(m_file, m_buffer, m_size, &m_bytes_read, &m_overlapped);
                DWORD errorCode = GetLastError();
                if (!ret && errorCode != ERROR_IO_PENDING)
                    return false;

                m_success = true;
                return ret ? false : true;
            }

            auto await_resume() noexcept -> std::int64_t {
                return m_success ? m_bytes_read : -1;
            }

        private:
            HANDLE        m_iocp;
            HANDLE        m_file;
            void         *m_buffer;
            OVERLAPPED    m_overlapped;
            bool          m_success;
            std::uint32_t m_size;
            DWORD         m_bytes_read;
        };
        return awaiter{m_iocp, file, buffer, offset, size};
    }

    /// @brief
    ///   Start an async write task.
    auto write(HANDLE file, const void *buffer, std::uint64_t offset, std::uint32_t size) noexcept {
        class awaiter {
        public:
            awaiter(HANDLE        iocp,
                    HANDLE        file,
                    const void   *buffer,
                    std::uint64_t offset,
                    std::uint32_t size) noexcept
                : m_iocp(iocp),
                  m_file(file),
                  m_buffer(buffer),
                  m_overlapped(),
                  m_success(),
                  m_size(),
                  m_bytes_written() {
                LARGE_INTEGER off;
                off.QuadPart            = offset;
                m_overlapped.Offset     = off.LowPart;
                m_overlapped.OffsetHigh = off.HighPart;
            }

            constexpr auto await_ready() const noexcept -> bool {
                return false;
            }

            auto await_suspend(std::coroutine_handle<> coroutine) noexcept -> bool {
                // Register to IOCP.
                void     *addr = coroutine.address();
                ULONG_PTR key  = reinterpret_cast<ULONG_PTR>(addr);
                if (CreateIoCompletionPort(m_file, m_iocp, key, 0) != m_iocp)
                    return false;

                // Start overlapped IO task.
                BOOL  ret = WriteFile(m_file, m_buffer, m_size, &m_bytes_written, &m_overlapped);
                DWORD errorCode = GetLastError();
                if (!ret && errorCode != ERROR_IO_PENDING)
                    return false;

                m_success = true;
                return ret ? false : true;
            }

            auto await_resume() noexcept -> std::int64_t {
                return m_success ? m_bytes_written : -1;
            }

        private:
            HANDLE        m_iocp;
            HANDLE        m_file;
            const void   *m_buffer;
            OVERLAPPED    m_overlapped;
            bool          m_success;
            std::uint32_t m_size;
            DWORD         m_bytes_written;
        };
        return awaiter{m_iocp, file, buffer, offset, size};
    }

    /// @brief
    ///   Start an async accept task.
    auto accept(SOCKET listener, SOCKET peer) {
        class awaiter {
        public:
            awaiter(HANDLE iocp, SOCKET listen, SOCKET peer) noexcept
                : m_iocp(iocp),
                  m_listener(listen),
                  m_peer(peer),
                  m_bytes_received(),
                  m_buffer(),
                  m_overlapped() {}

            constexpr auto await_ready() const noexcept -> bool {
                return false;
            }

            auto await_suspend(std::coroutine_handle<> coroutine) noexcept -> bool {
                // Get AcceptEx function pointer.
                LPFN_ACCEPTEX accept_ex;
                { // Get AcceptEx function pointer.
                    DWORD bytes_out;
                    GUID  accept_ex_guid = WSAID_ACCEPTEX;
                    int   result         = WSAIoctl(m_listener, SIO_GET_EXTENSION_FUNCTION_POINTER,
                                                    &accept_ex_guid, sizeof(accept_ex_guid), &accept_ex,
                                                    sizeof(accept_ex), &bytes_out, nullptr, nullptr);

                    // Failed to get AcceptEx function pointer and resume current coroutine
                    // immediately.
                    if (result != 0) {
                        m_peer = INVALID_SOCKET;
                        return false;
                    }
                }

                // Register IOCP.
                void     *addr            = coroutine.address();
                ULONG_PTR key             = reinterpret_cast<ULONG_PTR>(addr);
                HANDLE    listener_handle = reinterpret_cast<HANDLE>(m_listener);
                if (CreateIoCompletionPort(listener_handle, m_iocp, key, 0) != m_iocp) {
                    m_peer = INVALID_SOCKET;
                    return false;
                }

                // Start AcceptEx.
                BOOL ret = accept_ex(m_listener, m_peer, m_buffer, sizeof(m_buffer),
                                     sizeof(SOCKADDR_STORAGE), sizeof(SOCKADDR_STORAGE),
                                     &m_bytes_received, &m_overlapped);
                if (!ret) {
                    int errorCode = WSAGetLastError();
                    if (errorCode != WSA_IO_PENDING) {
                        m_peer = INVALID_SOCKET;
                        return false;
                    }
                }

                return true;
            }

            auto await_resume() const noexcept -> bool {
                return m_peer != INVALID_SOCKET;
            }

        private:
            HANDLE     m_iocp;
            SOCKET     m_listener;
            SOCKET     m_peer;
            DWORD      m_bytes_received;
            std::byte  m_buffer[2 * (sizeof(SOCKADDR_STORAGE) + 16)];
            OVERLAPPED m_overlapped;
        };

        return awaiter{m_iocp, listener, peer};
    }

private:
    thread_pool     &m_task_ctx;
    HANDLE           m_iocp;
    std::atomic_bool m_stopped;
};

} // namespace coro
