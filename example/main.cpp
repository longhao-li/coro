#include "../coro.h"

#include <iostream>

using namespace coro;

auto read_func(io_context &io_ctx, SOCKET peer) -> task<void> {
    char         buffer[256];
    std::int64_t ret =
        co_await io_ctx.read(reinterpret_cast<HANDLE>(peer), buffer, 0, sizeof(buffer));

    if (ret < 0)
        std::cout << "Failed to receive data from peer socket " << peer << '\n';
    else
        std::cout << buffer << '\n';

    closesocket(peer);
}

auto accept_task(io_context &io_ctx, SOCKET local) -> task<void> {
    std::cout << "accept_task started.\n";
    thread_pool &task_ctx = io_ctx.task_context();
    for (;;) {
        SOCKET peer = socket(AF_INET, SOCK_STREAM, 0);
        if (peer == INVALID_SOCKET)
            throw std::runtime_error("Failed to create new socket.");

        bool result = co_await io_ctx.accept(local, peer);
        if (!result) {
            closesocket(peer);
            throw std::runtime_error("Failed to accept new socket.");
        }

        task_ctx.schedule(read_func(io_ctx, peer));
    }
}

auto main() -> int {
    thread_pool task_ctx;
    io_context  io_ctx(task_ctx);

    WSADATA wsa_data;
    WSAStartup(MAKEWORD(2, 2), &wsa_data);

    SOCKET      local = socket(AF_INET, SOCK_STREAM, 0);
    SOCKADDR_IN addr;
    addr.sin_family           = AF_INET;
    addr.sin_port             = htons(2333);
    addr.sin_addr.S_un.S_addr = htonl(INADDR_LOOPBACK);

    bind(local, reinterpret_cast<const sockaddr *>(&addr), sizeof(addr));
    listen(local, SOMAXCONN);

    task_ctx.schedule(accept_task(io_ctx, local));
    io_ctx.run();
}
