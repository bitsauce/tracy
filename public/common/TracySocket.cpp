#include <assert.h>
#include <cerrno>
#include <chrono>
#include <inttypes.h>
#include <new>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <thread>

#include "TracyAlloc.hpp"
#include "TracySocket.hpp"
#include "TracySystem.hpp"

#ifdef __EMSCRIPTEN__
#  include <condition_variable>
#  include <deque>
#  include <mutex>
#  include <vector>
#  include <emscripten/emscripten.h>
#  include <emscripten/threading.h>
#  include <emscripten/websocket.h>
#endif

#ifdef _WIN32
#  ifndef NOMINMAX
#    define NOMINMAX
#  endif
#  include <winsock2.h>
#  include <ws2tcpip.h>
#  ifdef _MSC_VER
#    pragma warning(disable:4244)
#    pragma warning(disable:4267)
#  endif
#  define poll WSAPoll
#  ifdef _MSC_VER
#    pragma comment(lib, "ws2_32.lib")
#  endif
#else
#  include <arpa/inet.h>
#  include <sys/socket.h>
#  include <sys/param.h>
#  include <errno.h>
#  include <fcntl.h>
#  include <netinet/in.h>
#  include <netdb.h>
#  include <unistd.h>
#  include <poll.h>
#endif

#ifdef ENABLE_WEBSOCKETS
#ifdef ENABLE_SECURE_WEBSOCKETS
#include <websocketpp/config/asio.hpp>
#endif
#include <websocketpp/config/asio_no_tls.hpp>
#include <websocketpp/server.hpp>
#endif

#ifndef MSG_NOSIGNAL
#  define MSG_NOSIGNAL 0
#endif

namespace tracy
{

#ifdef _WIN32
typedef SOCKET socket_t;
#else
typedef int socket_t;
#endif

#ifdef _WIN32
struct __wsinit
{
    __wsinit()
    {
        WSADATA wsaData;
        if( WSAStartup( MAKEWORD( 2, 2 ), &wsaData ) != 0 )
        {
            fprintf( stderr, "Cannot init winsock.\n" );
            exit( 1 );
        }
    }
};

void InitWinSock()
{
    static __wsinit init;
}
#endif


enum { BufSize = 128 * 1024 };

#ifdef __EMSCRIPTEN__
// pImpl state for the emscripten WebSocket-backed Socket. Lives in main browser
// thread land (where ws callbacks fire) and is read/written by the Tracy network
// pthread (which calls Connect/Send/Recv). All cross-thread state goes through
// the mutex; the worker blocks on `cv` for both connect completion and message
// arrival.
struct EmWsImpl
{
    enum class State { None, Connecting, Open, Failed, Closed };

    EMSCRIPTEN_WEBSOCKET_T ws = 0;
    std::atomic<State> state { State::None };

    std::mutex mtx;
    std::condition_variable cv;
    std::deque<std::vector<uint8_t>> queue;
    size_t head = 0;                 // bytes consumed from queue.front()

    static EM_BOOL OnOpen ( int, const EmscriptenWebSocketOpenEvent*,    void* );
    static EM_BOOL OnClose( int, const EmscriptenWebSocketCloseEvent*,   void* );
    static EM_BOOL OnError( int, const EmscriptenWebSocketErrorEvent*,   void* );
    static EM_BOOL OnMsg  ( int, const EmscriptenWebSocketMessageEvent*, void* );
};

EM_BOOL EmWsImpl::OnOpen( int, const EmscriptenWebSocketOpenEvent*, void* user )
{
    auto self = static_cast<EmWsImpl*>( user );
    {
        std::lock_guard lk( self->mtx );
        self->state.store( State::Open, std::memory_order_release );
    }
    self->cv.notify_all();
    return EM_TRUE;
}

EM_BOOL EmWsImpl::OnClose( int, const EmscriptenWebSocketCloseEvent*, void* user )
{
    auto self = static_cast<EmWsImpl*>( user );
    {
        std::lock_guard lk( self->mtx );
        const auto prev = self->state.load( std::memory_order_acquire );
        // If we never reached Open, treat as a connect-time failure so Connect()
        // returns false rather than reporting a healthy-then-closed link.
        self->state.store( prev == State::Open ? State::Closed : State::Failed,
                           std::memory_order_release );
    }
    self->cv.notify_all();
    return EM_TRUE;
}

EM_BOOL EmWsImpl::OnError( int, const EmscriptenWebSocketErrorEvent*, void* user )
{
    auto self = static_cast<EmWsImpl*>( user );
    {
        std::lock_guard lk( self->mtx );
        self->state.store( State::Failed, std::memory_order_release );
    }
    self->cv.notify_all();
    return EM_TRUE;
}

EM_BOOL EmWsImpl::OnMsg( int, const EmscriptenWebSocketMessageEvent* e, void* user )
{
    // Tracy speaks binary only — ignore text frames defensively.
    if( e->isText ) return EM_TRUE;
    auto self = static_cast<EmWsImpl*>( user );
    {
        std::lock_guard lk( self->mtx );
        self->queue.emplace_back( e->data, e->data + e->numBytes );
    }
    self->cv.notify_all();
    return EM_TRUE;
}
#endif

Socket::Socket()
    : m_buf( (char*)tracy_malloc( BufSize ) )
    , m_bufPtr( nullptr )
    , m_sock( -1 )
    , m_bufLeft( 0 )
    , m_ptr( nullptr )
#ifdef __EMSCRIPTEN__
    , m_emWs( new EmWsImpl )
#endif
    , m_alive( false )
{
#ifdef _WIN32
    InitWinSock();
#endif
}

Socket::Socket( int sock )
    : m_buf( (char*)tracy_malloc( BufSize ) )
    , m_bufPtr( nullptr )
    , m_sock( sock )
    , m_bufLeft( 0 )
    , m_ptr( nullptr )
#ifdef __EMSCRIPTEN__
    , m_emWs( nullptr )   // accept-side; never used under emscripten (no listen)
#endif
    , m_alive( true )
{
}

Socket::~Socket()
{
    tracy_free( m_buf );
#ifdef __EMSCRIPTEN__
    if( m_emWs )
    {
        if( m_emWs->ws )
        {
            emscripten_websocket_close( m_emWs->ws, 1000, "shutdown" );
            emscripten_websocket_delete( m_emWs->ws );
        }
        delete m_emWs;
    }
#else
    if( m_sock.load( std::memory_order_relaxed ) != -1 )
    {
        Close();
    }
#endif
    if( m_ptr )
    {
        freeaddrinfo( m_res );
#ifdef _WIN32
        closesocket( m_connSock );
#else
        close( m_connSock );
#endif
    }
}

bool Socket::Connect( const char* addr, uint16_t port, bool tls )
{
    assert( !IsValid() );

#ifdef __EMSCRIPTEN__
    // Browsers don't have BSD sockets — go straight to the WebSocket API.
    // The handshake is async, so we wait on the impl's condvar for the
    // open/error/close callback to land on the main browser thread.
    assert( m_emWs && m_emWs->ws == 0 );
    char url[256];
    snprintf( url, sizeof url, "%s://%s:%u/", tls ? "wss" : "ws", addr, port );

    EmscriptenWebSocketCreateAttributes attr;
    emscripten_websocket_init_create_attributes( &attr );
    attr.url = url;
    attr.protocols = "binary";
    attr.createOnMainThread = EM_TRUE;

    m_emWs->state.store( EmWsImpl::State::Connecting, std::memory_order_release );
    m_emWs->ws = emscripten_websocket_new( &attr );
    if( m_emWs->ws <= 0 )
    {
        m_emWs->ws = 0;
        m_emWs->state.store( EmWsImpl::State::Failed, std::memory_order_release );
        return false;
    }

    // Run callbacks on the main browser thread, not the Tracy network pthread —
    // the pthread blocks on cv below (Atomics.wait) and would never pump events.
    const pthread_t cbThread = EM_CALLBACK_THREAD_CONTEXT_MAIN_BROWSER_THREAD;
    emscripten_websocket_set_onopen_callback_on_thread   ( m_emWs->ws, m_emWs, EmWsImpl::OnOpen,  cbThread );
    emscripten_websocket_set_onerror_callback_on_thread  ( m_emWs->ws, m_emWs, EmWsImpl::OnError, cbThread );
    emscripten_websocket_set_onclose_callback_on_thread  ( m_emWs->ws, m_emWs, EmWsImpl::OnClose, cbThread );
    emscripten_websocket_set_onmessage_callback_on_thread( m_emWs->ws, m_emWs, EmWsImpl::OnMsg,   cbThread );

    {
        std::unique_lock lk( m_emWs->mtx );
        m_emWs->cv.wait( lk, [&]{
            return m_emWs->state.load( std::memory_order_acquire ) != EmWsImpl::State::Connecting;
        } );
    }

    if( m_emWs->state.load( std::memory_order_acquire ) != EmWsImpl::State::Open )
    {
        emscripten_websocket_delete( m_emWs->ws );
        m_emWs->ws = 0;
        return false;
    }
    m_alive = true;
    return true;
#endif

    if( m_ptr )
    {
        const auto c = connect( m_connSock, m_ptr->ai_addr, m_ptr->ai_addrlen );
        if( c == -1 )
        {
#if defined _WIN32
            const auto err = WSAGetLastError();
            if( err == WSAEALREADY || err == WSAEINPROGRESS ) return false;
            if( err != WSAEISCONN )
            {
                freeaddrinfo( m_res );
                closesocket( m_connSock );
                m_ptr = nullptr;
                return false;
            }
#else
            const auto err = errno;
            if( err == EALREADY || err == EINPROGRESS ) return false;
            if( err != EISCONN )
            {
                freeaddrinfo( m_res );
                close( m_connSock );
                m_ptr = nullptr;
                return false;
            }
#endif
        }

#if defined _WIN32
        u_long nonblocking = 0;
        ioctlsocket( m_connSock, FIONBIO, &nonblocking );
#else
        int flags = fcntl( m_connSock, F_GETFL, 0 );
        fcntl( m_connSock, F_SETFL, flags & ~O_NONBLOCK );
#endif
        m_sock.store( m_connSock, std::memory_order_relaxed );
        m_alive = true;
        freeaddrinfo( m_res );
        m_ptr = nullptr;
        return true;
    }

    struct addrinfo hints;
    struct addrinfo *res, *ptr;

    memset( &hints, 0, sizeof( hints ) );
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    char portbuf[32];
    sprintf( portbuf, "%" PRIu16, port );

    if( getaddrinfo( addr, portbuf, &hints, &res ) != 0 ) return false;
    int sock = 0;
    for( ptr = res; ptr; ptr = ptr->ai_next )
    {
        if( ( sock = socket( ptr->ai_family, ptr->ai_socktype, ptr->ai_protocol ) ) == -1 ) continue;
#if defined __APPLE__
        int val = 1;
        setsockopt( sock, SOL_SOCKET, SO_NOSIGPIPE, &val, sizeof( val ) );
#endif
#if defined _WIN32
        u_long nonblocking = 1;
        ioctlsocket( sock, FIONBIO, &nonblocking );
#else
        int flags = fcntl( sock, F_GETFL, 0 );
        fcntl( sock, F_SETFL, flags | O_NONBLOCK );
#endif
        auto success = connect( sock, ptr->ai_addr, ptr->ai_addrlen ) == 0;
        if( success )
        {
            break;
        }
        else
        {
#if defined _WIN32
            const auto err = WSAGetLastError();
            if( err != WSAEWOULDBLOCK )
            {
                closesocket( sock );
                continue;
            }
#else
            if( errno != EINPROGRESS )
            {
                close( sock );
                continue;
            }
#endif
        }
        m_res = res;
        m_ptr = ptr;
        m_connSock = sock;
        return false;
    }
    freeaddrinfo( res );
    if( !ptr ) return false;

#if defined _WIN32
    u_long nonblocking = 0;
    ioctlsocket( sock, FIONBIO, &nonblocking );
#else
    int flags = fcntl( sock, F_GETFL, 0 );
    fcntl( sock, F_SETFL, flags & ~O_NONBLOCK );
#endif

    m_sock.store( sock, std::memory_order_relaxed );
    m_alive = true;
    return true;
}

bool Socket::ConnectBlocking( const char* addr, uint16_t port, bool tls )
{
    assert( !IsValid() );
#ifdef __EMSCRIPTEN__
    // Our emscripten Connect already blocks until the handshake resolves, so the
    // blocking variant is just a forward — there's no two-stage non-blocking
    // path for browser WebSockets to begin with.
    return Connect( addr, port, tls );
#endif
    assert( !m_ptr );

    struct addrinfo hints;
    struct addrinfo *res, *ptr;

    memset( &hints, 0, sizeof( hints ) );
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    char portbuf[32];
    sprintf( portbuf, "%" PRIu16, port );

    if( getaddrinfo( addr, portbuf, &hints, &res ) != 0 ) return false;
    int sock = 0;
    for( ptr = res; ptr; ptr = ptr->ai_next )
    {
        if( ( sock = socket( ptr->ai_family, ptr->ai_socktype, ptr->ai_protocol ) ) == -1 ) continue;
#if defined __APPLE__
        int val = 1;
        setsockopt( sock, SOL_SOCKET, SO_NOSIGPIPE, &val, sizeof( val ) );
#endif
        if( connect( sock, ptr->ai_addr, ptr->ai_addrlen ) == -1 )
        {
#ifdef _WIN32
            closesocket( sock );
#else
            close( sock );
#endif
            continue;
        }
        break;
    }
    freeaddrinfo( res );
    if( !ptr ) return false;

    m_sock.store( sock, std::memory_order_relaxed );
    m_alive = true;
    return true;
}

void Socket::Close()
{
#ifdef __EMSCRIPTEN__
    if( m_emWs && m_emWs->ws )
    {
        emscripten_websocket_close( m_emWs->ws, 1000, "shutdown" );
        emscripten_websocket_delete( m_emWs->ws );
        m_emWs->ws = 0;
    }
    m_alive = false;
    return;
#endif
    const auto sock = m_sock.load( std::memory_order_relaxed );
    assert( sock != -1 );
#ifdef _WIN32
    closesocket( sock );
#else
    close( sock );
#endif
    m_sock.store( -1, std::memory_order_relaxed );
}

int Socket::Send( const void* _buf, int len )
{
#ifdef __EMSCRIPTEN__
    if( !m_emWs || !m_emWs->ws || !m_alive ) return -1;
    if( emscripten_websocket_send_binary( m_emWs->ws, const_cast<void*>(_buf), len ) < 0 )
    {
        m_alive = false;
        return -1;
    }
    return len;
#else
    const auto sock = m_sock.load( std::memory_order_relaxed );
    auto buf = (const char*)_buf;
    assert( sock != -1 );
    auto start = buf;
    while( len > 0 )
    {
        auto ret = send( sock, buf, len, MSG_NOSIGNAL );
        if( ret == -1 ) return -1;
        len -= ret;
        buf += ret;
    }
    return int( buf - start );
#endif
}

int Socket::GetSendBufSize()
{
    const auto sock = m_sock.load( std::memory_order_relaxed );
    int bufSize;
#if defined _WIN32
    int sz = sizeof( bufSize );
    getsockopt( sock, SOL_SOCKET, SO_SNDBUF, (char*)&bufSize, &sz );
#else
    socklen_t sz = sizeof( bufSize );
    getsockopt( sock, SOL_SOCKET, SO_SNDBUF, &bufSize, &sz );
#endif
    return bufSize;
}

int Socket::RecvBuffered( void* buf, int len, int timeout )
{
    if( len <= m_bufLeft )
    {
        memcpy( buf, m_bufPtr, len );
        m_bufPtr += len;
        m_bufLeft -= len;
        return len;
    }

    if( m_bufLeft > 0 )
    {
        memcpy( buf, m_bufPtr, m_bufLeft );
        const auto ret = m_bufLeft;
        m_bufLeft = 0;
        return ret;
    }

    if( len >= BufSize ) return Recv( buf, len, timeout );

    m_bufLeft = Recv( m_buf, BufSize, timeout );
    if( m_bufLeft <= 0 ) return m_bufLeft;

    const auto sz = len < m_bufLeft ? len : m_bufLeft;
    memcpy( buf, m_buf, sz );
    m_bufPtr = m_buf + sz;
    m_bufLeft -= sz;
    return sz;
}

int Socket::Recv( void* _buf, int len, int timeout )
{
#ifdef __EMSCRIPTEN__
    if( !m_emWs ) return -1;
    auto buf = (char*)_buf;
    std::unique_lock lk( m_emWs->mtx );
    if( m_emWs->queue.empty() )
    {
        // No frame buffered. Wait up to `timeout` ms for one to arrive or for the
        // connection to drop. The cv is signalled by OnMsg/OnClose/OnError on the
        // main browser thread; either way we'll wake and re-check.
        auto deadline = std::chrono::milliseconds( timeout );
        m_emWs->cv.wait_for( lk, deadline, [&]{
            if( !m_emWs->queue.empty() ) return true;
            const auto s = m_emWs->state.load( std::memory_order_acquire );
            return s == EmWsImpl::State::Failed || s == EmWsImpl::State::Closed;
        } );
        if( m_emWs->queue.empty() )
        {
            const auto s = m_emWs->state.load( std::memory_order_acquire );
            if( s == EmWsImpl::State::Failed || s == EmWsImpl::State::Closed )
            {
                m_alive = false;
                return -1;
            }
            // Timeout — match Socket::Recv semantics (-1 means "no data this turn").
            return -1;
        }
    }
    auto& front = m_emWs->queue.front();
    const auto avail = front.size() - m_emWs->head;
    const auto n = (size_t)len < avail ? (size_t)len : avail;
    memcpy( buf, front.data() + m_emWs->head, n );
    m_emWs->head += n;
    if( m_emWs->head == front.size() )
    {
        m_emWs->queue.pop_front();
        m_emWs->head = 0;
    }
    return (int)n;
#else
    const auto sock = m_sock.load( std::memory_order_relaxed );
    auto buf = (char*)_buf;

    struct pollfd fd;
    fd.fd = (socket_t)sock;
    fd.events = POLLIN;

    if( poll( &fd, 1, timeout ) > 0 )
    {
        const auto read = recv( sock, buf, len, 0 );
        if( read == 0 )
        {
            m_alive = false;
            return -1;
        }
        return read;
    }
    else
    {
        return -1;
    }
#endif
}

int Socket::ReadUpTo( void* _buf, int len )
{
#ifdef __EMSCRIPTEN__
    // No long-blocking read for the WS backend — drain whatever's queued and
    // wait briefly for the next frame, mirroring the BSD recv() semantics
    // closely enough for ReadImpl's callers.
    auto buf = (char*)_buf;
    int rd = 0;
    while( len > 0 )
    {
        const auto res = Recv( buf, len, 10000 );
        if( res == 0 ) break;
        if( res == -1 ) return rd > 0 ? rd : -1;
        len -= res;
        rd += res;
        buf += res;
    }
    return rd;
#else
    const auto sock = m_sock.load( std::memory_order_relaxed );
    auto buf = (char*)_buf;

    int rd = 0;
    while( len > 0 )
    {
        const auto res = recv( sock, buf, len, 0 );
        if( res == 0 ) break;
        if( res == -1 ) return -1;
        len -= res;
        rd += res;
        buf += res;
    }
    return rd;
#endif
}

bool Socket::Read( void* buf, int len, int timeout )
{
    auto cbuf = (char*)buf;
    while( len > 0 )
    {
        if( !ReadImpl( cbuf, len, timeout ) ) return false;
    }
    return true;
}

bool Socket::ReadImpl( char*& buf, int& len, int timeout )
{
    const auto sz = RecvBuffered( buf, len, timeout );
    switch( sz )
    {
    case 0:
        return false;
    case -1:
    {
#ifdef _WIN32
        auto err = WSAGetLastError();
        if( err == WSAECONNABORTED || err == WSAECONNRESET ) return false;
#endif
        if (!IsValid()) return false;
    }
    break;
    default:
        len -= sz;
        buf += sz;
        break;
    }
    return true;
}

bool Socket::ReadRaw( void* _buf, int len, int timeout )
{
    auto buf = (char*)_buf;
    while( len > 0 )
    {
        const auto sz = Recv( buf, len, timeout );
        if( sz <= 0 ) return false;
        len -= sz;
        buf += sz;
    }
    return true;
}

bool Socket::HasData()
{
    if( m_bufLeft > 0 ) return true;
#ifdef __EMSCRIPTEN__
    if( !m_emWs ) return false;
    std::lock_guard lk( m_emWs->mtx );
    return !m_emWs->queue.empty();
#else
    const auto sock = m_sock.load( std::memory_order_relaxed );

    struct pollfd fd;
    fd.fd = (socket_t)sock;
    fd.events = POLLIN;

    return poll( &fd, 1, 0 ) > 0;
#endif
}

bool Socket::IsValid() const
{
#ifdef __EMSCRIPTEN__
    // Consult state, not just m_alive — OnClose/OnError can fire on the main
    // thread between worker Recv calls, and m_alive isn't cleared until the
    // next Recv observes the failure.
    if( !m_emWs || m_emWs->ws == 0 ) return false;
    return m_emWs->state.load( std::memory_order_acquire ) == EmWsImpl::State::Open && m_alive;
#else
    return m_sock.load( std::memory_order_relaxed ) >= 0 && m_alive;
#endif
}


ListenSocket::ListenSocket()
    : m_sock( -1 )
#ifdef ENABLE_WEBSOCKETS
    , m_webSocket( nullptr )
#endif
{
#ifdef _WIN32
    InitWinSock();
#endif
}

ListenSocket::~ListenSocket()
{
    if( m_sock != -1 ) Close();
#ifdef ENABLE_WEBSOCKETS
    delete m_webSocket;
#endif
}

static int addrinfo_and_socket_for_family( uint16_t port, int ai_family, struct addrinfo** res )
{
    struct addrinfo hints;
    memset( &hints, 0, sizeof( hints ) );
    hints.ai_family = ai_family;
    hints.ai_socktype = SOCK_STREAM;
#ifndef TRACY_ONLY_LOCALHOST
    const char* onlyLocalhost = GetEnvVar( "TRACY_ONLY_LOCALHOST" );
    if( !onlyLocalhost || onlyLocalhost[0] != '1' )
    {
        hints.ai_flags = AI_PASSIVE;
    }
#endif
    char portbuf[32];
    sprintf( portbuf, "%" PRIu16, port );
    if( getaddrinfo( nullptr, portbuf, &hints, res ) != 0 ) return -1;
    int sock = socket( (*res)->ai_family, (*res)->ai_socktype, (*res)->ai_protocol );
    if (sock == -1) freeaddrinfo( *res );
    return sock;
}

bool ListenSocket::Listen( uint16_t port, int backlog )
{
    assert( m_sock == -1 );

    struct addrinfo* res = nullptr;

#if !defined TRACY_ONLY_IPV4 && !defined TRACY_ONLY_LOCALHOST
    const char* onlyIPv4 = GetEnvVar( "TRACY_ONLY_IPV4" );
    if( !onlyIPv4 || onlyIPv4[0] != '1' )
    {
        m_sock = addrinfo_and_socket_for_family( port, AF_INET6, &res );
    }
#endif
    if (m_sock == -1)
    {
        // IPV6 protocol may not be available/is disabled. Try to create a socket
        // with the IPV4 protocol
        m_sock = addrinfo_and_socket_for_family( port, AF_INET, &res );
        if( m_sock == -1 ) return false;
    }
#if defined _WIN32
    unsigned long val = 0;
    setsockopt( m_sock, IPPROTO_IPV6, IPV6_V6ONLY, (const char*)&val, sizeof( val ) );
#elif defined BSD
    int val = 0;
    setsockopt( m_sock, IPPROTO_IPV6, IPV6_V6ONLY, (const char*)&val, sizeof( val ) );
    val = 1;
    setsockopt( m_sock, SOL_SOCKET, SO_REUSEADDR, &val, sizeof( val ) );
#else
    int val = 1;
    setsockopt( m_sock, SOL_SOCKET, SO_REUSEADDR, &val, sizeof( val ) );
#endif
    if( bind( m_sock, res->ai_addr, res->ai_addrlen ) == -1 ) { freeaddrinfo( res ); Close(); return false; }
    if( listen( m_sock, backlog ) == -1 ) { freeaddrinfo( res ); Close(); return false; }
    freeaddrinfo( res );
    return true;
}

Socket* ListenSocket::Accept()
{
    struct sockaddr_storage remote;
    socklen_t sz = sizeof( remote );

    struct pollfd fd;
    fd.fd = (socket_t)m_sock;
    fd.events = POLLIN;

    if( poll( &fd, 1, 10 ) > 0 )
    {
        int sock = accept( m_sock, (sockaddr*)&remote, &sz);
        if( sock == -1 ) return nullptr;

#if defined __APPLE__
        int val = 1;
        setsockopt( sock, SOL_SOCKET, SO_NOSIGPIPE, &val, sizeof( val ) );
#endif

        auto ptr = (Socket*)tracy_malloc( sizeof( Socket ) );
        new(ptr) Socket( sock );
        return ptr;
    }
    else
    {
        return nullptr;
    }
}

#ifdef ENABLE_WEBSOCKETS
class WebSocket : public Socket
{
public:
    using server = websocketpp::server<websocketpp::config::asio>;
#ifdef ENABLE_SECURE_WEBSOCKETS
    using server_tls = websocketpp::server<websocketpp::config::asio_tls>;
#endif

    WebSocket( uint16_t port, bool tls )
    {
        if( tls )
        {
#ifdef ENABLE_SECURE_WEBSOCKETS
            m_threadTls = std::thread( [this, port] { Server<true>( m_serverTls, port ); } );
#else
            (void)port;
#endif
        }
        else
        {
            m_thread = std::thread( [this, port] { Server<false>( m_server, port ); } );
        }
    }

    ~WebSocket() override
    {
        // Order matters: set shutdown first so a server thread that hasn't reached
        // listen() yet will bail out, then stop any listeners that did start.
        m_shutdown.store( true, std::memory_order_relaxed );
        StopListening();
        Close();
        // Only one of the two threads is started (the one matching the chosen
        // scheme); join whichever is actually running.
        if( m_thread.joinable() ) m_thread.join();
#ifdef ENABLE_SECURE_WEBSOCKETS
        if( m_threadTls.joinable() ) m_threadTls.join();
#endif
    }

    template <bool TLS, typename T>
    void Server( websocketpp::server<T>& server, uint16_t port )
    {
        if constexpr (TLS) SetThreadName( "WebSocket TLS Server" );
        else SetThreadName( "WebSocket Server" );

        server.init_asio();

        // Disable logging
        server.set_access_channels( websocketpp::log::alevel::none );
        server.set_error_channels( websocketpp::log::elevel::none );

        server.set_validate_handler(
            [&server]( const websocketpp::connection_hdl& hdl )
            {
                const auto con = server.get_con_from_hdl( hdl );
                const auto& protocols = con->get_requested_subprotocols();
                for( const auto& protocol : protocols )
                {
                    if( protocol == "binary" )
                    {
                        con->select_subprotocol( "binary" );
                        return true;
                    }
                }
                printf( "WebSocket: Expected binary protocol on handler\n" );
                return false;
            });

        server.set_message_handler(
            [this]( const websocketpp::connection_hdl& hdl, const server::message_ptr& msg )
            {
                if ( m_hdl.lock() != hdl.lock() ) return; // Ignore messages from any connection other than the first one
                try {
                    if( msg->get_opcode() != websocketpp::frame::opcode::binary )
                    {
                        printf( "WebSocket%s: Expected binary data protocol\n", TLS ? "TLS" : "" );
                        return;
                    }
                    std::lock_guard lock( m_messageQueueMutex );
                    m_messageQueueThread.emplace_back( std::move( msg->get_raw_payload() ) );
                } catch( ... ) {
                    printf( "WebSocket%s: Message handler encountered an exception\n", TLS ? "TLS" : "" );
                }
            });

        server.set_open_handler(
            [this, &server]( const websocketpp::connection_hdl& hdl )
            {
                std::lock_guard lock( m_hdlMutex );
                if( m_connectionType != ConnectionType::None )
                {
                    static bool printOnce = true;
                    if (printOnce) printf("WebSocket%s: Another client attempted to connect while a connection has been established, closing new connection\n", TLS ? "TLS" : "");
                    printOnce = false;
                    server.close( hdl, websocketpp::close::status::going_away, "shutdown" );
                    return;
                }
                printf("WebSocket%s: Client connected\n", TLS ? "TLS" : "");
                m_hdl = hdl;
                m_alive = true;
                if constexpr( TLS )
                {
                    m_connectionType = ConnectionType::TLS;
                    if( m_server.is_listening() ) m_server.stop_listening();
                }
                else
                {
                    m_connectionType = ConnectionType::Standard;
#ifdef ENABLE_SECURE_WEBSOCKETS
                    if( m_serverTls.is_listening() ) m_serverTls.stop_listening();
#endif
                }
            });

        server.set_close_handler(
            [this]( const websocketpp::connection_hdl& hdl )
            {
                std::lock_guard lock( m_hdlMutex );
                if( m_hdl.lock() == hdl.lock() )
                {
                    printf("WebSocket%s: Client disconnected\n", TLS ? "TLS" : "");
                    m_hdl.reset();
                    m_alive = false;
                }
            });

#ifdef ENABLE_SECURE_WEBSOCKETS
        if constexpr (TLS) {
            server.set_tls_init_handler(
                []( const websocketpp::connection_hdl& hdl )
                {
                    static bool printOnce = true;
                    auto ctx = websocketpp::lib::make_shared<asio::ssl::context>( asio::ssl::context::tls_server );
                    ctx->set_options( asio::ssl::context::default_workarounds | asio::ssl::context::no_sslv2 |
                                      asio::ssl::context::no_sslv3 );
                    const char* certificateFile = GetEnvVar( "TRACY_SSL_CERT_FILE" );
                    if( certificateFile )
                    {
                        try
                        {
                            ctx->use_certificate_chain_file( certificateFile );
                            if (printOnce) printf( "SSL: Certificate chain file configured\n" );
                        }
                        catch( ... )
                        {
                            if (printOnce) printf( "SSL: Failed to load certificate chain\n" );
                        }
                    }
                    else
                    {
                        if (printOnce) printf( "SSL: No certificate chain file set (use env var TRACY_SSL_CERT_FILE to set)\n" );
                    }
                    const char* privateKeyFile = GetEnvVar( "TRACY_SSL_PRIVATE_KEY_FILE" );
                    if( privateKeyFile )
                    {
                        try
                        {
                            ctx->use_private_key_file( privateKeyFile, asio::ssl::context::pem );
                            if (printOnce) printf( "SSL: Private key configured\n" );
                        }
                        catch( ... )
                        {
                            if (printOnce) printf( "SSL: Failed to load private key\n" );
                        }
                    }
                    else
                    {
                        if (printOnce) printf( "SSL: No private key file set (use env var TRACY_SSL_PRIVATE_KEY_FILE to set)\n" );
                    }
                    printOnce = false;
                    return ctx;
                });
        }
#endif

        {
            std::lock_guard lock( m_initMutex );
            if( m_shutdown.load( std::memory_order_relaxed ) ) return;
            try
            {
                printf( "WebSocket%s: Server listening for connections on %s://0.0.0.0:%i...\n", TLS ? "TLS" : "", TLS ? "wss" : "ws", port );
                fflush( stdout );
                server.listen( port );
            } catch ( ... ) {
                printf ( "WebSocket%s: Failed to listen on port %i\n", TLS ? "TLS" : "", port );
                return;
            }
            try
            {
                server.start_accept();
            } catch ( ... ) {
                printf ( "WebSocket%s: Failed to start accept loop on port %i\n", TLS ? "TLS" : "", port );
                return;
            }
        }
        try {
            // Start web socket loop (blocking)
            server.run();
        } catch ( ... ) {
            printf ( "WebSocket%s: Encountered an error in the server loop\n", TLS ? "TLS" : "" );
            return;
        }
        printf ( "WebSocket%s: Server thread closing...\n", TLS ? "TLS" : "" );
    }

    void StopListening()
    {
        // Lock excludes the server thread's listen()/start_accept() init section.
        // If a server hasn't reached that section yet, m_shutdown (already set by the
        // destructor) will make it bail out without ever calling run().
        std::lock_guard lock( m_initMutex );
        if( m_server.is_listening() ) m_server.stop_listening();
#ifdef ENABLE_SECURE_WEBSOCKETS
        if( m_serverTls.is_listening() ) m_serverTls.stop_listening();
#endif
    }

    void Close() override
    {
        websocketpp::connection_hdl hdl;
        {
            std::lock_guard guard( m_hdlMutex );
            hdl = m_hdl;
            m_hdl.reset();
        }
        if( hdl.lock() )
        {
#ifdef ENABLE_SECURE_WEBSOCKETS
            if( IsTLS() )
                m_serverTls.close( hdl, websocketpp::close::status::going_away, "shutdown" );
            else
#endif
                m_server.close( hdl, websocketpp::close::status::going_away, "shutdown" );
        }
    }

    int Send( const void* buf, int len ) override
    {
        websocketpp::connection_hdl hdl;
        {
            std::lock_guard guard( m_hdlMutex );
            hdl = m_hdl;
        }
        if( hdl.expired() ) return -1;
        websocketpp::lib::error_code ec;
        try
        {
#ifdef ENABLE_SECURE_WEBSOCKETS
            if( IsTLS() )
            {
                m_serverTls.send( hdl, buf, len, websocketpp::frame::opcode::binary, ec );
                if( ec ) throw websocketpp::exception(ec);
                auto con = m_serverTls.get_con_from_hdl(hdl);
                // Flush outgoing queue, but yield instead of busy-spinning and
                // bail if the connection dies mid-flush.
                while( con->get_buffered_amount() > 0 )
                {
                    if( !m_alive ) break;
                    std::this_thread::sleep_for( std::chrono::milliseconds( 1 ) );
                }
            }
            else
#endif
            {
                m_server.send( hdl, buf, len, websocketpp::frame::opcode::binary, ec );
                if( ec ) throw websocketpp::exception(ec);
                auto con = m_server.get_con_from_hdl(hdl);
                while( con->get_buffered_amount() > 0 )
                {
                    if( !m_alive ) break;
                    std::this_thread::sleep_for( std::chrono::milliseconds( 1 ) );
                }
            }
        }
        catch ( ... )
        {
            Close();
            return -1;
        }
        return len;
    }

    int GetSendBufSize() override { return 8 * 1024; }

    int Recv( void* _buf, int len, int timeout ) override
    {
        // Copy new messages from server thread
        {
            std::lock_guard lock( m_messageQueueMutex );
            for( auto& message : m_messageQueueThread )
                m_messageQueue.emplace_back().data = std::move( message );
            m_messageQueueThread.clear();
        }

        char* buf = (char*)_buf;
        int bytesRead = 0;
        for( int i = 0; i < 2; i++ )
        {
            while( !m_messageQueue.empty() && bytesRead < len )
            {
                auto& packet = m_messageQueue.front();
                const auto bytesRemaining = (int)packet.data.size() - packet.bytesRead;
                const auto bytesToRead = std::min( bytesRemaining, len - bytesRead );
                std::copy_n( packet.data.data() + packet.bytesRead, bytesToRead, buf );
                bytesRead += bytesToRead;
                buf += bytesToRead;
                packet.bytesRead += bytesToRead;
                if( packet.bytesRead == (int)packet.data.size() )
                    m_messageQueue.pop_front();
            }
            if( i == 0 && bytesRead != len && timeout > 0 )
            {
                // Very simple (and imprecise) timeout logic emulation
                std::this_thread::sleep_for( std::chrono::milliseconds( timeout ) );

                // Copy new messages from server thread
                {
                    std::lock_guard lock( m_messageQueueMutex );
                    for( auto& message : m_messageQueueThread )
                        m_messageQueue.emplace_back().data = std::move( message );
                    m_messageQueueThread.clear();
                }
                continue;
            }
            break;
        }

        if( bytesRead > 0 ) return bytesRead;
        // No data read: distinguish "no data within timeout" from "connection closed",
        // matching Socket::Recv semantics. Without this, ReadImpl's `case 0` path
        // treats every timeout as EOF, which causes the caller to retry and silently
        // skip past bytes already drained from the WS queue.
        return m_alive ? -1 : 0;
    }

    int ReadUpTo( void* buf, int len ) override { return Recv(buf, len, 0); }

    bool HasData() override
    {
        if( !m_messageQueue.empty() ) return true;
        std::lock_guard lock( m_messageQueueMutex );
        return !m_messageQueueThread.empty();
    }

    bool IsValid() const override { return !m_hdl.expired() && m_alive; }
    bool IsTLS() { return m_connectionType == ConnectionType::TLS; }

private:
    // m_shutdown / m_initMutex synchronize WebSocket destruction with server startup.
    // Declared first so they are initialized before the server threads start.
    std::atomic<bool> m_shutdown { false };
    std::mutex m_initMutex;
    std::thread m_thread;
    server m_server;
#ifdef ENABLE_SECURE_WEBSOCKETS
    std::thread m_threadTls;
    server_tls m_serverTls;
#endif
    std::mutex m_hdlMutex;
    websocketpp::connection_hdl m_hdl;

    enum class ConnectionType
    {
        None,
        Standard,
        TLS,
    };

    std::atomic<ConnectionType> m_connectionType = ConnectionType::None;

    struct RecvPacket
    {
        std::string data;
        int bytesRead = 0;
    };

    std::mutex m_messageQueueMutex;
    std::deque<RecvPacket> m_messageQueue;
    std::vector<std::string> m_messageQueueThread;
};

bool ListenSocket::ListenWebSocket( uint16_t port, bool tls )
{
    if (m_webSocket) delete m_webSocket;
    m_webSocket = new WebSocket( port, tls );
    return true;
}

Socket* ListenSocket::AcceptWebSocket()
{
    // Single-shot by design: this returns the underlying WebSocket only while a
    // client is actually connected. The first accept binds the WebSocket to that
    // client; once it disconnects no further clients will be admitted (subsequent
    // attempts are dropped in the open handler). The owner must destroy and
    // recreate the ListenSocket to accept another connection.
    if( m_webSocket->IsValid() ) return m_webSocket;
    return nullptr;
}
#endif

void ListenSocket::Close()
{
    assert( m_sock != -1 );
#ifdef _WIN32
    closesocket( m_sock );
#else
    close( m_sock );
#endif
    m_sock = -1;
}

UdpBroadcast::UdpBroadcast()
    : m_sock( -1 )
{
#ifdef _WIN32
    InitWinSock();
#endif
}

UdpBroadcast::~UdpBroadcast()
{
    if( m_sock != -1 ) Close();
}

bool UdpBroadcast::Open( const char* addr, uint16_t port )
{
    assert( m_sock == -1 );

    struct addrinfo hints;
    struct addrinfo *res, *ptr;

    memset( &hints, 0, sizeof( hints ) );
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_DGRAM;

    char portbuf[32];
    sprintf( portbuf, "%" PRIu16, port );

    if( getaddrinfo( addr, portbuf, &hints, &res ) != 0 ) return false;
    int sock = 0;
    for( ptr = res; ptr; ptr = ptr->ai_next )
    {
        if( ( sock = socket( ptr->ai_family, ptr->ai_socktype, ptr->ai_protocol ) ) == -1 ) continue;
#if defined __APPLE__
        int val = 1;
        setsockopt( sock, SOL_SOCKET, SO_NOSIGPIPE, &val, sizeof( val ) );
#endif
#if defined _WIN32
        unsigned long broadcast = 1;
        if( setsockopt( sock, SOL_SOCKET, SO_BROADCAST, (const char*)&broadcast, sizeof( broadcast ) ) == -1 )
#else
        int broadcast = 1;
        if( setsockopt( sock, SOL_SOCKET, SO_BROADCAST, &broadcast, sizeof( broadcast ) ) == -1 )
#endif
        {
#ifdef _WIN32
            closesocket( sock );
#else
            close( sock );
#endif
            continue;
        }
        break;
    }
    freeaddrinfo( res );
    if( !ptr ) return false;

    m_sock = sock;
    inet_pton( AF_INET, addr, &m_addr );
    return true;
}

void UdpBroadcast::Close()
{
    assert( m_sock != -1 );
#ifdef _WIN32
    closesocket( m_sock );
#else
    close( m_sock );
#endif
    m_sock = -1;
}

int UdpBroadcast::Send( uint16_t port, const void* data, int len )
{
    assert( m_sock != -1 );
    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons( port );
    addr.sin_addr.s_addr = m_addr;
    return sendto( m_sock, (const char*)data, len, MSG_NOSIGNAL, (sockaddr*)&addr, sizeof( addr ) );
}

IpAddress::IpAddress()
    : m_number( 0 )
{
    *m_text = '\0';
}

IpAddress::~IpAddress()
{
}

void IpAddress::Set( const struct sockaddr& addr )
{
#if defined _WIN32 && ( !defined NTDDI_WIN10 || NTDDI_VERSION < NTDDI_WIN10 )
    struct sockaddr_in tmp;
    memcpy( &tmp, &addr, sizeof( tmp ) );
    auto ai = &tmp;
#else
    auto ai = (const struct sockaddr_in*)&addr;
#endif
    inet_ntop( AF_INET, &ai->sin_addr, m_text, 17 );
    m_number = ai->sin_addr.s_addr;
}

UdpListen::UdpListen()
    : m_sock( -1 )
{
#ifdef _WIN32
    InitWinSock();
#endif
}

UdpListen::~UdpListen()
{
    if( m_sock != -1 ) Close();
}

bool UdpListen::Listen( uint16_t port )
{
    assert( m_sock == -1 );

    int sock;
    if( ( sock = socket( AF_INET, SOCK_DGRAM, 0 ) ) == -1 ) return false;

#if defined __APPLE__
    int val = 1;
    setsockopt( sock, SOL_SOCKET, SO_NOSIGPIPE, &val, sizeof( val ) );
#endif
#if defined _WIN32
    unsigned long reuse = 1;
    setsockopt( sock, SOL_SOCKET, SO_REUSEADDR, (const char*)&reuse, sizeof( reuse ) );
#else
    int reuse = 1;
    setsockopt( sock, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof( reuse ) );
#endif
#if defined _WIN32
    unsigned long broadcast = 1;
    if( setsockopt( sock, SOL_SOCKET, SO_BROADCAST, (const char*)&broadcast, sizeof( broadcast ) ) == -1 )
#else
    int broadcast = 1;
    if( setsockopt( sock, SOL_SOCKET, SO_BROADCAST, &broadcast, sizeof( broadcast ) ) == -1 )
#endif
    {
#ifdef _WIN32
        closesocket( sock );
#else
        close( sock );
#endif
        return false;
    }

    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons( port );
    addr.sin_addr.s_addr = INADDR_ANY;

    if( bind( sock, (sockaddr*)&addr, sizeof( addr ) ) == -1 )
    {
#ifdef _WIN32
        closesocket( sock );
#else
        close( sock );
#endif
        return false;
    }

    m_sock = sock;
    return true;
}

void UdpListen::Close()
{
    assert( m_sock != -1 );
#ifdef _WIN32
    closesocket( m_sock );
#else
    close( m_sock );
#endif
    m_sock = -1;
}

const char* UdpListen::Read( size_t& len, IpAddress& addr, int timeout )
{
    static char buf[2048];

    struct pollfd fd;
    fd.fd = (socket_t)m_sock;
    fd.events = POLLIN;
    if( poll( &fd, 1, timeout ) <= 0 ) return nullptr;

    sockaddr sa;
    socklen_t salen = sizeof( struct sockaddr );
    len = (size_t)recvfrom( m_sock, buf, 2048, 0, &sa, &salen );
    addr.Set( sa );

    return buf;
}

}
