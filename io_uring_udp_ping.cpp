/*
 * io_uring ping client/server program to benchmark NAPI busy_poll patch.
 *
 * Author: Olivier Langlois <olivier@trillion01.com>
 * Date: 2022-02-22
 *
 * To compile: g++ io_uring_udp_ping.cpp -luring -o io_uring_udp_ping
 * (Need liburing version > 2.1 for io_uring_submit_and_wait_timeout().
 *  As of now, only the git master branch will do)
 *
 * NOTE:
 * - Address must not use the loopback interface to have a NAPI id assigned to the socket.
 * - To enable NAPI busy_poll: echo 2000000 > /proc/sys/net/core/busy_poll
 * - To disable NAPI busy_poll: echo 0 > /proc/sys/net/core/busy_poll
 * - To check NAPI busy_poll status: cat /proc/sys/net/core/busy_poll
 *
 * Scenarios to test:
 * 1. disabled NAPI busy poll, nosqpoll
 * 2. disabled NAPI busy poll, nosqpoll, with io_uring busy_loop (-b)
 * 3. NAPI busy poll enabled, nosqpoll, no io_uring busy poll
 * 4. disabled NAPI busy poll, with sqpoll, no io_uring busy poll (-s)
 * 5. disabled NAPI busy poll, with sqpoll, with io_uring busy poll (-sb)
 * 6. NAPI busy poll enabled, with sqpoll, no io_uring busy poll (-s)
 * 7. NAPI busy poll enabled, with sqpoll, with io_uring busy poll (-sb)
 */

#include <liburing.h>
#include <getopt.h>    // for getopt()
#include <stdio.h>
#include <stdlib.h>    // for abort(), atoi()
#include <unistd.h>    // for close()
#include <errno.h>
#include <string.h>    // for strerror()
#include <memory.h>    // for memset()
#include <arpa/inet.h> // for inet_pton()
#include <sys/socket.h>
#include <stdint.h>    // for uint32_t, uint64_t
#include <strings.h>   // for bzero()
#include <math.h>      // for fabs()
#include <iostream>
#include <iomanip>
#include <string>
#include <stdexcept>
#include <limits>
#include <vector>
#include <sched.h>

#define QD 256
#define DEFAULT_PING_NUM 100
#define BUFFER_SZ 128

namespace {

enum {
    IOURING_RECV,
    IOURING_SEND,
    IOURING_RECVMSG,
    IOURING_SENDMSG
};

const char *Io_Uring_OpTypeToStr(char type)
{
    const char *res;
    switch (type) {
        case IOURING_RECV:
            res = "IOURING_RECV";
            break;
        case IOURING_SEND:
            res = "IOURING_SEND";
            break;
        case IOURING_RECVMSG:
            res = "IOURING_RECVMSG";
            break;
        case IOURING_SENDMSG:
            res = "IOURING_SENDMSG";
            break;
        default:
            res = "Unknown";
    }
    return res;
}

/*
 * iouring_encode_user_data()
 */
inline void *
iouring_encode_user_data(char type, int fd)
{
    return (void *)((uint32_t)fd | ((__u64)type << 56));
}

/*
 * iouring_decode_user_data()
 */
inline void
iouring_decode_user_data(uint64_t data, char *type, int *fd)
{
    *type = data >> 56;
    *fd   = data & 0xffffffffU;
}

inline
double convertUsec(const struct timespec &ts)
{
    return 1000000.0*ts.tv_sec + ts.tv_nsec/1000.0;
}

struct timespec &operator-=(struct timespec &t1,
                            const struct timespec &t2) noexcept
{
    // borrow
    if (t2.tv_nsec > t1.tv_nsec) {
        if (t1.tv_sec) {
            --t1.tv_sec;
            t1.tv_nsec += 1000000000;
        }
        else {
            goto substractError;
        }
    }
    if (t2.tv_sec > t1.tv_sec) {
        goto substractError;
    }
    t1.tv_sec  -= t2.tv_sec;
    t1.tv_nsec -= t2.tv_nsec;
    return t1;
substractError:
    memset(&t1, 0, sizeof(t1));
    return t1;
}

inline struct timespec operator-(const struct timespec &t1,
                                 const struct timespec &t2) noexcept
{
    struct timespec t1Copy{t1};
    return t1Copy -= t2;
}

/*
 * class CmdLineProcessor
 */
class CmdLineProcessor
{
public:
    CmdLineProcessor(int argc, char *argv[]);

    bool getListen()   const { return m_listen; }
    bool getSqPoll()   const { return m_sqpoll; }
    bool getBusyLoop() const { return m_busyLoop; }
    int  getPingNum()  const { return m_pingNum; }
    struct sockaddr *getSA()
    { return reinterpret_cast<struct sockaddr *>(&m_addr); }

private:
    void printError(const char *msg, int opt);
    void printUsage(const char *progName);

    struct sockaddr_in m_addr;
    int  m_pingNum;
    bool m_listen;
    bool m_sqpoll;
    bool m_busyLoop;
};

#define printable(ch) (isprint((unsigned char) ch) ? ch : '#')

/*
 * printError()
 */
void CmdLineProcessor::printError(const char *msg, int opt)
{
    if (msg != nullptr && opt != 0)
        fprintf(stderr, "%s (-%c)\n", msg, printable(opt));
}

/*
 * printUsage()
 */
void CmdLineProcessor::printUsage(const char *progName)
{
    fprintf(stderr,
            "Usage: %s [-l|--listen] [-a|--address ip_address:udp_port] [-s|--sqpoll] [-b|--busy] [-p|--ping num][-h|--help]\n\
 --listen\n\
  -l        : Server mode\n\
 --address\n\
  -a        : remote or local ip_address:udp_port (depends on -l option presence)\n\
 --sqpoll\n\
  -s        : Configure io_uring to use SQPOLL thread\n\
 --busy\n\
  -b        : Busy poll io_uring instead of blocking.\n\
 --ping\n\
  -p        : Ping num\n\
 --help\n\
  -h        : Display this usage message\n\n",
            progName);
}

CmdLineProcessor::CmdLineProcessor(int argc, char *argv[])
: m_pingNum(DEFAULT_PING_NUM),
  m_listen(false),
  m_sqpoll(false),
  m_busyLoop(false)
{
    int opt;
    struct option longopts[7] = {
        {"listen"  , 0, nullptr, 'l'},
        {"address" , 1, nullptr, 'a'},
        {"help"    , 0, nullptr, 'h'},
        {"sqpoll"  , 0, nullptr, 's'},
        {"busy"    , 0, nullptr, 'b'},
        {"ping"    , 1, nullptr, 'p'},
        {nullptr   , 0, nullptr, 0 }
    };

    bzero(&m_addr, sizeof(m_addr));
    /*
     * First colon char in optstring parameter is to indicate to getopt() to not
     * print error messages.
     */
    std::string addrStr;
    while ((opt = getopt_long(argc, argv, ":lhsba:p:", longopts, nullptr)) != -1) {
        switch (opt) {
            case 'l':
                m_listen = true;
                break;
            case 's':
                m_sqpoll = true;
                break;
            case 'b':
                m_busyLoop = true;
                break;
            case 'h':
                printUsage(argv[0]);
                exit(0);
                break;
            case 'a': {
                char *ptr = strchr(optarg, ':');
                addrStr = optarg;
                if (!ptr) {
                    fprintf(stderr, "Incorrect address argument format: %s\n", optarg);
                    printUsage(argv[0]);
                    exit(-1);
                }
                *ptr = '\0';
                if (inet_pton(AF_INET, optarg, &m_addr.sin_addr) <= 0) {
                    fprintf(stderr, "inet_pton error for %s\n", optarg);
                    printUsage(argv[0]);
                    exit(-1);
                }
                m_addr.sin_port = htons(atoi(ptr+1));
                m_addr.sin_family = AF_INET;
            }
                break;
            case 'p':
                m_pingNum = atoi(optarg);
                break;
            case ':':
                printError("Missing argument", optopt);
                printUsage(argv[0]);
                exit(-1);
                break;
            case '?':
                printError("Unrecognized option", optopt);
                printUsage(argv[0]);
                exit(-1);
                break;
            default:
                fprintf(stderr, "Fatal: Unexpected case in CmdLineProcessor switch()\n");
                exit(-1);
                break;
        }
    }
    if (addrStr.empty()) {
        fprintf(stderr, "address option is mandatory\n");
        printUsage(argv[0]);
        exit(-1);
    }
    if (getListen()) {
        printf("Listening %s...\n", addrStr.c_str());
    } else {
        printf("Connecting to %s... to send %d pings\n", addrStr.c_str(), m_pingNum);
    }
}

/*
 * struct hostRTTStats
 *
 * NOTE: Values are in uSecs.
 */
struct hostRTTStats
{
    hostRTTStats();
    double minRTT;
    double maxRTT;
    double avgRTT;
    double RTTdev;
};

hostRTTStats::hostRTTStats()
: minRTT(std::numeric_limits<double>::max()),
  maxRTT(std::numeric_limits<double>::min()),
  avgRTT(0),
  RTTdev(0)
{
}

std::ostream &operator<<(std::ostream &o,
                         hostRTTStats &rhs)
{
    o << "rtt min/avg/max/mdev = ";
    o << std::fixed << std::showpoint << std::setprecision(3);
    o << rhs.minRTT << '/' << rhs.avgRTT << '/' << rhs.maxRTT << '/'
      << rhs.RTTdev << " us";
    return o;
}

/*
 * class UDPEndpoint
 */
class UDPEndpoint
{
public:
    UDPEndpoint(unsigned int pingNum);
    virtual ~UDPEndpoint();

    virtual void initiate(struct io_uring *ring) = 0;
    virtual void processCompletion(struct io_uring *ring, struct io_uring_cqe *cqe) = 0;
    
    bool done() const { return !m_pingNum; }

protected:
    int getFd() const { return m_sockfd; }
    bool NapiUnreported() const { return !m_napiReported; }
    void reportNapi() const;

    unsigned int m_pingNum;
    char m_buffer[BUFFER_SZ];

private:
    int m_sockfd;
    mutable bool m_napiReported;
};

UDPEndpoint::UDPEndpoint(unsigned int pingNum)
: m_pingNum(pingNum+1), // Add 1 ping to discard the iteration printing the napi id
  m_napiReported(false)
{
    if ((m_sockfd = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
        fprintf(stderr, "socket() failed: (%d) %s\n",
                errno, strerror(errno));
        throw std::runtime_error("UDPEndpoint::UDPEndpoint()");
    }
}

UDPEndpoint::~UDPEndpoint()
{
    close(m_sockfd);
}

/*
 * reportNapi()
 */
void UDPEndpoint::reportNapi() const
{
    unsigned napi_id{};
    socklen_t len{sizeof(napi_id)};
    getsockopt(getFd(), SOL_SOCKET, SO_INCOMING_NAPI_ID, &napi_id,
               &len);
    std::cout << "socket " << getFd();
    if (napi_id)
        std::cout << " napi id: " << napi_id;
    else
        std::cout << " unassigned napi id";
    std::cout << std::endl;
    m_napiReported = true;
}

/*
 * class PingClient
 *
 * The client can connect to the server, therefore it can use the simpler
 * system calls recv()/send()
 */
class PingClient : public UDPEndpoint
{
public:
    PingClient(struct sockaddr *serverAddr, unsigned int pingNum);
    virtual ~PingClient();

    virtual void initiate(struct io_uring *ring) override;
    virtual void processCompletion(struct io_uring *ring,
                                   struct io_uring_cqe *cqe) override;
    void printStats() const;

private:
    void sendPing(struct io_uring *ring);
    void receivePing(struct io_uring *ring);
    void recordRTT(struct io_uring *ring);

    typedef std::vector<double> RTTVec_t;
    RTTVec_t m_rttVec;
};

PingClient::PingClient(struct sockaddr *serverAddr, unsigned int pingNum)
: UDPEndpoint(pingNum)
{
    if (connect(getFd(), serverAddr, sizeof(struct sockaddr_in)) < 0) {
        fprintf(stderr, "connect() failed: (%d) %s\n",
                errno, strerror(errno));
        throw std::runtime_error("PingClient::PingClient()");
    }
    m_rttVec.reserve(pingNum);
}

/*
 * printStats()
 */
void PingClient::printStats() const
{
    std::cout << m_rttVec.size() << " pings sent\n";
    hostRTTStats stats;
    for (double curRTT : m_rttVec) {
        if (curRTT < stats.minRTT)
            stats.minRTT = curRTT;
        if (curRTT > stats.maxRTT)
            stats.maxRTT = curRTT;
        stats.avgRTT += curRTT;
    }
    stats.avgRTT /= m_rttVec.size();

    // Calculate stddev
    for (double curRTT : m_rttVec)
        stats.RTTdev += fabs(curRTT - stats.avgRTT);

    stats.RTTdev /= m_rttVec.size();
    std::cout << stats << '\n';
}

PingClient::~PingClient()
{
    printStats();
}

/*
 * sendPing()
 */
void PingClient::sendPing(struct io_uring *ring)
{
    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    clock_gettime(CLOCK_REALTIME, (struct timespec *)m_buffer);
    io_uring_prep_send(sqe, getFd(), m_buffer, sizeof(struct timespec), 0);
    io_uring_sqe_set_data(sqe, iouring_encode_user_data(IOURING_SEND, getFd()));
}

/*
 * receivePing()
 */
void PingClient::receivePing(struct io_uring *ring)
{
    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    io_uring_prep_recv(sqe, getFd(), m_buffer, BUFFER_SZ, 0);
    io_uring_sqe_set_data(sqe, iouring_encode_user_data(IOURING_RECV, getFd()));
}

/*
 * recordRTT()
 *
 * Called on recv() completion.
 */
void PingClient::recordRTT(struct io_uring *ring)
{
    struct timespec startTs = *(struct timespec *)m_buffer;
    // Send next ping
    sendPing(ring);
    m_rttVec.push_back(convertUsec(*(struct timespec *)m_buffer - startTs));
}

/*
 * initiate()
 */
void PingClient::initiate(struct io_uring *ring)
{
    sendPing(ring);
}

/*
 * processCompletion()
 */
void PingClient::processCompletion(struct io_uring *ring,
                                   struct io_uring_cqe *cqe)
{
    char type;
    int  fd;
    int res = cqe->res;

    iouring_decode_user_data(cqe->user_data, &type, &fd);

    if (res < 0) {
        fprintf(stderr, "unexpected %s failure: (%d) %s\n",
                Io_Uring_OpTypeToStr(type), -res, strerror(-res));
        abort();
    }

    switch (type) {
        case IOURING_SEND:
            receivePing(ring);
            break;
        case IOURING_RECV:
            if (res != sizeof(struct sockaddr_in)) {
                fprintf(stderr, "unexpected ping reply len: %d\n", res);
                abort();
            }
            if (NapiUnreported()) {
                reportNapi();
                sendPing(ring);
            } else
                recordRTT(ring);
                
            --m_pingNum;
            break;
        default:
            fprintf(stderr, "unexpected %s completion\n",
                    Io_Uring_OpTypeToStr(type));
            abort();
            break;
    }
}

/*
 * class PingServer
 *
 * The server has to remain unconnected to reply to every client.
 * It could use recvfrom()/sendto() system calls but since they aren't
 * supported by io_uring, we have to fallback to the more general recvmgs()/sendmsg().
 * IMHO, I don't like much using them because they force to fill a struct.
 * Fortunately, manipulating the ancillary data pointed by msg_control is not
 * needed.
 */
class PingServer : public UDPEndpoint
{
public:
    PingServer(struct sockaddr *serverAddr, unsigned int pingNum);

    virtual void initiate(struct io_uring *ring) override;
    virtual void processCompletion(struct io_uring *ring,
                                   struct io_uring_cqe *cqe) override;

private:
    void sendPing(struct io_uring *ring);
    void receivePing(struct io_uring *ring);

    struct msghdr      m_msghdr;
    struct sockaddr_in m_senderAddr;
    struct iovec       m_msg_iov;
};

PingServer::PingServer(struct sockaddr *serverAddr, unsigned int pingNum)
: UDPEndpoint(pingNum)
{
    if (bind(getFd(), serverAddr, sizeof(struct sockaddr_in)) < 0) {
        fprintf(stderr, "bind() failed: (%d) %s\n",
                errno, strerror(errno));
        throw std::runtime_error("PingServer::PingServer()");
    }
}

/*
 * sendPing()
 *
 * Resend the exact same data back to the sender so m_msghdr
 * does not need to be touched at all. The content returned by
 * recvmsg() is passed back to sendmsg() as-is.
 */
void PingServer::sendPing(struct io_uring *ring)
{
    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    io_uring_prep_sendmsg(sqe, getFd(), &m_msghdr, 0);
    io_uring_sqe_set_data(sqe,
                          iouring_encode_user_data(IOURING_SENDMSG, getFd()));
}

/*
 * receivePing()
 */
void PingServer::receivePing(struct io_uring *ring)
{
    bzero(&m_msghdr, sizeof(m_msghdr));
    m_msghdr.msg_name    = &m_senderAddr;
    m_msghdr.msg_namelen = sizeof(m_senderAddr);
    m_msg_iov.iov_base   = m_buffer;
    m_msg_iov.iov_len    = BUFFER_SZ;
    m_msghdr.msg_iov     = &m_msg_iov;
    m_msghdr.msg_iovlen  = 1;
    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    io_uring_prep_recvmsg(sqe, getFd(), &m_msghdr, 0);
    io_uring_sqe_set_data(sqe,
                          iouring_encode_user_data(IOURING_RECVMSG, getFd()));
}

/*
 * initiate()
 */
void PingServer::initiate(struct io_uring *ring)
{
    receivePing(ring);
}

/*
 * processCompletion()
 */
void PingServer::processCompletion(struct io_uring *ring,
                                   struct io_uring_cqe *cqe)
{
    char type;
    int  fd;
    int res = cqe->res;

    iouring_decode_user_data(cqe->user_data, &type, &fd);

    if (res < 0) {
        fprintf(stderr, "unexpected %s failure: (%d) %s\n",
                Io_Uring_OpTypeToStr(type), -res, strerror(-res));
        abort();
    }

    switch (type) {
        case IOURING_SENDMSG:
            receivePing(ring);
            --m_pingNum;
            break;
        case IOURING_RECVMSG:
            m_msg_iov.iov_len = res;
            sendPing(ring);
            if (NapiUnreported()) reportNapi();
            break;
        default:
            fprintf(stderr, "unexpected %s completion\n",
                    Io_Uring_OpTypeToStr(type));
            abort();
            break;
    }
}

bool bHalting{};

/*
 * _sig_die()
 */
void _sig_die(int signo) noexcept
{
    bHalting = true;
}

/*
 * sighandler_initialize()
 */
void sighandler_initialize() noexcept
{
    struct sigaction act;
    sigemptyset(&act.sa_mask);
    act.sa_handler = _sig_die;
    act.sa_flags = 0;
#ifdef SA_INTERRUPT
    act.sa_flags = SA_INTERRUPT;
#endif
    if (sigaction(SIGINT, &act, nullptr) < 0) {
        fprintf(stderr, "Failed to install the SIGINT handler");
    }
    if (sigaction(SIGTERM, &act, nullptr) < 0) {
        fprintf(stderr, "Failed to install the SIGTERM handler");
    }
}

/*
 * changeProcessScheduler()
 */
void changeProcessScheduler()
{
    struct sched_param param;
    param.sched_priority = sched_get_priority_max(SCHED_FIFO);
    if (sched_setscheduler(0, SCHED_FIFO, &param) < 0)
        fprintf(stderr, "sched_setscheduler() failed: (%d) %s\n",
                errno, strerror(errno));
}

}

/*
 * main()
 */
int main(int argc, char *argv[])
{
    int res = 0;
    UDPEndpoint *endPointPtr = nullptr;
    struct __kernel_timespec *tsPtr, ts;
    CmdLineProcessor options(argc, argv);
    struct io_uring_params params;
    struct io_uring ring;
    
    memset(&params, 0, sizeof(params));
    memset(&ts, 0, sizeof(ts));

    if (options.getSqPoll()) {
        params.flags = IORING_SETUP_SQPOLL;
        params.sq_thread_idle = 50;
    }

    if (io_uring_queue_init_params(QD, &ring, &params) < 0) {
        fprintf(stderr, "io_uring_queue_init_params() failed: (%d) %s\n",
                errno, strerror(errno));
        return -1;
    }

    if (options.getBusyLoop()) {
        tsPtr = &ts;
    } else
        tsPtr = nullptr;

    changeProcessScheduler();
    sighandler_initialize();
    try {
        if (options.getListen())
            endPointPtr = new PingServer(options.getSA(), options.getPingNum());
        else
            endPointPtr = new PingClient(options.getSA(), options.getPingNum());

        endPointPtr->initiate(&ring);

        while (!endPointPtr->done() && !bHalting) {
            do {
                struct io_uring_cqe *cqe_ptr;
                int res = io_uring_submit_and_wait_timeout(&ring, &cqe_ptr, 1,
                                                           tsPtr, nullptr);
//                if (res != 0) printf("res = %d\n", res);
            }
            while (res < 0 && errno == ETIME && !bHalting);
            unsigned completed = 0;
            unsigned head;
            struct io_uring_cqe *cqe;
            io_uring_for_each_cqe(&ring, head, cqe) {
                ++completed;
                endPointPtr->processCompletion(&ring, cqe);
            }
            if (completed) {
                io_uring_cq_advance(&ring, completed);
            }
        }
    }
    catch (std::exception &e) {
        fprintf(stderr, "Exception caught: %s\n", e.what());
        res = -1;
    }
    delete endPointPtr;
    io_uring_queue_exit(&ring);
    return res;
}
