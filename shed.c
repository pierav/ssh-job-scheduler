#include <argp.h>
#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <libssh/callbacks.h>
#include <libssh/libssh.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/********************************************************************
 * Configuration
 ********************************************************************/
char *hosts[] = {"calc11", "calc12", "calc14", "calc15", "calc16", "calc17",
                 "calc18", "calc19", "calc20", "calc21", "calc22", "calc23",
                 "calc24", "calc25", "calc26", "calc27", "calc28"};
#define NB_HOSTS_MAX 17
#define NB_HOSTS NB_HOSTS_MAX

#define MYSSH_CHANNEL_PER_SESSION 8

/********************************************************************
 * Arguments
 ********************************************************************/

const char *argp_program_version = "shed v0.0.0";
const char *argp_program_bug_address =
    "<pierre.ravenel@univ-grenoble-alpes.fr>";
static char doc[] = "Minimal jobs scheduler";
static char args_doc[] = "args_doc";

/* The options we understand. */
static struct argp_option options[] = {
    {"verbose", 'v', 0, 0, "Produce verbose output"},
    {"jobs", 'j', "FILE", 0, "Job files. One line per cmd."},
    {0}};

struct arguments {
    int verbose;
    char *jobsfile;
};

/* Parse a single option. */
static error_t parse_opt(int key, char *arg, struct argp_state *state) {
    struct arguments *arguments = state->input;
    switch (key) {
        case 'v':
            arguments->verbose = 1;
            break;
        case 'j':
            arguments->jobsfile = arg;
            break;
        default:
            return ARGP_ERR_UNKNOWN;
    }
    return 0;
}

static struct argp argp = {options, parse_opt, args_doc, doc};

struct arguments *parse_args(int argc, char **argv) {
    static struct arguments arguments = {.verbose = 0, .jobsfile = NULL};
    argp_parse(&argp, argc, argv, 0, 0, &arguments);
    return &arguments;
}

/********************************************************************
 * Defines
 ********************************************************************/

#define MIN(a, b) ((a) < (b) ? (a) : (b))

#define handle_error_en(en, msg) \
    do {                         \
        errno = en;              \
        perror(msg);             \
        exit(EXIT_FAILURE);      \
    } while (0)

#define handle_error(msg)                             \
    do {                                              \
        perror(msg);                                  \
        fprintf(stderr, "*** PANIC ***: " #msg "\n"); \
        exit(EXIT_FAILURE);                           \
    } while (0)

#define handle_assert(cond, msg) \
    do {                         \
        if (!cond) {             \
            handle_error(msg);   \
        }                        \
    } while (0)

/********************************************************************
 * Jobs
 ********************************************************************/

static pthread_mutex_t jobs_mutex = PTHREAD_MUTEX_INITIALIZER;
char **jobs_list;
size_t jobs_list_size = 0;
size_t job_list_read = 0;
bool job_list_debug_mode = false;
char *job_list_debug_job = "sleep 10";

void jobs_init(char *filename) {
    char *line = NULL;
    size_t len = 0;
    ssize_t read;
    FILE *fp = fopen(filename, "r");
    handle_assert(fp, "fopen");

    // Read number of lines
    jobs_list_size = 0;
    while ((read = getline(&line, &len, fp)) != -1) {
        jobs_list_size++;
    }
    printf("Got %ld jobs\n", jobs_list_size);

    // Allocate jobs buffer
    jobs_list = calloc(jobs_list_size, sizeof(char *));
    handle_assert(jobs_list, "calloc");

    // Read again
    rewind(fp);
    size_t i = 0;
    while ((read = getline(&line, &len, fp)) != -1) {
        jobs_list[i] = strdup(line);
        for (char *c = jobs_list[i]; *c != '\0'; c++) {
            if (*c == '\n') {
                *c = '\0';
                break;
            }
        }
        i++;
    }

    // Close file
    if (line) {
        free(line);
    }
    fclose(fp);

    // Dump
    for (size_t j = 0; j < jobs_list_size; j++) {
        printf("Job %ld : %s\n", j, jobs_list[j]);
    }
}

int jobs_get(char **job) {
    int job_index = -1;
    pthread_mutex_lock(&jobs_mutex);
    if (job_list_read != jobs_list_size) {
        // get job index
        job_index = job_list_read;
        // increment next job
        job_list_read += 1;
    }
    pthread_mutex_unlock(&jobs_mutex);
    if (job_index != -1) {
        if (job_list_debug_mode) {
            *job = job_list_debug_job;
        } else {
            *job = jobs_list[job_index];
        }
    }
    return job_index;
}

/********************************************************************
 * Threads
 ********************************************************************/

typedef enum thread_state { IDLE, RUN, DONE } thread_state_t;

typedef struct thread_info { /* Used as argument to thread_start() */
    pthread_t thread_id;     /* ID returned by pthread_create() */
    int thread_num;          /* Application-defined thread # */
    /* Bool is running */
    thread_state_t state;

    /* Current job */
    char *job;
    int job_index;
    time_t job_start_time;

    /* Remote */
    char *hostname;
    int hosttid;
    ssh_session session;
    bool session_owner;
    ssh_channel channel;

    /* Elapsed time measurement */
    time_t start_time;
    time_t end_time;
} thread_info_t;

size_t num_threads = 0;
struct thread_info *tinfo;

void close_all_sessions() {
    int rc;

    // Kill all channels
    for (size_t tnum = 0; tnum < num_threads; tnum++) {
        ssh_channel channel = tinfo[tnum].channel;
        if (channel && ssh_channel_is_open(channel)) {
            printf("kill channel [%ld]\n", tnum);

            // Bypass API
            char KILL = 0x03;
            int nwritten = ssh_channel_write(channel, &KILL, 1);
            if (nwritten != 1) handle_error_en(1, "ssh_channel_write");

            // if(ssh_channel_request_send_signal(channel, "KILL") != SSH_OK){
            //     printf("Cannot kill [%ld] !\n", tnum);
            // }
            // rc = ssh_channel_request_send_break(channel, 0);
            // if (rc != SSH_OK) handle_error_en(1,
            // "ssh_channel_request_send_break");

            rc = ssh_channel_close(channel);
            if (rc != SSH_OK) handle_error_en(1, "ssh_channel_close");
            ssh_channel_free(channel);
            tinfo[tnum].channel = NULL;
        }
    }

    // Close all sessions
    for (size_t tnum = 0; tnum < num_threads; tnum++) {
        if (tinfo[tnum].session_owner) {
            ssh_disconnect(tinfo[tnum].session);
            ssh_free(tinfo[tnum].session);
        }
        tinfo[tnum].session = NULL;
    }
}
#include <signal.h>

void INThandler(int sig) {
    char c;
    signal(sig, SIG_IGN);
    printf(
        "OUCH, did you hit Ctrl-C?\n"
        "Do you really want to quit? [y/n] ");
    c = getchar();
    if (c == 'y' || c == 'Y') {
        printf("Close all sessions!\n");
        close_all_sessions();
        exit(0);
    } else {
        signal(SIGINT, INThandler);
    }
    getchar();  // Get new line character
}

int verify_knownhost(ssh_session session) {
    enum ssh_known_hosts_e state;
    unsigned char *hash = NULL;
    ssh_key srv_pubkey = NULL;
    size_t hlen;
    int rc;

    rc = ssh_get_server_publickey(session, &srv_pubkey);
    if (rc < 0) {
        return -1;
    }

    rc = ssh_get_publickey_hash(srv_pubkey, SSH_PUBLICKEY_HASH_SHA1, &hash,
                                &hlen);
    ssh_key_free(srv_pubkey);
    if (rc < 0) {
        return -1;
    }

    ssh_clean_pubkey_hash(&hash);

    state = ssh_session_is_known_server(session);
    if (state != SSH_KNOWN_HOSTS_OK) {
        return -1;
    }

    return 0;
}

void my_ssh_channel_exit_status_callback(ssh_session session,
                                         ssh_channel channel, int exit_status,
                                         void *userdata) {
    printf("Session exit %d\n", exit_status);
}

void my_ssh_channel_exit_signal_callback(ssh_session session,
                                         ssh_channel channel,
                                         const char *signal, int core,
                                         const char *errmsg, const char *lang,
                                         void *userdata) {
    printf("Session exit_signal %s\n", signal);
}

struct ssh_channel_callbacks_struct ccbs = {
    .channel_exit_status_function = my_ssh_channel_exit_status_callback,
    .channel_exit_signal_function = my_ssh_channel_exit_signal_callback};

ssh_session open_session(char *hostname) {
    int rc;
    ssh_session my_ssh_session = ssh_new();
    if (my_ssh_session == NULL) handle_error("ssh_new");

    int verbosity = SSH_LOG_NOLOG;  // SSH_LOG_PROTOCOL;
    int port = 22;

    ssh_options_set(my_ssh_session, SSH_OPTIONS_HOST, hostname);
    ssh_options_set(my_ssh_session, SSH_OPTIONS_LOG_VERBOSITY, &verbosity);
    ssh_options_set(my_ssh_session, SSH_OPTIONS_PORT, &port);

    // Connect
    rc = ssh_connect(my_ssh_session);
    if (rc != SSH_OK) {
        fprintf(stderr, "Error connecting : %s\n",
                ssh_get_error(my_ssh_session));
        ssh_free(my_ssh_session);
        return NULL;
    }

    // Verify the server's identity
    if (verify_knownhost(my_ssh_session) < 0) {
        fprintf(stderr, "Server identification failed: %s\n",
                ssh_get_error(my_ssh_session));
        ssh_disconnect(my_ssh_session);
        ssh_free(my_ssh_session);
        return NULL;
    }

    // Authenticate ourselves
    rc = ssh_userauth_publickey_auto(my_ssh_session, NULL, 0);
    if (rc == SSH_AUTH_ERROR) {
        fprintf(stderr, "Authentication failed: %s\n",
                ssh_get_error(my_ssh_session));
        ssh_disconnect(my_ssh_session);
        ssh_free(my_ssh_session);
        return NULL;
    }

    return my_ssh_session;
}

#define CGREEN "\e[32m"
#define CEND "\e[0m"

/** Returns the number of core by host */
int host_probe(char *host) {
    int count = 0;
    int rc;
    ssh_session session = open_session(host);
    if (session) {
        ssh_channel channel = ssh_channel_new(session);
        if (channel) {
            char buf[256];
            rc = ssh_channel_open_session(channel);
            if (rc != SSH_OK) handle_error_en(rc, "ssh_channel_open_session");
            rc = ssh_channel_request_exec(channel, "nproc");
            if (rc != SSH_OK) handle_error_en(rc, "ssh_channel_request_exec");
            int nbytes;
            do {
                nbytes = ssh_channel_read(channel, buf, sizeof(buf), 0);
            } while (nbytes > 0);
            ssh_channel_send_eof(channel);
            ssh_channel_close(channel);
            ssh_channel_free(channel);
            ssh_disconnect(session);
            ssh_free(session);
            count = atoi(buf);
        }
    }
    printf("%s : %d\n", host, count);
    return count / 2;
}

bool context_tick(thread_info_t *info, size_t *number_jobs_done) {
    bool finished = false;
    int rc;
    switch (info->state) {
        case IDLE: {  // Send job
            char *job;
            int job_index;
            if ((job_index = jobs_get(&job)) == -1) {
                info->state = DONE;
                info->end_time = time(NULL);
                finished = true;
            } else {
                info->job = job;
                info->job_index = job_index;
                info->channel = ssh_channel_new(info->session);
                assert(info->channel);

                // ssh_callbacks_init(&ccbs);
                // ssh_set_channel_callbacks (info->channel, &ccbs);

                rc = ssh_channel_open_session(info->channel);
                if (rc != SSH_OK)
                    handle_error_en(rc, "ssh_channel_open_session");
                rc = ssh_channel_request_pty(info->channel);
                if (rc != SSH_OK)
                    handle_error_en(rc, "ssh_channel_request_pty");
                rc = ssh_channel_request_shell(info->channel);
                if (rc != SSH_OK)
                    handle_error_en(rc, "ssh_channel_request_shell");

                char *exit_cmd = "; exit";
                int nbytes = strlen(job) + strlen(exit_cmd) + 1;
                char buffcmd[4096];
                assert(nbytes < sizeof(buffcmd));
                snprintf(buffcmd, sizeof(buffcmd), "%s%s\n", job, exit_cmd);
                printf(CGREEN "[%03d/%03ld]@%s:%02d" CEND " $ %s\n",
                       info->job_index, jobs_list_size, info->hostname,
                       info->hosttid, buffcmd);

                int nwritten =
                    ssh_channel_write(info->channel, buffcmd, nbytes);
                if (nwritten != nbytes)
                    handle_error_en(rc, "ssh_channel_write");

                // rc = ssh_channel_request_exec(info->channel, job);
                // if (rc != SSH_OK) handle_error_en(rc,
                // "ssh_channel_request_exec");
                info->state = RUN;
                info->job_start_time = time(NULL);
            }
            break;
        }
        case RUN: {  // poll job
            int nbytes;
            char channel_buffer[1024];
            nbytes = ssh_channel_read_nonblocking(info->channel, channel_buffer,
                                                  sizeof(channel_buffer), 0);
            if (nbytes == SSH_ERROR)
                handle_error_en(1, "ssh_channel_read_nonblocking");
            if (nbytes > 0) {
                // printf("%d : ", nbytes);
                // fwrite(channel_buffer, 1, nbytes, stdout);
                // for(int i = 0; i < nbytes; i++){
                //     // printf("-%x\n",channel_buffer[i]);
                // }
                // fflush(stdout);
                // Print ...
                // if (fwrite(channel_buffer, 1, nbytes, stdout) != nbytes) {
                //     assert(!"???");
                // }
            }
            if (ssh_channel_is_eof(info->channel)) {
                double dt = (double)time(NULL) - info->job_start_time;
                *number_jobs_done += 1;
                printf(CGREEN "[%03d/%03ld]@%s:%02d" CEND
                              " Success DT=%.2f | %s\n",
                       info->job_index, jobs_list_size, info->hostname,
                       info->hosttid, dt, info->job);
                printf(CGREEN "Jobs done : %ld/%ld" CEND "\n",
                       *number_jobs_done, jobs_list_size);

                ssh_channel_send_eof(info->channel);  // ?
                ssh_channel_close(info->channel);
                ssh_channel_free(info->channel);
                info->channel = NULL;
                info->state = IDLE;
            }
        } break;
        case DONE:  // Nothing to do
            break;
    }
    return finished;
}

int main(int argc, char **argv) {
    /* Arguments*/
    struct arguments *args = parse_args(argc, argv);

    /* Initialise SSH */
    printf("Initialise sshlib...\n");
    ssh_init();

    /* Parse jobs */
    if (args->jobsfile) {
        printf("jobs files is %s\n", args->jobsfile);
        jobs_init(args->jobsfile);
    } else {
        printf("No jobs, switch to test mode\n");
    }

    /* Probe hosts */
    printf("*** Probe hosts ...\n");
    size_t hosts_cores[NB_HOSTS] = {0};
    size_t total_cores = 0;
    for (size_t i = 0; i < NB_HOSTS; i++) {
        hosts_cores[i] = host_probe(hosts[i]);
        total_cores += hosts_cores[i];
    }

    if (!args->jobsfile) {  // Switch to demo mode
        jobs_list_size = total_cores * 2;
        job_list_debug_mode = 1;
    }

    num_threads = MIN(total_cores, jobs_list_size);
    printf("*** #cores=%ld #jobs=%ld -> #=%ld\n", total_cores, jobs_list_size,
           num_threads);

    /* Allocate contexts */
    tinfo = calloc(num_threads, sizeof(struct thread_info));
    if (tinfo == NULL) handle_error("calloc");

    /* Create one thread for each core */
    printf("Create sessions...\n");
    size_t tnum = 0;
    for (size_t i = 0; i < NB_HOSTS; i++) {
        for (size_t j = 0; j < hosts_cores[i]; j++) {
            if (tnum == num_threads) break;
            printf(".");
            fflush(stdout);
            // Setup ssh connection. Must not fault
            char *host = hosts[i];
            bool is_owner = j % MYSSH_CHANNEL_PER_SESSION == 0;
            ssh_session session;
            if (is_owner) {
                session = open_session(host);
                if (session == NULL) handle_error_en(1, "open_session");
                ssh_options_set(session, SSH_OPTIONS_SSH2, NULL);
            } else {
                session = tinfo[tnum - (j % MYSSH_CHANNEL_PER_SESSION)].session;
            }
            tinfo[tnum].hostname = host;
            tinfo[tnum].hosttid = j;
            tinfo[tnum].thread_num = tnum;
            tinfo[tnum].session = session;
            tinfo[tnum].session_owner = is_owner;
            tinfo[tnum].start_time = time(NULL);
            tnum++;
        }
    }

    /* Setup clean exit */
    signal(SIGINT, INThandler);

    // ssh_callbacks_init(&ccbs);
    // ssh_set_channel_callbacks (channel, &ccbs);

    time_t start_time = time(NULL);
    time_t end_time;
    double eff_time, total_time;

    size_t number_context_done = 0;
    size_t number_jobs_done = 0;
    while (number_context_done != num_threads) {
        for (size_t i = 0; i < num_threads; i++) {
            thread_info_t *info = &tinfo[i];
            // printf("thread %d is in state %d\n", i, info->state);
            number_context_done += context_tick(info, &number_jobs_done);
        }
        usleep(1000);  // 1 ms delay
    }

    close_all_sessions();

    // Compute times
    end_time = time(NULL);
    eff_time = (double)end_time - start_time;
    for (tnum = 0; tnum < num_threads; tnum++) {
        total_time += (double)tinfo[tnum].end_time - tinfo[tnum].start_time;
    }

    printf("user %.2fs system 0.00s %d%% cpu %.2fs total\n", total_time,
           (int)(total_time / eff_time * 100), eff_time);

    ssh_finalize();
    free(tinfo);
    exit(EXIT_SUCCESS);
}

