#include "../fs/config.h"
#include "../fs/operations.h"
#include "../fs/state.h"
#include "../utils/logging.h"
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#define FIFO_NAME_MAX_SIZE 256
#define BOX_NAME_MAX_SIZE 32
#define MESSAGE_MAX_SIZE 1024
#define UINT64_SIZE 20
#define PATH_FIFOS 18 // "../fifos/register/"

#define MAIN_REQUEST_SIZE 1 + FIFO_NAME_MAX_SIZE + BOX_NAME_MAX_SIZE + 1
#define LISTING_RESPONSE_SIZE                                                  \
    1 + 1 + BOX_NAME_MAX_SIZE + UINT64_SIZE + UINT64_SIZE + UINT64_SIZE + 1
#define BOX_RESPONSE_SIZE 1 + 1 + MESSAGE_MAX_SIZE + 1
#define PUBLISHING_REQUEST_SIZE 1 + MESSAGE_MAX_SIZE + FIFO_NAME_MAX_SIZE + 1

int min(int a, int b) { return (a < b) ? a : b; }

// make publisher's pipe_name global, to be accessible by signal handler

char pipe_name[FIFO_NAME_MAX_SIZE + 1];

// parsing functions------------------------------------------------------
void build_main_request(char *request, char code, char *fifo_name,
                        char *box_name) {
    request[0] = code;
    int i = 0;

    int offset = 1;
    while (i < FIFO_NAME_MAX_SIZE && fifo_name[i] != '\0') {
        request[i + offset] = fifo_name[i];
        i++;
    }
    while (i < FIFO_NAME_MAX_SIZE) {
        request[i + offset] = '\0';
        i++;
    }
    i = 0;

    offset = 1 + FIFO_NAME_MAX_SIZE;
    while (i < BOX_NAME_MAX_SIZE && box_name[i] != '\0') {
        request[offset + i] = box_name[i];
        i++;
    }
    while (i < BOX_NAME_MAX_SIZE) {
        request[i + offset] = '\0';
        i++;
    }

    request[MAIN_REQUEST_SIZE - 1] = '\0';
}

void build_message(char *request, char *message, char *pub) {
    request[0] = '9';
    int i = 0;

    int offset = 1;
    while (i < MESSAGE_MAX_SIZE && message[i] != '\0') {
        request[i + offset] = message[i];
        i++;
    }
    while (i < MESSAGE_MAX_SIZE) {
        request[i + offset] = '\0';
        i++;
    }
    offset = 1 + MESSAGE_MAX_SIZE;
    while (i < FIFO_NAME_MAX_SIZE && message[i] != '\0') {
        request[i + offset] = pub[i];
        i++;
    }
    while (i < FIFO_NAME_MAX_SIZE) {
        request[i + offset] = '\0';
        i++;
    }

    request[PUBLISHING_REQUEST_SIZE - 1] = '\0';
}
// end of parsing functions------------------------------------------------

void read_message_from_user(char *buffer) {
    if (fgets(buffer, MESSAGE_MAX_SIZE, stdin) != NULL) {
        int len = (int)strlen(buffer);
        if (len > 0 && buffer[len - 1] != '\n') {
            int c;
            while ((c = getchar()) != '\n' && c != EOF)
                ;
        }
    } else {
        printf("ERROR: error reading line from pub\n");
    }
}

// global variable for fifos paths
// to group them all in one folder

char fifo_path[PATH_FIFOS + FIFO_NAME_MAX_SIZE + 1];
char register_fifo_path[PATH_FIFOS + FIFO_NAME_MAX_SIZE + 1];

void handler() {
    unlink(fifo_path);
    exit(0);
}

// pub <register_pipe_name> <pipe_name> <box_name>
int main(int argc, char **argv) {

    signal(SIGINT, handler);

    if (argc != 4) {
        fprintf(stderr,
                "usage: pub <register_pipe_name> <pipe_name> <box_name>\n");
        return -1;
    }
    char *register_pipe_name = argv[1];
    strcpy(pipe_name, argv[2]);
    char *box_name = argv[3];

    sprintf(fifo_path, "../fifos/%s", pipe_name);
    sprintf(register_fifo_path, "../fifos/register/%s", register_pipe_name);

    // register fifo for this publisher
    if (mkfifo(fifo_path, 0777) != 0 && errno != EEXIST) {
        return -1;
    }

    // mensagem de pedido de registo de publisher (code = 1)
    char pub_register_request[MAIN_REQUEST_SIZE];

    build_main_request(pub_register_request, '1', pipe_name, box_name);

    int fd = open(register_fifo_path, O_WRONLY);
    if (fd == -1) {
        printf("No such register_pipe_name\n");
        unlink(fifo_path);
        return -1;
    }

    if (write(fd, pub_register_request, sizeof(pub_register_request)) == -1) {
        fprintf(stdout, "ERROR %s\n", "couldn't write onto register_fifo");
        unlink(fifo_path);
        return -1;
    }
    close(fd);

    // check if login was successful
    int successful_login;
    fd = open(fifo_path, O_RDONLY);
    if (fd == -1) {
        fprintf(stdout, "ERROR %s\n", "couldn't open client_fifo");
        unlink(fifo_path);
        return -1;
    }

    if (read(fd, &successful_login, sizeof(successful_login)) == -1) {
        fprintf(stdout, "ERROR %s\n", "couldn't read from client_fifo");
        unlink(fifo_path);
        return -1;
    }
    close(fd);
    if (!successful_login) {
        fprintf(stdout, "ERROR %s\n",
                "there's no box with the given name, or that that box already "
                "has a publisher");
        unlink(fifo_path);
        return -1;
    }
    // if login was successful, read stdin and send messages to mbroker
    int end_publisher = 0;
    while (!end_publisher) {

        // read message
        char request_send_message[PUBLISHING_REQUEST_SIZE];
        char message[MESSAGE_MAX_SIZE];
        read_message_from_user(message);
        build_message(request_send_message, message, pipe_name);
        // send request_send_message to mbroker
        fd = open(fifo_path, O_WRONLY);
        if (fd == -1) {
            unlink(fifo_path);
            return -1;
        }
        if (write(fd, request_send_message, sizeof(request_send_message)) ==
            -1) {
            unlink(fifo_path);
            return -1;
        }
        close(fd);
    }
    unlink(fifo_path);
    return 0;
}
