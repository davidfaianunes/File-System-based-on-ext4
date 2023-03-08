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
#define PATH_FIFOS 18 // "../fifos/register/"w

#define MAIN_REQUEST_SIZE 1 + FIFO_NAME_MAX_SIZE + BOX_NAME_MAX_SIZE + 1
#define LISTING_RESPONSE_SIZE                                                  \
    1 + 1 + BOX_NAME_MAX_SIZE + UINT64_SIZE + UINT64_SIZE + UINT64_SIZE + 1
#define BOX_RESPONSE_SIZE 1 + 1 + MESSAGE_MAX_SIZE + 1
#define PUBLISHING_REQUEST_SIZE 1 + MESSAGE_MAX_SIZE + 1

int min(int a, int b) { return (a < b) ? a : b; }

// parsing functions -----------------------------------------------
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

void extract_message(char *request, char *message) {
    int i = 0;

    int offset = 1;
    while (i < MESSAGE_MAX_SIZE && request[i + offset] != '\0') {
        message[i] = request[i + offset];
        i++;
    }
    while (i < MESSAGE_MAX_SIZE) {
        message[i] = '\0';
        i++;
    }
}

// end of parsing functions -----------------------------------------

// make subscriber's pipe_name and message counter global vars,
// to be accessible by signal handler

char pipe_name[FIFO_NAME_MAX_SIZE + 1];
int message_counter = 0;

// global variable for fifos paths
// to group them all in one folder

char fifo_path[PATH_FIFOS + FIFO_NAME_MAX_SIZE + 1];
char register_fifo_path[PATH_FIFOS + FIFO_NAME_MAX_SIZE + 1];

void handler() {
    unlink(fifo_path);
    printf("\nNumber of received messages: %d.\n", message_counter);
    exit(0);
}

// sub <register_pipe_name> <pipe_name> <box_name>

int main(int argc, char **argv) {

    signal(SIGINT, handler);

    if (argc != 4) {
        fprintf(stderr,
                "usage: sub <register_pipe_name> <pipe_name> <box_name>\n");
        return -1;
    }
    char *register_pipe_name = argv[1];
    strcpy(pipe_name, argv[2]);
    char *box_name = argv[3];

    sprintf(fifo_path, "../fifos/%s", pipe_name);
    sprintf(register_fifo_path, "../fifos/register/%s", register_pipe_name);

    // register fifo for this subscriber
    if (mkfifo(fifo_path, 0777) != 0 && errno != EEXIST) {
        return -1;
    }

    // mensagem de pedido de registo de subscriber (code = 1)
    char sub_register_request[MAIN_REQUEST_SIZE];

    build_main_request(sub_register_request, '2', pipe_name, box_name);

    sub_register_request[MAIN_REQUEST_SIZE - 1] = '\0';
    int fd = open(register_fifo_path, O_WRONLY);
    if (fd == -1) {
        printf("No such register_pipe_name\n");
        unlink(fifo_path);
        return -1;
    }
    if (write(fd, sub_register_request, sizeof(sub_register_request)) == -1) {
        unlink(fifo_path);
        return -1;
    }
    close(fd);

    // check if login was successful
    int successful_login;
    fd = open(fifo_path, O_RDONLY);
    if (fd == -1) {
        unlink(fifo_path);
        return -1;
    }

    if (read(fd, &successful_login, sizeof(successful_login)) == -1) {
        unlink(fifo_path);
        return -1;
    }
    close(fd);
    if (!successful_login) {
        // it means there's no box with the given name, or that that box already
        // has a publisher
        fprintf(stdout, "ERROR %s\n", "couldn't subscribe");
        unlink(fifo_path);
        return -1;
    }

    // if login was successful, print received messages from mbroker
    while (1) {
        // receive message from mbroker
        char mbroker_message[1 + MESSAGE_MAX_SIZE + 1];
        fd = open(fifo_path, O_RDONLY);
        if (fd == -1) {
            unlink(fifo_path);
            return -1;
        }

        if (read(fd, mbroker_message, sizeof(mbroker_message)) == -1) {
            unlink(fifo_path);
            return -1;
        }
        close(fd);
        // print message
        char message[MESSAGE_MAX_SIZE];

        extract_message(mbroker_message, message);

        printf("%s", message);

        message_counter++;
    }

    return -1;
}
