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
#define UINT64_SIZE 20
#define MESSAGE_MAX_SIZE 1024
#define PATH_FIFOS 18 // "../fifos/register/"

#define MAIN_REQUEST_SIZE 1 + FIFO_NAME_MAX_SIZE + BOX_NAME_MAX_SIZE + 1
#define LISTING_RESPONSE_SIZE                                                  \
    1 + 1 + BOX_NAME_MAX_SIZE + UINT64_SIZE + UINT64_SIZE + UINT64_SIZE + 1
#define BOX_RESPONSE_SIZE 1 + 1 + MESSAGE_MAX_SIZE + 1
#define PUBLISHING_REQUEST_SIZE 1 + MESSAGE_MAX_SIZE + 1

static void print_usage() {
    fprintf(stderr,
            "usage: \n"
            "   manager <register_pipe_name> <pipe_name> create <box_name>\n"
            "   manager <register_pipe_name> <pipe_name> remove <box_name>\n"
            "   manager <register_pipe_name> <pipe_name> list\n");
}

int min(int a, int b) { return (a < b) ? a : b; }

char fifo_path[PATH_FIFOS + FIFO_NAME_MAX_SIZE + 1];
char register_fifo_path[PATH_FIFOS + FIFO_NAME_MAX_SIZE + 1];

// parsing functions---------------------------------------------------------

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

void build_listing_request(char *request, char code, char *fifo_name) {
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
    while (i < BOX_NAME_MAX_SIZE) {
        request[i + offset] = '\0';
        i++;
    }

    request[MAIN_REQUEST_SIZE - 1] = '\0';
}
void extract_box_response(char *response, char *return_code,
                          char *error_message) {
    *return_code = response[1];

    int i = 0;

    int offset = 2;
    while (i < MESSAGE_MAX_SIZE && response[i + offset] != '\0') {
        error_message[i] = response[i + offset];
        i++;
    }
    while (i < MESSAGE_MAX_SIZE) {
        error_message[i] = '\0';
        i++;
    }
}
void extract_listing_response(char *response, char *last, char *box_name,
                              uint64_t *box_size, uint64_t *n_publishers,
                              uint64_t *n_subscribers) {
    char temp_uint_string[UINT64_SIZE + 1];

    *last = response[1];

    int i = 0;
    int offset = 2;
    while (i < BOX_NAME_MAX_SIZE && response[i + offset] != '\0') {
        box_name[i] = response[i + offset];
        i++;
    }
    while (i < BOX_NAME_MAX_SIZE) {
        box_name[i] = '\0';
        i++;
    }

    i = 0;
    offset = 2 + BOX_NAME_MAX_SIZE;
    while (i < UINT64_SIZE && response[i + offset] != '\0') {
        temp_uint_string[i] = response[i + offset];
        i++;
    }
    while (i < UINT64_SIZE) {
        temp_uint_string[i] = '\0';
        i++;
    }
    *box_size = strtoull(temp_uint_string, NULL, 10);

    i = 0;
    offset = 2 + BOX_NAME_MAX_SIZE + UINT64_SIZE;
    while (i < UINT64_SIZE && response[i + offset] != '\0') {
        temp_uint_string[i] = response[i + offset];
        i++;
    }
    while (i < UINT64_SIZE) {
        temp_uint_string[i] = '\0';
        i++;
    }
    *n_publishers = strtoull(temp_uint_string, NULL, 10);

    i = 0;
    offset = 2 + BOX_NAME_MAX_SIZE + UINT64_SIZE + UINT64_SIZE;
    while (i < UINT64_SIZE && response[i + offset] != '\0') {
        temp_uint_string[i] = response[i + offset];
        i++;
    }
    while (i < UINT64_SIZE) {
        temp_uint_string[i] = '\0';
        i++;
    }
    *n_subscribers = strtoull(temp_uint_string, NULL, 10);
}

// end of parsing functions---------------------------------------------------

void handler() {
    unlink(fifo_path);
    exit(0);
}

//
//
//
//
//
// manager <register_pipe_name> <pipe_name> create <box_name>
// manager <register_pipe_name> <pipe_name> remove <box_name>
// manager <register_pipe_name> <pipe_name> list

int main(int argc, char **argv) {

    signal(SIGINT, handler);

    if (argc == 4 || argc == 5) {
        char *register_pipe_name = argv[1];
        char *pipe_name = argv[2];

        sprintf(fifo_path, "../fifos/%s", pipe_name);
        sprintf(register_fifo_path, "../fifos/register/%s", register_pipe_name);

        // register fifo for this subscriber
        if (mkfifo(fifo_path, 0777) != 0 && errno != EEXIST) {
            fprintf(stdout, "ERROR %s\n", "couldn't initialize client_fifo");
            unlink(fifo_path);
            return -1;
        }

        char request[1 + FIFO_NAME_MAX_SIZE + BOX_NAME_MAX_SIZE + 1];

        if (argc == 4) {
            if (!strcmp("list", argv[3])) {
                build_listing_request(request, '7', pipe_name);

            } else {
                print_usage();
                unlink(fifo_path);
                return -1; // invalid input
            }
        } else {
            int create_comm = !strcmp("create", argv[3]);
            int remove_comm = !strcmp("remove", argv[3]);

            if (!create_comm && !remove_comm) {
                print_usage();
                unlink(fifo_path);
                return -1; // invalid input

            } else {
                char *box_name = argv[4];
                if (create_comm) {
                    build_main_request(request, '3', pipe_name, box_name);
                } else { // remove_comm
                    build_main_request(request, '5', pipe_name, box_name);
                }
            }
        }
        int fd = open(register_fifo_path, O_WRONLY);
        if (fd == -1) {
            printf("No such register_pipe_name\n");
            unlink(fifo_path);
            return -1;
        }
        if (write(fd, request, sizeof(request)) == -1) {
            fprintf(stdout, "ERROR %s\n", "couldn't write onto register_fifo");
            unlink(fifo_path);
            return -1;
        }
        close(fd);
        if (request[0] == '7') { // listing boxes command
            int there_are_more_boxes = 1;
            while (there_are_more_boxes) {
                fd = open(fifo_path, O_RDONLY);

                char listing_response[LISTING_RESPONSE_SIZE];

                if (read(fd, listing_response, sizeof(listing_response)) ==
                    -1) {

                    fprintf(stdout, "ERROR %s\n",
                            "couldn't read from client_fifo");

                    unlink(fifo_path);
                    return -1;
                }
                close(fd);
                char last;
                char box_name[BOX_NAME_MAX_SIZE];
                uint64_t box_size;
                uint64_t n_publishers;
                uint64_t n_subscribers;
                extract_listing_response(listing_response, &last, box_name,
                                         &box_size, &n_publishers,
                                         &n_subscribers);

                if (!strcmp(box_name, "")) {
                    // there are no boxes in the mbroker
                    there_are_more_boxes = 0;

                    unlink(fifo_path);
                    return 0;
                } else if (last == '1') {
                    there_are_more_boxes = 0;
                    fprintf(stdout, "%s %zu %zu %zu\n", box_name, box_size,
                            n_publishers, n_subscribers);
                } else {
                    fprintf(stdout, "%s %zu %zu %zu\n", box_name, box_size,
                            n_publishers, n_subscribers);
                }
            }
        } else { // create or remove box command

            // read request_status and print accordingly

            fd = open(fifo_path, O_RDONLY);
            if (fd == -1) {
                printf("No such register_pipe_name\n");

                unlink(fifo_path);
                return -1;
            }
            char box_response[BOX_RESPONSE_SIZE];
            if (read(fd, box_response, sizeof(box_response)) == -1) {

                unlink(fifo_path);
                return -1;
            }
            close(fd);

            char return_code;
            char error_message[MESSAGE_MAX_SIZE];

            extract_box_response(box_response, &return_code, error_message);

            if (return_code == '1') {
                fprintf(stdout, "OK\n");
            } else {
                fprintf(stdout, "ERROR %s\n", "Couldn't create/remove box");
            }
        }
    } else {
        print_usage();
        unlink(fifo_path);
        return -1;
    }
    unlink(fifo_path);
    return 0;
}