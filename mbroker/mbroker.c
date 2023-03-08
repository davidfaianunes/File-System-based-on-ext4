#include "../fs/config.h"
#include "../fs/operations.h"
#include "../fs/state.h"
#include "../producer-consumer/producer-consumer.h"
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
#define MAX_NUM_BOXES 16
#define FIFO_NAME_MAX_SIZE 256
#define BOX_NAME_MAX_SIZE 32
#define MESSAGE_MAX_SIZE 1024
#define UINT64_SIZE 20
#define PATH_FIFOS 18 // "../fifos/register/"

#define MAIN_REQUEST_SIZE 1 + FIFO_NAME_MAX_SIZE + BOX_NAME_MAX_SIZE + 1
#define LISTING_RESPONSE_SIZE                                                  \
    1 + 1 + BOX_NAME_MAX_SIZE + UINT64_SIZE + UINT64_SIZE + UINT64_SIZE + 1
#define BOX_RESPONSE_SIZE 1 + 1 + MESSAGE_MAX_SIZE + 1
#define PUBLISHING_REQUEST_SIZE 1 + MESSAGE_MAX_SIZE + 1

typedef struct sub_node sub_node;
struct sub_node {
    char *sub;
    sub_node *next;
};

typedef struct {
    sub_node *subs_head;
    char *pub;
    int fhandle;
    char *name;
} box_t;

struct box_node {
    box_t box;
    struct box_node *next;
};
typedef struct box_node box_node;

// linked list implementation for subscribers

void sub_append(sub_node *head, char *sub) {
    sub_node *new_node = (sub_node *)malloc(sizeof(sub_node));
    new_node->sub = sub;
    new_node->next = NULL;
    if (head == NULL) {
        head = new_node;
        return;
    }
    sub_node *current = head;
    while (current->next != NULL) {
        current = current->next;
    }
    current->next = new_node;
}

void sub_delete(sub_node *head, char *sub) {
    sub_node *current = head;
    sub_node *previous = NULL;
    if (current != NULL && strcmp(current->sub, sub) == 0) {
        head = current->next;
        free(current);
        return;
    }
    while (current != NULL && strcmp(current->sub, sub) != 0) {
        previous = current;
        current = current->next;
    }
    if (current == NULL)
        return;
    previous->next = current->next;
    free(current);
}

// linked list implementation for boxes

int box_is_in_list(box_node *head, char *box_name) {
    box_node *current = head;
    while (current != NULL) {
        if (!strcmp(current->box.name, box_name)) {
            return 1;
        }
        current = current->next;
    }
    return 0;
}

// valid box for setting a pub
int valid_box(box_node *head, char *box_name) {
    box_node *current = head;
    while (current != NULL) {
        if (!strcmp(current->box.name, box_name) && current->box.pub == NULL) {
            return 1;
        }
        current = current->next;
    }
    return 0;
}
// main functions for requests:

int insert_sub(char *sub, char *box_name, box_node *boxes_head) {
    box_node *current = boxes_head;
    while (current != NULL) {
        if (!strcmp(current->box.name, box_name)) {
            sub_append(current->box.subs_head, sub);
            return 0;
        }
        current = current->next;
    }
    return -1;
}

int remove_sub(char *sub, char *box_name, box_node *boxes_head) {
    box_node *current = boxes_head;
    while (current != NULL) {
        if (!strcmp(current->box.name, box_name)) {
            sub_node *subs_current = current->box.subs_head;
            sub_node *subs_previous = NULL;
            if (subs_current != NULL && !strcmp(subs_current->sub, sub)) {
                current->box.subs_head = subs_current->next;
                free(subs_current);
                return 0;
            }
            while (subs_current != NULL &&
                   strcmp(subs_current->sub, sub) != 0) {
                subs_previous = subs_current;
                subs_current = subs_current->next;
            }
            if (subs_current == NULL)
                return -1;
            subs_previous->next = subs_current->next;
            free(subs_current);
            return 0;
        }
        current = current->next;
    }
    return -1;
}

//
//
//
//

void add_box(box_t box, box_node *boxes_head) {
    if (boxes_head == NULL) {
        boxes_head = (box_node *)malloc(sizeof(box_node));
        boxes_head->box = box;
        boxes_head->next = NULL;
    } else {
        box_node *current = boxes_head;
        while (current->next != NULL) {
            current = current->next;
        }
        current->next = (box_node *)malloc(sizeof(box_node));
        current->next->box = box;
        current->next->next = NULL;
    }
}

char remove_box(char *box_name, box_node **boxes_head) {
    if (boxes_head == NULL) {
        return '0';
    }
    if (strcmp((*boxes_head)->box.name, box_name) == 0) {
        box_node *temp = *boxes_head;
        *boxes_head = (*boxes_head)->next;
        free(temp);
        return '1';
    } else {
        box_node *current = *boxes_head;
        while (current->next != NULL &&
               strcmp(current->next->box.name, box_name) != 0) {
            current = current->next;
        }
        if (current->next == NULL) {
            return '0';
        }
        box_node *temp = current->next;
        current->next = current->next->next;
        free(temp);
        return '1';
    }
}

int set_pub(char *box_name, char *pub, box_node *boxes_head) {
    box_node *current = boxes_head;
    while (current != NULL) {
        if (!strcmp(current->box.name, box_name)) {
            if (current->box.pub != NULL)
                free(current->box.pub);
            current->box.pub = (char *)malloc(strlen(pub) + 1);
            strcpy(current->box.pub, pub);
            return 0;
        }
        current = current->next;
    }
    // If box not found
    return -1;
}

int remove_pub(char *box_name, box_node *boxes_head) {
    box_node *current = boxes_head;
    while (current != NULL) {
        if (!strcmp(current->box.name, box_name)) {
            current->box.pub = NULL;
            return 0;
        }
        current = current->next;
    }
    // If box not found
    return -1;
}

box_t get_box_from_pub(char *pub, box_node *head) {
    box_node *current = head;
    while (current != NULL) {
        if (current->box.pub != NULL && strcmp(current->box.pub, pub) == 0) {
            return current->box;
        }
        current = current->next;
    }
    return (box_t){NULL, NULL, -1, NULL};
}

void list_boxes(box_node *head) {
    if (head == NULL) {
        printf("The list is empty\n");
    }
    box_node *current = head;
    while (current != NULL) {
        printf("Name: %s\n", current->box.name);
        current = current->next;
    }
}

void list_subs_from_box(box_node *boxes_head, char *box_name) {
    box_node *current = boxes_head;
    while (current != NULL) {
        if (!strcmp(current->box.name, box_name)) {
            sub_node *subs_current = current->box.subs_head;
            if (subs_current == NULL) {
                printf("There are no subs in the box %s", box_name);
                return;
            }
            while (subs_current != NULL) {
                printf("Subscriber: %s\n", subs_current->sub);
                subs_current = subs_current->next;
            }
            return;
        }
        current = current->next;
    }
    printf("Box not found");
}
// parse
// functions------------------------------------------------------------------

void extract_main_request(char *request, char *fifo_name, char *box_name) {
    int i = 0;

    int offset = 1;
    while (i < FIFO_NAME_MAX_SIZE && request[i + offset] != '\0') {
        fifo_name[i] = request[i + offset];
        i++;
    }
    while (i < FIFO_NAME_MAX_SIZE) {
        fifo_name[i] = '\0';
        i++;
    }

    i = 0;
    offset = 1 + FIFO_NAME_MAX_SIZE;
    while (i < BOX_NAME_MAX_SIZE && request[i + offset] != '\0') {
        box_name[i] = request[i + offset];
        i++;
    }
    while (i < BOX_NAME_MAX_SIZE) {
        box_name[i] = '\0';
        i++;
    }
}

void extract_listing_request(char *request, char *fifo_name) {
    int i = 0;

    int offset = 1;
    while (i < FIFO_NAME_MAX_SIZE && request[i + offset] != '\0') {
        fifo_name[i] = request[i + offset];
        i++;
    }
    while (i < FIFO_NAME_MAX_SIZE) {
        fifo_name[i] = '\0';
        i++;
    }
}

void build_box_response(char *response, char return_code, char *error_message) {
    response[0] = '4';
    response[1] = return_code;
    int i = 0;

    int offset = 2;
    while (i < MESSAGE_MAX_SIZE && error_message[i] != '\0') {
        response[i + offset] = error_message[i];
        i++;
    }
    while (i < MESSAGE_MAX_SIZE) {
        response[i + offset] = '\0';
        i++;
    }

    response[BOX_RESPONSE_SIZE - 1] = '\0';
}

void build_listing_response(char *response, char last, char *box_name,
                            uint64_t box_size, uint64_t n_publishers,
                            uint64_t n_subscribers) {
    response[0] = '8';
    response[1] = last;
    char temp_uint_string[UINT64_SIZE + 1];
    int i = 0;

    int offset = 2;
    while (i < BOX_NAME_MAX_SIZE && box_name[i] != '\0') {
        response[i + offset] = box_name[i];
        i++;
    }
    while (i < BOX_NAME_MAX_SIZE) {
        response[i + offset] = '\0';
        i++;
    }
    i = 0;

    snprintf(temp_uint_string, UINT64_SIZE + 1, "%020lu", box_size);
    offset = 2 + BOX_NAME_MAX_SIZE;
    while (i < UINT64_SIZE && temp_uint_string[i] != '\0') {
        response[offset + i] = temp_uint_string[i];
        i++;
    }
    while (i < UINT64_SIZE) {
        response[i + offset] = '\0';
        i++;
    }

    snprintf(temp_uint_string, UINT64_SIZE + 1, "%020lu", n_publishers);
    offset = 2 + BOX_NAME_MAX_SIZE + UINT64_SIZE;
    while (i < UINT64_SIZE && temp_uint_string[i] != '\0') {
        response[offset + i] = temp_uint_string[i];
        i++;
    }
    while (i < UINT64_SIZE) {
        response[i + offset] = '\0';
        i++;
    }

    snprintf(temp_uint_string, UINT64_SIZE + 1, "%020lu", n_subscribers);
    offset = 2 + BOX_NAME_MAX_SIZE + UINT64_SIZE * 2;
    while (i < UINT64_SIZE && temp_uint_string[i] != '\0') {
        response[offset + i] = temp_uint_string[i];
        i++;
    }
    while (i < UINT64_SIZE) {
        response[i + offset] = '\0';
        i++;
    }

    response[LISTING_RESPONSE_SIZE - 1] = '\0';
}

void extract_publishing_request(char *request, char *message, char *pub) {
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
    offset = 1 + MESSAGE_MAX_SIZE;
    while (i < FIFO_NAME_MAX_SIZE && request[i + offset] != '\0') {
        pub[i] = request[i + offset];
        i++;
    }
    while (i < FIFO_NAME_MAX_SIZE) {
        pub[i] = '\0';
        i++;
    }
}

void build_message(char *request, char *message) {
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

    request[PUBLISHING_REQUEST_SIZE - 1] = '\0';
}

// end of parse
// function------------------------------------------------------------

// function that closes every file before ending the process

void close_all_fhandlers(box_node *boxes_head) {
    if (boxes_head == NULL) {
        return;
    }
    box_node *current = boxes_head;
    while (current != NULL) {
        tfs_close(current->box.fhandle);
        current = current->next;
    }
}

// global variables

box_node *boxes_head = NULL;

int num_boxes = 0;

char fifo_path[PATH_FIFOS + FIFO_NAME_MAX_SIZE + 1];
char register_fifo_path[PATH_FIFOS + FIFO_NAME_MAX_SIZE + 1];
int max_sessions;

// signal handler

void handler() {
    close_all_fhandlers(boxes_head);
    unlink(register_fifo_path);
    exit(0);
}

// worker threads function:

void *process_requests(void *arg) {
    pc_queue_t *queue = (pc_queue_t *)arg;
    while (1) { // server must deal with requests indefinetely

        int fd;
        char *request = (char *)pcq_dequeue(queue);
        if (request[0] == '1') {
            // pedido de registro do publisher
            int request_status = 1;

            char fifo_name[FIFO_NAME_MAX_SIZE + 1];
            char box_name[BOX_NAME_MAX_SIZE + 1];

            extract_main_request(request, fifo_name, box_name);
            sprintf(fifo_path, "../fifos/%s", fifo_name);

            if (valid_box(boxes_head, box_name) && max_sessions != 0) {
                // linkedlist Operation
                set_pub(box_name, fifo_name, boxes_head);
                max_sessions--;
            } else {
                request_status = 0;
            }

            // send request_status
            fd = open(fifo_path, O_WRONLY);
            if (fd == -1) {
                fprintf(stdout, "ERROR %s\n", "couldn't open client_fifo");
                unlink(register_fifo_path);
                raise(SIGINT);
            }
            if (write(fd, &request_status, sizeof(request_status)) == -1) {
                fprintf(stdout, "ERROR %s\n",
                        "couldn't write request_status onto client_fifo");
                unlink(register_fifo_path);
                raise(SIGINT);
            }
            close(fd);

            if (request_status == 1) {
                sprintf(fifo_path, "../fifos/%s", fifo_name);
                while (1) {
                    char message_request[PUBLISHING_REQUEST_SIZE];
                    char message_for_sub[1 + MESSAGE_MAX_SIZE + 1];
                    fd = open(fifo_path, O_RDONLY);
                    if (fd == -1) {
                        fprintf(stdout, "ERROR %s\n",
                                "couldn't read message from pub");
                        unlink(register_fifo_path);

                        raise(SIGINT);
                    }
                    if (read(fd, message_request, sizeof(message_request)) ==
                        -1) {
                        fprintf(
                            stdout, "ERROR %s\n",
                            "couldn't write request_status onto client_fifo");
                        unlink(register_fifo_path);
                        raise(SIGINT);
                    }
                    close(fd);
                    char message[MESSAGE_MAX_SIZE + 1];
                    char pub[FIFO_NAME_MAX_SIZE + 1];
                    extract_publishing_request(message_request, message, pub);
                    printf("message_request: %s\n", message_request); // FIXME
                    box_t box = get_box_from_pub(fifo_name, boxes_head);
                    build_message(message_for_sub, message);

                    // loop through all subs and send
                    sub_node *subs_current = box.subs_head->next;
                    char sub_path[PATH_FIFOS + FIFO_NAME_MAX_SIZE + 1];
                    while (subs_current != NULL) {
                        sprintf(sub_path, "../fifos/%s", subs_current->sub);

                        if ((fd = open(sub_path, O_WRONLY)) == -1) {
                            fprintf(stdout, "ERROR %s\n",
                                    "couldn't open sub_fifo");
                            unlink(register_fifo_path);
                            raise(SIGINT);
                        }
                        if (fd == -1) {
                            fprintf(stdout, "ERROR %s\n",
                                    "couldn't open sub_fifo");
                            unlink(register_fifo_path);
                            raise(SIGINT);
                        }
                        if (write(fd, message_for_sub,
                                  sizeof(message_for_sub)) == -1) {
                            fprintf(stdout, "ERROR %s\n",
                                    "couldn't write listing_message onto "
                                    "client_fifo");
                            unlink(register_fifo_path);
                            raise(SIGINT);
                        }
                        close(fd);

                        subs_current = subs_current->next;
                    }
                }
            }
        }

        else if (request[0] == '2') {
            // pedido de registo de subscriber
            int request_status = 1;

            char fifo_name[FIFO_NAME_MAX_SIZE + 1];
            char box_name[BOX_NAME_MAX_SIZE + 1];

            extract_main_request(request, fifo_name, box_name);
            sprintf(fifo_path, "../fifos/%s", fifo_name);

            if (box_is_in_list(boxes_head, box_name) && max_sessions != 0) {
                // linkedlist Operation
                insert_sub(fifo_name, box_name, boxes_head);
                max_sessions--;
            } else {
                request_status = 0;
            }
            // send request_status
            fd = open(fifo_path, O_WRONLY);
            if (fd == -1) {
                fprintf(stdout, "ERROR %s\n", "couldn't open client_fifo");
                unlink(register_fifo_path);
                raise(SIGINT);
            }
            if (write(fd, &request_status, sizeof(request_status)) == -1) {
                fprintf(stdout, "ERROR %s\n",
                        "couldn't write request_status onto client_fifo");
                unlink(register_fifo_path);
                raise(SIGINT);
            }
            close(fd);
        }

        else if (request[0] == '3') {
            // pedido de criação de caixa
            char request_response[BOX_RESPONSE_SIZE];

            char fifo_name[FIFO_NAME_MAX_SIZE + 1];
            char box_name[BOX_NAME_MAX_SIZE + 1];
            char request_status;
            char error_message[MESSAGE_MAX_SIZE + 1] = "";

            extract_main_request(request, fifo_name, box_name);

            sprintf(fifo_path, "../fifos/%s", fifo_name);
            // send request_response
            if (num_boxes < MAX_NUM_BOXES &&
                !box_is_in_list(boxes_head, box_name)) {

                int fhandle;
                char tfs_file_path[MAX_FILE_NAME];
                sprintf(tfs_file_path, "/%s", box_name);
                if ((fhandle = tfs_open(tfs_file_path, TFS_O_CREAT)) == -1) {
                    fprintf(stdout, "ERROR %s\n",
                            "couldn't open file from tfs");
                    unlink(register_fifo_path);
                    raise(SIGINT);
                }

                box_t *new_box = (box_t *)malloc(sizeof(box_t));
                new_box->subs_head = (sub_node *)malloc(sizeof(sub_node));
                new_box->subs_head->sub = NULL;
                new_box->subs_head->next = NULL;
                new_box->fhandle = fhandle;
                new_box->pub = NULL;
                new_box->name = (char *)malloc(FIFO_NAME_MAX_SIZE + 1);
                strcpy(new_box->name, box_name);
                box_node *new_box_node = (box_node *)malloc(sizeof(box_node));
                new_box_node->box = *new_box;
                new_box_node->next = boxes_head;
                boxes_head = new_box_node;

                num_boxes++;
                request_status = '1';
            } else {
                strcpy(error_message, "Too many boxes.");
                request_status = '0';
            }

            build_box_response(request_response, request_status, error_message);

            fd = open(fifo_path, O_WRONLY);
            if (fd == -1) {
                fprintf(stdout, "ERROR %s\n", "couldn't open client_fifo");
                unlink(register_fifo_path);
                raise(SIGINT);
            }
            if (write(fd, request_response, sizeof(request_response)) == -1) {
                fprintf(stdout, "ERROR %s\n",
                        "couldn't write request_response onto client_fifo");
                unlink(register_fifo_path);
                raise(SIGINT);
            }
            close(fd);
        }

        else if (request[0] == '5') {
            // pedido de remoção de caixa
            char request_response[BOX_RESPONSE_SIZE];

            char fifo_name[FIFO_NAME_MAX_SIZE + 1];
            char box_name[BOX_NAME_MAX_SIZE + 1];
            char error_message[MESSAGE_MAX_SIZE + 1] = "";

            extract_main_request(request, fifo_name, box_name);

            sprintf(fifo_path, "../fifos/%s", fifo_name);
            char request_status = remove_box(box_name, &boxes_head);
            // send request_response
            if (request_status == '1') {
                num_boxes--;
            } else {
                strcpy(error_message, "Couldn't remove box.");
            }
            build_box_response(request_response, request_status, error_message);

            fd = open(fifo_path, O_WRONLY);
            if (fd == -1) {
                fprintf(stdout, "ERROR %s\n", "couldn't open client_fifo");
                unlink(register_fifo_path);
                raise(SIGINT);
            }
            if (write(fd, request_response, sizeof(request_response)) == -1) {
                fprintf(stdout, "ERROR %s\n",
                        "couldn't write request_response register_fifo");
                unlink(register_fifo_path);
                raise(SIGINT);
            }
            close(fd);
        }

        else if (request[0] == '7') {
            // pedido de listagem de caixas

            // extract the client_named_pipe
            char fifo_name[FIFO_NAME_MAX_SIZE + 1];
            extract_listing_request(request, fifo_name);

            sprintf(fifo_path, "../fifos/%s", fifo_name);
            // actual listing of boxes

            if (boxes_head == NULL) {
                char listing_message[LISTING_RESPONSE_SIZE];
                build_listing_response(listing_message, '0', "", 0, 0, 0);

                // send listing_message
                fd = open(fifo_path, O_WRONLY);
                if (fd == -1) {
                    fprintf(stdout, "ERROR %s\n", "couldn't open client_fifo");
                    unlink(register_fifo_path);
                    raise(SIGINT);
                }
                if (write(fd, listing_message, sizeof(listing_message)) == -1) {
                    fprintf(stdout, "ERROR %s\n",
                            "couldn't write listing_message onto client_fifo");
                    unlink(register_fifo_path);
                    raise(SIGINT);
                }
                close(fd);
            } else {
                box_node *current = boxes_head;
                int n_subs = 0;
                while (current != NULL) {
                    n_subs = 0;
                    sub_node *subs_current = current->box.subs_head;
                    while (subs_current != NULL) {
                        n_subs++;
                        subs_current = subs_current->next;
                    }

                    char listing_message[LISTING_RESPONSE_SIZE];
                    char last;
                    uint64_t n_publishers;
                    if (current->next == NULL) {
                        last = '1';
                    } else {
                        last = '0';
                    }
                    if (current->box.pub == NULL) {
                        n_publishers = 0;
                    } else {
                        n_publishers = 1;
                    }
                    build_listing_response(
                        listing_message, last, current->box.name,
                        sizeof(current->box), n_publishers, (uint64_t)n_subs);
                    fd = open(fifo_path, O_WRONLY);
                    if (fd == -1) {
                        fprintf(stdout, "ERROR %s\n",
                                "couldn't open client_fifo");
                        unlink(register_fifo_path);
                        raise(SIGINT);
                    }
                    if (write(fd, listing_message, sizeof(listing_message)) ==
                        -1) {
                        fprintf(
                            stdout, "ERROR %s\n",
                            "couldn't write listing_message onto client_fifo");
                        unlink(register_fifo_path);
                        raise(SIGINT);
                    }
                    close(fd);

                    current = current->next;
                }
            }
        } else {
            unlink(register_fifo_path);
            fprintf(stdout, "ERROR %s\n", "Wrong request code:");
            fprintf(stdout, "   -%c-\n", request[0]);
            raise(SIGINT);
        }
    }
}

// mbroker <register_pipe_name> <max_sessions>

int main(int argc, char **argv) {

    signal(SIGINT, handler);

    if (tfs_init(NULL) == -1) {
        fprintf(stdout, "ERROR %s\n", "couldn't initialize TFS");
        return -1;
    }

    if (argc != 3) {
        fprintf(stderr, "usage: mbroker <register_pipe_name> <max_sessions>\n");
        return -1;
    }

    char *register_pipe_name = argv[1];
    max_sessions = atoi(argv[2]);

    // initialize worker threads and producer-consumer queue

    pc_queue_t queue;
    pcq_create(&queue, (u_int32_t)max_sessions *
                           (u_int32_t)(PUBLISHING_REQUEST_SIZE) * sizeof(char));
    pthread_t workers[max_sessions];
    for (int i = 0; i < max_sessions; i++) {
        // CONSUMER CODE
        pthread_create(&workers[i], NULL, process_requests, &queue);
    }

    //
    //
    //
    sprintf(register_fifo_path, "../fifos/register/%s", register_pipe_name);

    // initializes fifo
    if (mkfifo(register_fifo_path, 0777) != 0 && errno != EEXIST) {

        fprintf(stdout, "ERROR %s\n", "couldn't initialize register_fifo");
        return -1;
    }
    while (1) {
        // PRODUCER CODE
        char request[PUBLISHING_REQUEST_SIZE]; // publishing request size is the
                                               // biggest size it'll ever need
                                               // in the buffer

        int fd = open(register_fifo_path, O_RDONLY);
        if (fd == -1) {
            fprintf(stdout, "ERROR %s\n", "couldn't open register_fifo");
            unlink(register_fifo_path);
            raise(SIGINT);
        }
        if (read(fd, request, sizeof(request)) == -1) {
            fprintf(stdout, "ERROR %s\n", "couldn't read register_fifo");
            unlink(register_fifo_path);
            raise(SIGINT);
        }
        close(fd);
        if (strcmp(request, "")) {
            pcq_enqueue(&queue, request);
        }
    }

    for (int i = 0; i < max_sessions; i++) {
        pthread_join(workers[i], NULL);
    }

    pcq_destroy(&queue);
    tfs_destroy();

    return 0;
}
