#include "../fs/operations.h"
#include <assert.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>

void *remove_file_create_new() {
    char *path = "/f1";

    tfs_params params = tfs_default_params();
    params.max_inode_count = 2;
    params.max_block_count = 2;
    assert(tfs_init(&params) != -1);

    // Create file
    int fd = tfs_open(path, TFS_O_CREAT);
    assert(fd != -1);

    const char write_contents[] = "Hello World!";

    // Write to file
    assert(tfs_write(fd, write_contents, sizeof(write_contents)));

    assert(tfs_close(fd) != -1);

    // Unlink file
    assert(tfs_unlink(path) != -1);

    // Create new file with the same name
    fd = tfs_open(path, TFS_O_CREAT);
    assert(fd != -1);

    // Check if new file is empty
    char read_contents[sizeof(write_contents)];
    assert(tfs_read(fd, read_contents, sizeof(read_contents)) == 0);
    return NULL;
}

int main() {
    pthread_t t1;
    pthread_t t2;
    pthread_t t3;
    pthread_t t4;
    pthread_create(&t1, NULL, remove_file_create_new, NULL);
    pthread_create(&t2, NULL, remove_file_create_new, NULL);
    pthread_create(&t3, NULL, remove_file_create_new, NULL);
    pthread_create(&t4, NULL, remove_file_create_new, NULL);
    remove_file_create_new(NULL);
    pthread_join(t1, NULL);
    pthread_join(t2, NULL);
    pthread_join(t3, NULL);
    pthread_join(t4, NULL);

    printf("Successful test.\n");
    return 0;
}