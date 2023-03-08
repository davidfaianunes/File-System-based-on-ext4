#include "fs/operations.h"
#include <assert.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

const char *target_path = "/f1";
const char *sym_path = "/s1";
const char *hardlink_path = "/h1";
uint8_t const *file_contents = "teste 1 2 3";

void assert_contents_ok(char const *path) {
    int f = tfs_open(path, 0);
    assert(f != -1);

    uint8_t buffer[sizeof(file_contents)];
    assert(tfs_read(f, buffer, sizeof(buffer)) == sizeof(buffer));
    assert(memcmp(buffer, file_contents, sizeof(buffer)) == 0);

    assert(tfs_close(f) != -1);
}

void assert_empty_file(char const *path) {
    int f = tfs_open(path, 0);
    assert(f != -1);

    uint8_t buffer[sizeof(file_contents)];
    assert(tfs_read(f, buffer, sizeof(buffer)) == 0);

    assert(tfs_close(f) != -1);
}

void write_contents(char const *path) {
    int f = tfs_open(path, 0);
    assert(f != -1);

    assert(tfs_write(f, file_contents, sizeof(file_contents)) ==
           sizeof(file_contents));

    assert(tfs_close(f) != -1);
}

int main() {
    
    assert(tfs_init(NULL) != -1);

    {
        int f = tfs_open(target_path, TFS_O_CREAT);
        assert(f != -1);
        assert(tfs_close(f) != -1);

        assert_empty_file(target_path); //  sanity check
    }

    // Write to symlink and read original file
    assert(tfs_sym_link(target_path, sym_path) != -1);
    assert_empty_file(sym_path);

    write_contents(sym_path);
    assert_contents_ok(target_path);


    // Create a hardlink and checks if it has the same content
    assert(tfs_link(hardlink_path, sym_path) != -1);
    assert_contents_ok(hardlink_path);

    // Removes symlink
    assert(tfs_unlink(sym_path) != -1);

    // Link unusable - target no longer exists
    assert(tfs_open(hardlink_path) == -1);

    assert_contents_ok(target_path); //sanity check

    return 0;

}