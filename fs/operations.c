#include "operations.h"
#include "config.h"
#include "state.h"
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "betterassert.h"
pthread_mutex_t tfs_open_mutex;

tfs_params tfs_default_params() {
    tfs_params params = {
        .max_inode_count = 64,
        .max_block_count = 1024,
        .max_open_files_count = 16,
        .block_size = 1024,
    };
    return params;
}

int tfs_init(tfs_params const *params_ptr) {
    tfs_params params;
    if (params_ptr != NULL) {
        params = *params_ptr;
    } else {
        params = tfs_default_params();
    }

    if (state_init(params) != 0) {
        return -1;
    }

    pthread_mutex_init(&tfs_open_mutex, NULL);

    // create root inode
    int root = inode_create(T_DIRECTORY);
    if (root != ROOT_DIR_INUM) {
        return -1;
    }

    return 0;
}

int tfs_destroy() {
    if (state_destroy() != 0) {
        return -1;
    }
    pthread_mutex_destroy(&tfs_open_mutex);
    return 0;
}

static bool valid_pathname(char const *name) {
    return name != NULL && strlen(name) > 1 && name[0] == '/';
}

/**
 * Looks for a file.
 *
 * Note: as a simplification, only a plain directory space (root directory only)
 * is supported.
 *
 * Input:
 *   - name: absolute path name
 *   - root_inode: the root directory inode
 * Returns the inumber of the file, -1 if unsuccessful.
 */
static int tfs_lookup(char const *name, inode_t const *root_inode) {
    ALWAYS_ASSERT(root_inode == inode_get(ROOT_DIR_INUM),
                  "verify if inode is relative to root dir");

    if (!valid_pathname(name)) {
        return -1;
    }

    // skip the initial '/' character
    name++;
    return find_in_dir(root_inode, name);
}

int tfs_open(char const *name, tfs_file_mode_t mode) {
    // Checks if the path name is valid
    if (!valid_pathname(name)) {
        return -1;
    }

    inode_t *root_dir_inode = inode_get(ROOT_DIR_INUM);
    ALWAYS_ASSERT(root_dir_inode != NULL,
                  "tfs_open: root dir inode must exist");
    int inum = tfs_lookup(name, root_dir_inode);
    size_t offset;

    if (inum >= 0) {
        pthread_mutex_lock(&tfs_open_mutex);
        // The file already exists
        inode_t *inode = inode_get(inum);
        ALWAYS_ASSERT(inode != NULL,
                      "tfs_open: directory files must have an inode");
        pthread_mutex_unlock(&tfs_open_mutex);
        if (inode->i_node_type == T_SYM) {
            if (inode->i_size == 0) {
                return -1;
            }
            void *block = data_block_get(inode->i_data_block);
            inum = tfs_lookup(block, inode_get(ROOT_DIR_INUM));
            if (inum == -1) {
                return -1;
            }
            inode = inode_get(inum);
            if (inode == NULL) {
                return -1;
            }
        }
        pthread_mutex_lock(&tfs_open_mutex);

        // Truncate (if requested)
        if (mode & TFS_O_TRUNC) {
            if (inode->i_size > 0) {
                data_block_free(inode->i_data_block);
                inode->i_size = 0;
            }
        }
        // Determine initial offset
        if (mode & TFS_O_APPEND) {
            offset = inode->i_size;
        } else {
            offset = 0;
        }
        pthread_mutex_unlock(&tfs_open_mutex);
    } else if (mode & TFS_O_CREAT) {
        pthread_mutex_lock(&tfs_open_mutex);
        // The file does not exist; the mode specified that it should be created
        // Create inode
        inum = inode_create(T_FILE);
        if (inum == -1) {
            pthread_mutex_unlock(&tfs_open_mutex);
            return -1; // no space in inode table
        }

        // Add entry in the root directory
        if (add_dir_entry(root_dir_inode, name + 1, inum) == -1) {
            inode_delete(inum);
            pthread_mutex_unlock(&tfs_open_mutex);
            return -1; // no space in directory
        }

        offset = 0;
        pthread_mutex_unlock(&tfs_open_mutex);
    } else {
        return -1;
    }

    // Finally, add entry to the open file table and return the corresponding
    // handle
    return add_to_open_file_table(inum, offset);

    // Note: for simplification, if file was created with TFS_O_CREAT and there
    // is an error adding an entry to the open file table, the file is not
    // opened but it remains created
}

int tfs_sym_link(char const *target_file, char const *sym_name) {
    if (!valid_pathname(target_file) || !valid_pathname(sym_name)) {
        return -1;
    }
    int fhandle = tfs_open(sym_name, TFS_O_CREAT);
    if (fhandle == -1) {
        return -1;
    }
    if (tfs_write(fhandle, target_file, strlen(target_file)) < 0) {
        tfs_close(fhandle);
        return -1;
    }
    int inumber = tfs_lookup(sym_name, inode_get(ROOT_DIR_INUM));
    if (inumber == -1) {
        return -1;
    }
    inode_get(inumber)->i_node_type = T_SYM;

    sym_name++; // convert from path_name to file_name

    if (add_dir_entry(inode_get(ROOT_DIR_INUM), sym_name, inumber) == -1) {
        inode_delete(inumber);
        return -1;
    } else {
        return 0;
    }
}

int tfs_link(char const *target_file, char const *hard_name) {
    if (!valid_pathname(target_file) || !valid_pathname(hard_name)) {
        return -1;
    }
    target_file++; // to convert from path to file_name

    int inumber = find_in_dir(inode_get(ROOT_DIR_INUM), target_file);

    if (inumber == -1) {
        return -1;
    }
    if (inode_get(inumber)->i_node_type == T_SYM) {
        return -1;
    }
    hard_name++; // to convert from path to file_name
    if (add_dir_entry(inode_get(ROOT_DIR_INUM), hard_name, inumber) == -1) {
        inode_delete(inumber);
        return -1;
    } else {
        inode_get(inumber)->num_of_links++;
        return 0;
    }
}

int tfs_close(int fhandle) {
    open_file_entry_t *file = get_open_file_entry(fhandle);
    if (file == NULL) {
        return -1; // invalid fd
    }

    pthread_mutex_lock(&tfs_open_mutex);
    remove_from_open_file_table(fhandle);
    pthread_mutex_unlock(&tfs_open_mutex);

    return 0;
}

ssize_t tfs_write(int fhandle, void const *buffer, size_t to_write) {

    open_file_entry_t *file = get_open_file_entry(fhandle);
    if (file == NULL) {
        return -1;
    }
    pthread_rwlock_wrlock(&file->lock);

    //  From the open file table entry, we get the inode
    inode_t *inode = inode_get(file->of_inumber);
    ALWAYS_ASSERT(inode != NULL, "tfs_write: inode of open file deleted");

    // Determine how many bytes to write
    size_t block_size = state_block_size();
    if (to_write + file->of_offset > block_size) {
        to_write = block_size - file->of_offset;
    }

    if (to_write > 0) {
        if (inode->i_size == 0) {
            // If empty file, allocate new block
            int bnum = data_block_alloc();
            if (bnum == -1) {

                pthread_rwlock_unlock(&file->lock);
                return -1; // no space
            }

            inode->i_data_block = bnum;
        }

        void *block = data_block_get(inode->i_data_block);
        ALWAYS_ASSERT(block != NULL, "tfs_write: data block deleted mid-write");

        // Perform the actual write
        memcpy(block + file->of_offset, buffer, to_write);

        // The offset associated with the file handle is incremented accordingly
        file->of_offset += to_write;

        if (file->of_offset > inode->i_size) {
            inode->i_size = file->of_offset;
        }
    }
    pthread_rwlock_unlock(&file->lock);
    return (ssize_t)to_write;
}

ssize_t tfs_read(int fhandle, void *buffer, size_t len) {
    open_file_entry_t *file = get_open_file_entry(fhandle);
    if (file == NULL) {
        return -1;
    }

    // From the open file table entry, we get the inode
    inode_t const *inode = inode_get(file->of_inumber);
    ALWAYS_ASSERT(inode != NULL, "tfs_read: inode of open file deleted");

    // Determine how many bytes to read
    size_t to_read = inode->i_size - file->of_offset;
    if (to_read > len) {
        to_read = len;
    }

    if (to_read > 0) {
        void *block = data_block_get(inode->i_data_block);
        ALWAYS_ASSERT(block != NULL, "tfs_read: data block deleted mid-read");
        // Perform the actual read
        memcpy(buffer, block + file->of_offset, to_read);
        // The offset associated with the file handle is incremented accordingly
        file->of_offset += to_read;
    }

    return (ssize_t)to_read;
}

int tfs_unlink(char const *target) {
    if (!valid_pathname(target)) {
        return -1;
    }
    int inumber = tfs_lookup(target, inode_get(ROOT_DIR_INUM));
    if (inumber == -1) {
        return -1;
    }
    inode_t *inode = inode_get(inumber);
    switch (inode->i_node_type) {
    case T_FILE:
        if ((--inode->num_of_links) == 0) {
            inode_delete(inumber);
        }
        break;
    case T_SYM:
        inode_delete(inumber);
        break;

    case T_DIRECTORY:
        break;

    default:
        break;
    }

    if (clear_dir_entry(inode_get(ROOT_DIR_INUM), ++target) == -1) {
        return -1;
    }

    return 0;
}

int tfs_copy_from_external_fs(char const *source_path, char const *dest_path) {

    FILE *source = fopen(source_path, "r");
    if (source == NULL) {
        return -1;
    }

    int dest;

    if ((dest = tfs_open(dest_path, TFS_O_CREAT | TFS_O_TRUNC)) == -1) {
        return -1;
    }

    char buffer[state_block_size()];
    size_t read = fread(buffer, sizeof(char), sizeof(buffer), source);

    ssize_t write = tfs_write(dest, buffer, read * sizeof(char));
    if (write < 0) {
        return -1;
    }
    return 0;
}
