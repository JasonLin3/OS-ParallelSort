#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <sys/sysinfo.h>
#include <sys/stat.h>
#include <sys/mman.h>

struct rec {
    int key[1];
    int value[24]; // 96 bytes
};

//debug print
    // for(int i = 0; i<num_records; i++) {
    //     for(int j = 0; j<4; j++) {
    //         printf("%d", records[i].key[j]);
    //     }
    //     for(int j= 4; j<100; j++) {
    //         printf("%d", records[i].value[j]);
    //     }
    //     printf("\n");
    // }

// get number of threads
int num_threads;
int num_records;
struct rec *records;
int curr_partition;

// int merge() {

// }

// int merge_sort(list, start, end) {
//     if(start < end) {
//         int mid = 1 + (end-1) /2;

//         mergeSort(arr, 1, m);
//         mergeSort(arr, m + 1, r);

//         merge();
//     }
// }

void* merge_caller(void* t) {
    // int curr_part = curr_partition++;
    int* thread_index = (int*) t;
    printf("HELLO FROM  %d\n", *thread_index);
    
}

int main(int argc, char** argv) {
    // parse input - use threading
    // FILE *fp;
    //char buffer[100];
    int filelen, fd, err;     
    struct stat statbuf;
    num_threads = get_nprocs();
    
    // get file descriptor and size
    fd = open(argv[1], O_RDWR); 
    err = stat(argv[1], &statbuf);
    if (err < 0) {
        printf("\n\"%s \" could not open\n", argv[1]);
        exit(2);
    }
    filelen = statbuf.st_size;
    num_records = filelen / 100;

    //converted to char pointer
    //made map private to avoid modifications from other thread.
    char *ptr = (char *) mmap(0, statbuf.st_size, PROT_READ, MAP_PRIVATE, fd, 0 );
    close(fd);

    //create an array of structs
    records = malloc(num_records * sizeof(struct rec));

    int j = 0; // index of record
    //copy key and value into struct
    for(int i = 0; i<statbuf.st_size; i = i + 100) {
        memcpy(records[j].key, ptr+i, 4);
        memcpy(records[j].value, ptr+i+4, 96);
        j++;
    }

    pthread_t threads[num_threads];
    printf("NUMTHREADS: %d\n", num_threads);
    int* indexes = malloc(num_threads * sizeof(int));
    // sort     
    for(int i = 0; i<num_threads; i++) {
        indexes[i] = i;
        pthread_create(&threads[i], NULL, merge_caller, indexes+i);
    }
    for(int i = 0; i<num_threads; i++) {
        pthread_join(threads[i], NULL);
    }

    free(indexes);
    
    // send to output
    
    
    
    return 0;
}






