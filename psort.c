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

int merge(int left, int mid, int right) {
    // get size of each half
    int size_left = mid - left + 1;
    int size_right = right-mid;

    // make temporary arrays
    struct rec* left_array = malloc(size_left * sizeof(struct rec));
    struct rec* right_array = malloc(size_right * sizeof(struct rec));
    for(int i = 0; i<size_left; i++) {
        left_array[i] = records[left+i];
    }
    for(int i = 0; i<size_right; i++) {
        right_array[i] = records[mid+i+1];
    }

    // merge
    int l = 0;
    int r = 0;
    int i = left;
    while(l<size_left && r< size_right) {
        if(left_array[l].key[0] < right_array[r].key[0]) {
            records[i] = left_array[l];
            l++;
        } else {
            records[i] = right_array[r];
            r++;
        }
        i++;
    }
    // add the rest
    while(l<size_left) {
        records[i] = left_array[l];
        l++;
        i++;
    }
    while(r<size_right) {
        records[i] = right_array[r];
        r++;
        i++;
    }
    free(left_array);
    free(right_array);
    return 0;
}

int merge_sort(int left, int right) {
    if(left<right) {
        int mid = (left+right)/2;

        merge_sort(left, mid);
        merge_sort(mid + 1, right);

        merge(left, mid, right);
    }
    return 0;
}

void* merge_caller(void* t) {
    // int curr_part = curr_partition++;
    int thread_index = *(int*) t;
    // get low and high
    int size = num_records/num_threads;
    int low = thread_index * size;
    int high;
    if(thread_index == num_threads-1) {
        high = num_records - 1;
    } else {
        high = low + size;
    }
    merge_sort(low, high);
    // for(int i = low; low<high; low++) {
    //     for(int j = 0; j<4; j++) {
    //         printf("%d", records[i].key[j]);
    //     }
    //     for(int j= 4; j<100; j++) {
    //         printf("%d", records[i].value[j]);
    //     }
    //     printf("\n");
    // }

    // printf("THREAD %d\n", thread_index);
    // int prev = -1215752192;
    // for(int i = low; i<high; i++) {
    //     printf("%d\n",records[i].key[0]);
    //     if(records[i].key[0]<prev) {
    //         printf("BAD DIDNT WORK\n");
    //     }
    //     prev = records[i].key[0];
    // }
    return 0;
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
    for(int i = 0; i<num_records; i++) {
        printf("%d\n",records[i].key[0]);
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
    // int prev = -100000000000;
    // for(int i = 0; i<num_records; i++) {
    //     printf("%d\n",records[i].key[0]);
    //     if(records[i].key[0]<prev) {
    //         printf("BAD DIDNT WORK\n");
    //     }
    //     prev = records[i].key[0];
    // }

    // remerge all of them
    // int num_sections = num_threads;
    // int i = 0;
    // while(num_sections > 1) {
    //     while(i+1 < num_sections) {

    //     }
        
    // }





    free(indexes);
    
    // send to output
    
    
    
    return 0;
}






