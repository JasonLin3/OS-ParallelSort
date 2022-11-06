#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <sys/sysinfo.h>
#include <sys/stat.h>
#include <sys/mman.h>

int debug = 0;

struct rec {
    int key;
    char value[96];
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
struct rec **records;
int curr_partition;

int merge(int left, int mid, int right) {
    // get size of each half
    int size_left = mid - left + 1;
    int size_right = right-mid;

    if (debug) {
        printf("IN MERGE\n");
    }

    // make temporary arrays
    // struct rec** left_array = malloc(size_left * sizeof(struct rec *));
    // for (int n1 = 0; n1 < size_left; n1++) {
    //     left_array[n1] = malloc(sizeof(struct rec));
    // }

    // struct rec** right_array = malloc(size_right * sizeof(struct rec *));
    // for (int n2 = 0; n2 < size_right; n2++) {
    //     right_array[n2] = malloc(sizeof(struct rec))
    // }
    //struct rec *left_array[size_left];
    //struct rec *right_array[size_right]; 
    struct rec **left_array = malloc(size_left * sizeof(struct rec*));
    struct rec **right_array = malloc(size_left * sizeof(struct rec*));

    for(int i = 0; i<size_left; i++) {
        left_array[i] = records[left+i];
        // printf("Left array[%d] set to %d\n", i, left_array[i]);
    }
    for(int i = 0; i<size_right; i++) {
        right_array[i] = records[mid+i+1];
    }
    if (debug) {
        printf("IN HERE 5\n");
    }

    // merge
    int l = 0;
    int r = 0;
    int i = left;
    while(l<size_left && r< size_right) {
        if (debug) {
            // printf("IN HERE 7\n");
            printf("L: %d, R: %d, I: %d\n",l,r,i);
        }
        
        if(left_array[l]->key < right_array[r]->key) {
            // printf("HERE HERE HERE\n");
            records[i] = left_array[l];
            l++;
        } else {
            // printf("HERE HERE HERE HERE\n");
            records[i] = right_array[r];
            r++;
        }
        i++;
    }

    if (debug) {
        printf("IN HERE 6\n");
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
    // free(left_array);
    // free(right_array);
    return 0;
}

int merge_sort(int left, int right) {
    // printf("%d%d\n", left, right);
    if(left<right) {
        int mid = left + (right-left)/2;
        if (debug) {
            printf("IN MERGE SORT\n");
        }

        merge_sort(left, mid);
        merge_sort(mid + 1, right);

        merge(left, mid, right);
    }
    return 0;
}

void* merge_caller(void* t) {
    // int curr_part = curr_partition++;
    int thread_index = (long) t;
    if (debug) {
        printf("HERE 1\n");
    }
    // get low and high
    int size = num_records/num_threads;
    int low = thread_index * size;
    int high;
    if (debug) {
        printf("HERE 2\n");
    }
    if(thread_index == num_threads-1) {
        high = num_records - 1;
    } else {
        high = low + size-1;
    }
    //printf("MERGING THREAD %d: from %d to %d\n", thread_index, low,high);
    if (debug) {
        printf("HERE 3\n");
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
    //     //printf("%d\n",records[i].key[0]);
    //     if(records[i].key[0]<prev) {
    //         printf("BAD DIDNT WORK\n");
    //         printf("RECORD: %d\n", records[i].key[0]);
    //         printf("PREVIOUS: %d\n", prev);
    //     }
    //     prev = records[i].key[0];
    // }
    return 0;
}

int main(int argc, char** argv) {
    // parse input - use threading
    FILE *fp;
    //char buffer[100];
    int filelen, fd, err;     
    struct stat statbuf;
    // num_threads = get_nprocs();
    num_threads = 1;
    // num_threads = get_nprocs_conf();

    // printf("HERE");
    
    // get file descriptor and size
    fp = fopen(argv[1], "r"); 
    if (fp == NULL) {
        fprintf(stderr, "An error has occurred\n");
        exit(0);
    }
    fd = fileno(fp);
    err = fstat(fd, &statbuf);
    if (err < 0) {
        fprintf(stderr, "An error has occurred\n");
        exit(0);
    }
    filelen = (int)statbuf.st_size;
    num_records = filelen / 100;

    // printf("%d\n", filelen);

    if (filelen <= 0 || filelen % 100 != 0) {
        fprintf(stderr, "An error has occurred\n");
        exit(0);
    }

    //converted to char pointer
    //made map private to avoid modifications from other thread.
    char *ptr = (char *) mmap(0, statbuf.st_size, PROT_READ, MAP_PRIVATE, fd, 0 );
    close(fd);

    // SOMETHING WRONG HERE!
    if (ptr == MAP_FAILED) {
        printf("MMAP FAILED!\n");
        return -1;
    }

    //create an array of structs
    records = malloc(num_records * sizeof(struct rec*));

    for (int x = 0; x < num_records; x++) {
        records[x] = malloc(sizeof(struct rec));
    }
    //Record = pointer to a pointer of a record
    //record[0] = pointer to a record (struct)
    //

    //copy key and value into struct
    for(int i = 0; i<filelen; i = i + 100) {
        if (debug) {
            printf("%d\n", i);
        }
        memcpy((void*)records[i/100], ptr+i, 100);
        if (debug) {
            printf("COPIED EVERYTHING\n");
        }
    }
    // printf("records[0] is set to: %d\n", records[0]->key);
    // for(int i = 0; i<num_records; i++) {
    //     printf("%d\n",records[i].key[0]);
    // }

    if (debug) {
        printf("HERE NOW\n");
    }

    pthread_t threads[num_threads];
    //printf("NUMTHREADS: %d\n", num_threads);
    int* indexes = malloc(num_threads * sizeof(int));
    if (debug) {
        printf("CREATED INDEXES\n");
    }
    // sort     
    for(long i = 0; i<num_threads; i++) {
        indexes[i] = i;
        pthread_create(&threads[i], NULL, merge_caller, (void*)i);
    }
    for(int i = 0; i<num_threads; i++) {
        pthread_join(threads[i], NULL);
    }

    if (debug) {
        printf("CREATED THREADS\n");
    }
    // int prev = -100000000000;
    // for(int i = 0; i<num_records; i++) {
    //     printf("%d\n",records[i].key[0]);
    //     if(records[i].key[0]<prev) {
    //         printf("BAD DIDNT WORK\n");
    //     }
    //     prev = records[i].key[0];
    // }

    // printf("HERE 2");

    // remerge all of them
    int num_sections = num_threads;
    int num_merges = num_sections/2;
    int size = num_records/num_sections*2;
    while(num_merges > 0) {
        int sections = num_sections;
        for(int i = 0; i<num_merges; i++) {
            //int size = num_records/num_merges;
            int low = i * size;
            int high;
            if(num_sections %2 == 0 && i==num_merges-1) {  // i == num_merges-1)
                high = num_records - 1;
            } else {
                high = low + size - 1;
            }
            int mid = low + size/2 - 1;
            merge(low, mid, high);
            // printf("MERGING %d to %d\n", low, high);
            // printf("Num_sections: %d, Num_merges: %d, i = %d, size = %d\n", num_sections, num_merges,i,size);
            sections--;
        }
        size *= 2;
        num_sections = sections;
        num_merges = num_sections/2;
    }

    // int prev = -1215752192;
    // for(int i = 0; i<num_records; i++) {
    //     //printf("%d\n",records[i].key[0]);
    //     if(records[i].key[0]<prev) {
    //         printf("BAD DIDNT WORK\n");
    //         printf("RECORD: %d\n", records[i].key[0]);
    //         printf("PREVIOUS: %d\n", prev);
    //     }
    //     prev = records[i].key[0];
    // }

    free(indexes);
    
    // send to output
    // FILE* fp;

    // fp = fopen(argv[2], "w");

    if (debug) printf("Opening file\n");

    FILE *fdout = fopen(argv[2], "w");
    if (fdout == NULL) {
        fprintf(stderr, "An error has occurred\n");
        exit(0);
    }
    if (debug) printf("File open\n");

    for(int i = 0; i< num_records; i++) {
        //int record[25];
        //record[0] = records[i]->key;
        // for(int j = 0; j <24; j++) {
        //     record[j+1] = records[i]->value[j];
        // }
       fwrite(records[i],sizeof(struct rec),1,fdout);
    //    if (rc != 100) {
    //         fprintf(stderr, "An error has occurred\n");
    //         exit(0);
    //    }
    }
    fclose(fdout);
    
    return 0;
}