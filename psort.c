#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <sys/sysinfo.h>
#include <sys/stat.h>
#include <sys/mman.h>

int debug = 2;

struct rec {
    int key;
    char value[96];
};

struct caller {
    int index;
};

struct targs {
    int i; 
    int size;
    int sections;
    int merges;
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
    if (debug)printf("left: %d, mid: %d, right: %d\n", left,mid,right);
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
    struct rec *left_array = malloc(size_left * sizeof(struct rec));
    struct rec *right_array = malloc(size_right * sizeof(struct rec));

    for(int i = 0; i<size_left; i++) {
        if (debug == 2) printf("Before set left\n");
        left_array[i] = records[left+i];
         if (debug == 2) printf("After set left\n");
        // printf("Left array[%d] set to %d\n", i, left_array[i]);
    }
    for(int i = 0; i<size_right; i++) {
         if (debug == 2) printf("Before set right (%d,%d)\n",i, mid+i+1);
        right_array[i] = records[mid+i+1];
         if (debug == 2) printf("After set right\n");
    }
    if (debug) {
        printf("IN HERE 5\n");
    }

    // merge
    int l = 0;
    int r = 0;
    int i = left;
    while(l<size_left && r < size_right) {
        if (debug) {
            // printf("IN HERE 7\n");
            printf("L: %d, R: %d, I: %d\n",l,r,i);
        }
        
        if(left_array[l].key < right_array[r].key) {
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
    if (debug) printf("l: %d, size_left: %d, r: %d, size_right: %d\n",l,size_left,r,size_right);
    while(l<size_left) {
        if (debug == 2) printf("Before set records[%d]\n", i);

        records[i] = left_array[l];
        if (debug == 2) printf("After set records[%d]\n", i);
        
        l++;
        i++;
    }
    while(r<size_right) {
        if (debug == 2) printf("Before set records[%d]\n", i);
        
        records[i] = right_array[r];
        if (debug == 2) printf("After set records[%d]\n", i);
        
        r++;
        i++;
    }
    if (debug) printf("Returned\n");
    // free(left_array);
    // free(right_array);
    return 0;
}

int merge_sort(int left, int right) {
    // printf("%d%d\n", left, right);
    if (debug) printf("Merge_sort\tleft: %d, right: %d\n",left,right);
    if(left<right) {
        int mid = left + (right-left)/2;
        if (debug) {
            printf("IN MERGE SORT\n");
        }

        merge_sort(left, mid);
        merge_sort(mid + 1, right);

        merge(left, mid, right);

        if (debug) printf("Finished merge\n");
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

    if (debug) printf("num_threads = %d\n", num_threads);
    if(thread_index == num_threads-1) {
        high = num_records - 1;
    } else {
        high = low + size-1;
    }

    //printf("MERGING THREAD %d: from %d to %d\n", thread_index, low,high);
    if (debug) {
        printf("HERE 3\n");
    }
    if (debug)
    printf("Calling merge sort: (%d,%d)\n",low,high);
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

void* parallel_merge(void * args) {
    // int i =  (args + 0);
    // int s =  (args + 1);
    // int sections =  (args + 2);
    // int merges = (args + 3);
    struct targs *curr = (struct targs*) args;
    int i = curr->i;
    int s = curr->size;
    int sections = curr->sections;
    int merges = curr->merges;

    if (debug == -1) printf("i: %d, size: %d, sections: %d, merges: %d\n", i, s, sections, merges);
    
    int low = i * s;
    int high;
    if(sections % 2 == 0 && i==merges-1) {
        high = num_records - 1;
    } else {
        high = low + s - 1;
    }
    int mid = low + s/2 - 1;
    merge(low, mid, high);
    if(debug)
     printf("MERGED %d to %d\n", low, high);

    return 0;
}

int main(int argc, char** argv) {
    // parse input - use threading
    FILE *fp;
    //char buffer[100];
    int filelen, fd, err;     
    struct stat statbuf;
    // num_threads = get_nprocs();
    // num_threads = 1;
    num_threads = 5;

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

    if (num_records < 100) {
        num_threads = 1;
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
    records = malloc(num_records * sizeof(struct rec));
    for (int i = 0; i < num_records; i++) {
        if (debug)
        printf("record %d, %d\n", i, records[i].key);
    }

    // for (int x = 0; x < num_records; x++) {
    //     records[x] = malloc(sizeof(struct rec));
    // }
    //Record = pointer to a pointer of a record
    //record[0] = pointer to a record (struct)

    //copy key and value into struct
    // memcpy(&records, ptr, num_records * sizeof(struct rec));
    // fread(records, statbuf.st_size, 1, fp);
    for(int i = 0; i<filelen; i = i + 100) {
        if (debug) {
            printf("%d\n", i);
        }
        memcpy(&records[(int) i/100], ptr+i, 100);
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
        if (debug) printf("Current thread: %ld\n", i);
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

    if (debug) printf("Finished sorting\n");
    // remerge all of them
    int num_sections = num_threads;
    int num_merges = num_sections/2;
    int size = num_records/num_sections*2;
    while(num_merges > 0) {
        int sections = num_sections;
        pthread_t thread_pool[num_merges];
        struct targs data[num_merges];
        for(int i = 0; i<num_merges; i++) {
            //int size = num_records/num_merges;
            data[i].i = i;
            data[i].size = size;
            data[i].sections = sections;
            data[i].merges = num_merges;
            
            // args[0] = i;
            // args[1] = size;
            // args[2] = num_sections;
            // args[3] = num_merges;
            if (debug) printf("Creating thread (%d,%d,%d,%d)\n",i, size,num_sections,num_merges);
            pthread_create(&thread_pool[i], NULL, parallel_merge, (void *)&data[i]);
            // free(args);
            
            sections--;
        }
        for(int i = 0; i<num_merges; i++) {
            if(debug)printf("Joined thread %d\n",i);
            //if(debug) printf("Thread: %d\n", thread_pool[i]);
            pthread_join(thread_pool[i], NULL);
            //if(debug) printf("Finished thread %d\n",i);
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
    int last = 0;
    for(int i = 0; i< num_records; i++) {
        //int record[25];
        //record[0] = records[i]->key;
        // for(int j = 0; j <24; j++) {
        //     record[j+1] = records[i]->value[j];
        // }
       fwrite(&records[i],sizeof(struct rec),1,fdout);
       if(debug == 2) printf("Writing: %d",records[i].key);
       if (debug == 2) {
        if (last > records[i].key) printf(" INCORRECT!");
        last = records[i].key;
        printf("\n");
       }
    //    if (rc != 100) {
    //         fprintf(stderr, "An error has occurred\n");
    //         exit(0);
    //    }
    }
    fclose(fdout);
    
    return 0;
}