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

int main(int argc, char** argv) {
    // parse input - use threading
    // FILE *fp;
    //char buffer[100];
    int filelen;

    
    // fp = fopen(argv[1], "rb");
    int fd = open(argv[1], O_RDWR);         
    struct stat statbuf;
    int err = stat(argv[1], &statbuf);
    if (err < 0) {
        printf("\n\"%s \" could not open\n", argv[1]);
        exit(2);
    }

    filelen = statbuf.st_size;

    int num_records = filelen / 100;

    //converted to char pointer
    //made map private to avoid modifications from other thread.
    char *ptr = (char *) mmap(0, statbuf.st_size, PROT_READ, MAP_PRIVATE, fd, 0 );

    //create an array of structs
    struct rec *records = malloc(num_records * sizeof(struct rec));

    int j = 0;

    //copy key and value into struct
    for(int i = 0; i<statbuf.st_size; i = i + 100) {
        memcpy(records[j].key, ptr+i, 4);
        memcpy(records[j].value, ptr+i+4, 96);
        j++;
        // printf("%s\n", ptr + i);
        // printf("%d\n", records[j].key[0]);
    }

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

    close(fd);

    // sort     
    
    
    // send to output
    
    
    
    return 0;
}






