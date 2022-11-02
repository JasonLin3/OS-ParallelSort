#include <stdio.h>
#include <sys/mman.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>


int main(int argc, char** argv) {
    // parse input - use threading
    FILE *fp;
    //char buffer[100];
    int filelen;

    
    // fp = fopen(argv[1], "rb");
    int fd = open(argv[1], O_RDWR);         
    struct stat statbuf;
    int err = fstat(fd, &statbuf);
    if (err < 0) {
        printf("\n\"%s \" could not open\n", argv[1]);
        exit(2);
    }
    int *ptr = mmap(0, statbuf.st_size, PROT_READ | PROT_EXEC, MAP_SHARED, fd, 0 );
    
    for(int i = 0; i<statbuf.st_size; i++) {
        printf("%d", ptr[i]);
    }
    close(fd);
    // fclose(fp);
    // ssize_t n = write(1, ptr, statbuf.st_size);    

    // sort     
    
    
    // send to output
    
    
    
    return 0;
}






