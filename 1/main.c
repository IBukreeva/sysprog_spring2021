#define _XOPEN_SOURCE /* Mac compatibility. */
#include <ucontext.h>
#include <signal.h>
#include <sys/mman.h>

#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <inttypes.h>
#include <string.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>


#define SORTED "sorted_"
#define handle_error(msg) \
   do { perror(msg); exit(EXIT_FAILURE); } while (0)
#define stack_size 1024 * 1024


int merge_sort(const char *filename, int,int);
void func(int * masA, int *masB, int * masC, int n, int m);
void merge_files();
int add_merge_queue(char *);


static void *
allocate_stack_mmap()
{
	return mmap(NULL, stack_size, PROT_READ | PROT_WRITE | PROT_EXEC,
		    MAP_ANON | MAP_PRIVATE, -1, 0);
}

char ** merge_queue;
int merge_queue_len = 0;
int finished_coros = 0;
int * coro_statuses; // -1 - haven't started, 0 - started, not completed, 1 - completed
char** coro_stacks;
static ucontext_t * coros;

int main(int argc, char *argv[]) {

    int i=0;
    int num_files = argc - 1;
    if(num_files < 1){
        printf("no files to sort\n");
        return 0;
    }

    merge_queue = (char**)malloc(num_files*sizeof(char*));
    if(merge_queue==NULL) {
        printf("memory allocation error: merge_queue\n");
        return 0;
    }
    coro_stacks = (char**)malloc((num_files+1)*sizeof(char**));
    if(coro_stacks==NULL) {
        printf("memory allocation error: coro_stacks\n");
        free(merge_queue);
        return 0;
    }
    coros = (ucontext_t*)malloc((num_files+1)*sizeof(ucontext_t));
    if(coros==NULL) {
        printf("memory allocation error: coros\n");
        free(merge_queue); free(coro_stacks);
        return 0;
    }
    coro_statuses = (int*)malloc((num_files+1)*sizeof(int));
    if(coro_statuses==NULL) {
        printf("memory allocation error: coro_statuses\n");
        free(merge_queue); free(coro_stacks); free(coros);
        return 0;
    }

    for(i=0;i<num_files;i++){ //create coroutine for each file

        coro_stacks[i] = allocate_stack_mmap();
        if(getcontext(&coros[i])==-1){
            handle_error("getcontext");
        }
        coros[i].uc_stack.ss_sp = coro_stacks[i];
	    coros[i].uc_stack.ss_size = stack_size;
        coros[i].uc_link = &coros[num_files];
        makecontext(&coros[i], merge_sort, 3, argv[i+1], i, num_files);
        coro_statuses[i] = -1; //set status: coro has not started yet
    }

    while(finished_coros<num_files){
        if (coro_statuses[i]==1){ //coro which to we supposed to switch is completed already, 
            i = (i+1)%num_files; //but we still have uncompleted coros
            continue;
        }
        if (swapcontext(&coros[num_files], &coros[i%num_files]) == -1){
		    handle_error("swapcontext");
        }
        i = (i+1)%num_files;
    }
    
    merge_files();
    printf("all os OK\n");

    free(coro_stacks);
    free(coro_statuses);
    free(coros);
    for(int i=0;i<merge_queue_len;i++){
        free(merge_queue[i]);
    }
    free(merge_queue);

    return 0;
}

int merge_sort(const char * fname, int coro_order, int num_files){

    int p;
    int *mas, *hp;
    struct stat file_stat_buff;
    int64_t file_size;
    int len=0;

    printf("coro %d started\n", coro_order);
    coro_statuses[coro_order] = 0; //change status: coro started

    FILE *fd = fopen(fname, "r");
    if(fd == NULL){
        printf("no such file: %s\n", fname);
        coro_statuses[coro_order] = 1; //completed work. status: can't switch on it again
        finished_coros += 1;
        return 0;
    }
    
    if ((stat(fname, &file_stat_buff) != 0) || (!S_ISREG(file_stat_buff.st_mode))) {
		file_size = -1;
	} else{
		file_size = file_stat_buff.st_size;
	}

    mas = (int*)malloc(file_size);
    if(mas==NULL) {
        printf("memory error: mas in coro merge_sort");
        coro_statuses[coro_order] = 1; //completed work. status: can't switch on it again
        finished_coros += 1;
        fclose(fd);
        return 0;
    }
    hp = (int*)malloc(file_size);
    if(hp==NULL) {
        printf("memory error: hp in coro merge_sort");
        coro_statuses[coro_order] = 1; //completed work. status: can't switch on it again
        finished_coros += 1;
        fclose(fd); free(mas);
        return 0;
    }


    while(fscanf(fd, "%d", &(mas[len])) != EOF){ // TODO: переделать на асинхронное чтение с диска
        len++;
    }
    
    for(int i=1; i<len; i*=2){ 
        for(int j=0;j<len-i;j+=2*i){
            if(j+2*i<len){ p=j+2*i; }//p - это конец, до какого момента 2й отрезок, мы ничего не знаем про кратность/совпадение длин
            else{ p=len;}
            func(&mas[j], &mas[j+i], &hp[j], i, p-i-j);

            if (swapcontext(&coros[coro_order], &coros[num_files]) == -1){ //try to switch on nother coro after every merge operation
	        	handle_error("swapcontext");
            }
        }
    }

    fclose(fd);
    free(mas); free(hp);

    int filename_len = strlen(fname);
    char * out_fname = (char*)malloc((filename_len+strlen(SORTED))*sizeof(char));
    if( out_fname==NULL){
        coro_statuses[coro_order] = 1; //completed work. status: can't switch on it again
        finished_coros += 1;
        return 0;
    }
    snprintf(out_fname, filename_len+strlen(SORTED), "%s%s", SORTED, fname);

    int key = add_merge_queue(out_fname);
    if( key!=0){ //something wrong with adding to merge_queue
        coro_statuses[coro_order] = 1; //completed work. status: can't switch on it again
        finished_coros += 1;
        free(out_fname);
        return 0;
    }

    FILE * out_fd = fopen(out_fname, "w");
    free(out_fname); //don't need this name anymore
    if(out_fd == NULL) {
        coro_statuses[coro_order] = 1; //completed work. status: can't switch on it again
        finished_coros += 1;
        return 0;
    }

    for (int i=0;i<len;i++){
        fprintf(out_fd, "%d ", mas[i]);
    }

    fclose(out_fd);

    printf("coro %d completed\n", coro_order);
    coro_statuses[coro_order] = 1; //completed work. status: can't switch on it again
    finished_coros += 1;
    return 0;
}


void func(int * masA, int *masB, int * masC, int n, int m){ //слияния функция
    
    int indC=0, indA=0, indB=0;
    
    while((indA<n)&&(indB<m)){
        if(masA[indA]<masB[indB]){
            masC[indC]=masA[indA];
            indA++;
        }
        else{
            masC[indC]=masB[indB];
            indB++;
        }
        indC++;
    }
    
    while(indA<n){
        masC[indC]=masA[indA];
        indC++;
        indA++;
    }
    while(indB<m){        
        masC[indC]=masB[indB];
        indC++;
        indB++;
    }//сначала отсортировали во вспомогательный массив
    for(int i=0;i<n;i++){ masA[i]=masC[i];}//а сейчас записываем обратно в исходный
    for(int i=0;i<m;i++){ masB[i]=masC[n+i];}
    return;   
}

int add_merge_queue(char * fname){ //add new file to merge to merge_queue

    merge_queue[merge_queue_len] = (char*)malloc((strlen(fname)+1)*sizeof(char));
    if(merge_queue[merge_queue_len]==NULL){
        return -1;
    }
    strcpy(merge_queue[merge_queue_len], fname);
    merge_queue[merge_queue_len][strlen(fname)] = '\0';
    merge_queue_len += 1;
    return 0;
}

void merge_files(){

    int * pointers = (int*)malloc(merge_queue_len*sizeof(int));
    if(pointers==NULL){
        printf("memory allocation error: merge_files");
        return;
    }
    int * max_lens = (int*)malloc(merge_queue_len*sizeof(int));
    if(max_lens==NULL){
        printf("memory allocation error: merge_files");
        free(pointers);
        return;
    }
    int ** data = (int**)malloc(merge_queue_len*sizeof(int*));
    if(data==NULL){
        printf("memory allocation error: merge_files");
        free(max_lens); free(pointers);
        return;
    }
    FILE** fds = (FILE**)malloc(merge_queue_len*sizeof(FILE*));
    if(fds==NULL){
        printf("memory allocation error: merge_files");
        free(max_lens); free(pointers); free(data);
        return;
    }
    int64_t file_size;
    struct stat file_stat_buff;

    for(int i=0;i<merge_queue_len;i++){
        pointers[i]=0; max_lens[i]=0;
        fds[i] = fopen(merge_queue[i], "r");

        if(fds[i]==NULL){
            printf("can't open file: %s\n", merge_queue[i]);
            for(int j=0;j<i;j++){
                fclose(fds[i]);
                free(data[j]);
            }
            free(pointers); free(fds); free(max_lens); free(data);
            return;
        }

        if ((stat(merge_queue[i], &file_stat_buff) != 0) || (!S_ISREG(file_stat_buff.st_mode))) {
			file_size = -1;
		}
		else{
			file_size = file_stat_buff.st_size;
		}
        data[i] = (int*)malloc(file_size);
        if(data[i]==NULL){
            printf("memory allocation error: %s\n", merge_queue[i]);
            fclose(fds[i]);
            for(int j=0;j<i;j++){
                fclose(fds[j]);
                free(data[j]);
            }
            free(pointers); free(fds); free(max_lens); free(data);
            return;
        }

        while(fscanf(fds[i], "%d", &(data[i][max_lens[i]])) != EOF){
            max_lens[i]++;
        }
    }
    
    FILE * ans_file = fopen("ans_file.txt", "w");
    if (ans_file==NULL) {
        for(int j=0;j<merge_queue_len;j++){
                fclose(fds[j]);
                free(data[j]);
            }
            free(pointers); free(fds); free(max_lens); free(data);
        return;
    }

    int curmin = 0;
    int curmin_ind = -1;
    int finished_files = 0;
    while(finished_files < merge_queue_len) {
        curmin = 0;
        curmin_ind = -1;
        for(int i=0;i<merge_queue_len;i++){

            if(curmin_ind==-1){
                if(pointers[i] < max_lens[i]){ //не достигли конца файла
                    curmin_ind = i;
                    curmin = data[i][pointers[i]];
                }
            } else{
                if(pointers[i] < max_lens[i]){ //минимум какой-то уже установлен, конец файла не достигнут
                    if (data[i][pointers[i]]<curmin){
                        curmin = data[i][pointers[i]];
                        curmin_ind = i;
                    }
                }
            }  
            
        }

        fprintf(ans_file, "%d ", curmin);
        pointers[curmin_ind] += 1;
        if(pointers[curmin_ind]>=max_lens[curmin_ind]){
            finished_files++;
        }
    }

    fclose(ans_file);
    for(int i=0;i<merge_queue_len;i++){
        fclose(fds[i]);
        free(data[i]);
        remove(merge_queue[i]);
    }
    free(data);
    free(pointers);
    free(max_lens);
    free(fds);

    return;
}
