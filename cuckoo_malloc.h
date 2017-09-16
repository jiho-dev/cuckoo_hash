#ifndef __CUCKOO_MALLOC_H__
#define __CUCKOO_MALLOC_H__

#ifdef __KERNEL__

#else
#include <stdlib.h>

#endif

void* cuckoo_malloc(size_t len);
void  cuckoo_free(void *mem);

#endif
